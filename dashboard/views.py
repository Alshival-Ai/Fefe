from __future__ import annotations

from functools import wraps
from urllib.parse import urlencode

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied
from django.core.mail import send_mail
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.crypto import get_random_string
from django.views.decorators.http import require_POST

from .global_ssh_store import (
    add_global_ssh_credential,
    delete_global_ssh_credential,
    list_global_ssh_credentials,
)
from .health import check_health
from .resources_store import (
    add_resource,
    add_ssh_credential,
    delete_resource,
    delete_ssh_credential,
    get_resource,
    list_resources,
    list_ssh_credentials,
    update_resource,
)

_TEAM_DIRECTORY_STATUS = {
    'team_created': ('Team created.', 'success'),
    'team_renamed': ('Team renamed.', 'success'),
    'team_deleted': ('Team deleted.', 'success'),
    'team_member_added': ('User added to team.', 'success'),
    'team_member_removed': ('User removed from team.', 'success'),
    'user_permissions_updated': ('User access updated.', 'success'),
    'invite_sent': ('Invite sent.', 'success'),
    'invite_created_email_failed': ('User was created, but invite email could not be sent.', 'warning'),
    'team_name_required': ('Team name is required.', 'warning'),
    'team_name_exists': ('Team name already exists.', 'warning'),
    'invite_required_fields': ('Username and email are required to send an invite.', 'warning'),
    'cannot_demote_self': ('You cannot remove your own superuser access.', 'warning'),
    'cannot_remove_last_superuser': ('At least one superuser must remain.', 'warning'),
}


def _team_directory_status_context(status_code: str) -> tuple[str, str]:
    return _TEAM_DIRECTORY_STATUS.get(status_code, ('', 'info'))


def _redirect_team_directory(*, tab: str, status: str = ''):
    query = {'tab': tab}
    if status:
        query['status'] = status
    return redirect(f"{reverse('team_directory')}?{urlencode(query)}")


def _post_flag(request, key: str) -> bool:
    return str(request.POST.get(key, '')).strip().lower() in {'1', 'true', 'on', 'yes'}


def _ssh_team_choices_for_user(user) -> list[str]:
    if user.is_superuser:
        qs = Group.objects.all()
    else:
        qs = user.groups.all()
    return list(qs.order_by('name').values_list('name', flat=True))


def superuser_required(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect(f"{settings.LOGIN_URL}?next={request.path}")
        if not request.user.is_superuser:
            raise PermissionDenied("Superuser access required.")
        return view_func(request, *args, **kwargs)

    return _wrapped


def _resource_metadata_from_request(request) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for key, value in request.POST.items():
        if not key.startswith("meta_"):
            continue
        resolved = (value or "").strip()
        if not resolved:
            continue
        metadata[key.removeprefix("meta_")] = resolved
    return metadata


def _normalize_resource_target(resource_type: str, target: str, address: str, port: str, healthcheck_url: str) -> tuple[str, str, str, str]:
    if resource_type == 'api':
        if not healthcheck_url and target:
            healthcheck_url = target
        target = healthcheck_url
    elif resource_type == 'vm':
        if not address and target:
            address = target
        target = address
    elif resource_type == 'database':
        if not address and target:
            if ':' in target:
                address, port = target.rsplit(':', 1)
            else:
                address = target
        target = f"{address}:{port}" if address and port else address
    return target, address, port, healthcheck_url


def _format_alert_time(value: str) -> str:
    raw = (value or '').strip()
    if not raw:
        return 'Not checked yet'
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        return parsed.strftime('%b %d, %H:%M UTC')
    except Exception:
        return raw[:16]


def _resource_alerts(resources) -> list[dict[str, str | int]]:
    alerts: list[dict[str, str | int]] = []
    for item in resources:
        status = (item.last_status or '').strip().lower()
        if status == 'unhealthy':
            tone = 'critical'
            label = 'Critical'
            title = f'{item.name} is unhealthy'
        elif status == 'unknown':
            tone = 'warning'
            label = 'Unknown'
            title = f'{item.name} status is unknown'
        else:
            continue

        detail = (item.last_error or '').strip() or f'Target: {item.target}'
        alerts.append(
            {
                'resource_id': item.id,
                'tone': tone,
                'label': label,
                'title': title,
                'text': detail,
                'time_label': _format_alert_time(item.last_checked_at),
            }
        )
    return alerts


def _normalize_ssh_scope_level(raw_scope: str, *, allow_global: bool) -> str:
    normalized = (raw_scope or "").strip().lower()
    if normalized == "team":
        return "team"
    if normalized in {"global", "global_team"} and allow_global:
        return "global"
    return "account"


def _resolve_ssh_payload(request, *, default_key_name: str = "") -> dict[str, str | bool]:
    ssh_key_name = (request.POST.get('ssh_key_name') or '').strip()
    ssh_username = (request.POST.get('ssh_username') or '').strip()
    ssh_key_text = (request.POST.get('ssh_key_text') or '').strip()
    ssh_port = (request.POST.get('ssh_port') or '').strip()
    ssh_key_file = request.FILES.get('ssh_key_file')
    ssh_mode = (request.POST.get('ssh_mode') or 'inline').strip()
    ssh_scope_level = _normalize_ssh_scope_level(
        request.POST.get('ssh_scope') or 'account',
        allow_global=request.user.is_superuser,
    )
    ssh_team_name = (request.POST.get('ssh_team_name') or '').strip()
    allowed_team_names = set(_ssh_team_choices_for_user(request.user))
    ssh_credential_id = (request.POST.get('ssh_credential_id') or '').strip()
    ssh_credential_scope = ''
    clear_ssh_key = (request.POST.get('clear_ssh_key') or '') == '1'

    if ssh_mode == 'saved':
        if ssh_credential_id:
            local_items = list_ssh_credentials(request.user)
            local_credentials = {f"local:{item.id}": item for item in local_items}
            for item in local_items:
                local_credentials[str(item.id)] = item
            global_credentials = {f"global:{item.id}": item for item in list_global_ssh_credentials()}
            credential = local_credentials.get(ssh_credential_id) or global_credentials.get(ssh_credential_id)
            if not credential:
                ssh_credential_id = ''
            else:
                if ssh_credential_id.startswith('global:'):
                    resolved_scope = 'global_team'
                else:
                    resolved_scope = credential.scope if credential.scope in {'account', 'team'} else 'account'

                ssh_key_name = credential.name
                ssh_key_text = ''
                clear_ssh_key = True
                ssh_credential_scope = resolved_scope

        if ssh_credential_id and not ssh_username:
            ssh_credential_id = ''
            ssh_credential_scope = ''
            ssh_key_name = ''
            ssh_key_text = ''
            clear_ssh_key = True

        if not ssh_credential_id:
            ssh_key_name = ''
            ssh_key_text = ''
            ssh_credential_scope = ''

    if ssh_mode != 'saved':
        ssh_credential_id = ''
        ssh_credential_scope = ''
        if ssh_key_file:
            ssh_key_text = ssh_key_file.read().decode('utf-8', errors='ignore').strip()
            if not ssh_key_name:
                ssh_key_name = ssh_key_file.name
        if ssh_scope_level == 'team':
            if not ssh_team_name or ssh_team_name not in allowed_team_names:
                ssh_key_name = ''
                ssh_key_text = ''
                clear_ssh_key = True
        if ssh_key_text and not ssh_username:
            ssh_key_text = ''
        if ssh_key_text and ssh_username:
            resolved_key_name = ssh_key_name or default_key_name or "resource-ssh-key"
            if ssh_scope_level == 'global' and request.user.is_superuser:
                credential_id = add_global_ssh_credential(
                    user=request.user,
                    name=resolved_key_name,
                    team_name=ssh_team_name,
                    private_key_text=ssh_key_text,
                )
                ssh_credential_id = f'global:{credential_id}'
                ssh_credential_scope = 'global_team'
            else:
                local_scope = 'team' if ssh_scope_level == 'team' else 'account'
                credential_id = add_ssh_credential(
                    request.user,
                    resolved_key_name,
                    local_scope,
                    ssh_team_name,
                    ssh_key_text,
                )
                ssh_credential_id = str(credential_id)
                ssh_credential_scope = local_scope
            ssh_key_name = resolved_key_name
            ssh_key_text = ''
            clear_ssh_key = True

    if ssh_username and not ssh_port:
        ssh_port = '22'

    return {
        'ssh_key_name': ssh_key_name,
        'ssh_username': ssh_username,
        'ssh_key_text': ssh_key_text,
        'ssh_port': ssh_port,
        'ssh_credential_id': ssh_credential_id,
        'ssh_credential_scope': ssh_credential_scope,
        'clear_ssh_key': clear_ssh_key,
    }


@login_required
def home(request):
    return render(request, 'pages/home.html')


@superuser_required
def team_directory(request):
    active_tab = (request.GET.get('tab') or 'teams').strip().lower()
    if active_tab not in {'teams', 'users'}:
        active_tab = 'teams'

    status_code = (request.GET.get('status') or '').strip()
    status_message, status_tone = _team_directory_status_context(status_code)

    User = get_user_model()
    teams = Group.objects.all().order_by('name').prefetch_related('user_set')
    users = User.objects.all().order_by('username', 'email').prefetch_related('groups')

    context = {
        'active_tab': active_tab,
        'teams': teams,
        'users': users,
        'status_message': status_message,
        'status_tone': status_tone,
        'status_code': status_code,
    }
    return render(request, 'pages/team_directory.html', context)


@require_POST
@superuser_required
def team_directory_create_team(request):
    name = (request.POST.get('name') or '').strip()
    if not name:
        return _redirect_team_directory(tab='teams', status='team_name_required')
    if Group.objects.filter(name__iexact=name).exists():
        return _redirect_team_directory(tab='teams', status='team_name_exists')

    Group.objects.create(name=name)
    return _redirect_team_directory(tab='teams', status='team_created')


@require_POST
@superuser_required
def team_directory_rename_team(request, team_id: int):
    team = get_object_or_404(Group, id=team_id)
    name = (request.POST.get('name') or '').strip()
    if not name:
        return _redirect_team_directory(tab='teams', status='team_name_required')
    if Group.objects.exclude(id=team.id).filter(name__iexact=name).exists():
        return _redirect_team_directory(tab='teams', status='team_name_exists')

    team.name = name
    team.save(update_fields=['name'])
    return _redirect_team_directory(tab='teams', status='team_renamed')


@require_POST
@superuser_required
def team_directory_delete_team(request, team_id: int):
    team = get_object_or_404(Group, id=team_id)
    team.delete()
    return _redirect_team_directory(tab='teams', status='team_deleted')


@require_POST
@superuser_required
def team_directory_add_team_member(request, team_id: int):
    team = get_object_or_404(Group, id=team_id)
    user_id = (request.POST.get('user_id') or '').strip()
    User = get_user_model()
    user = get_object_or_404(User, id=user_id)
    team.user_set.add(user)
    return _redirect_team_directory(tab='teams', status='team_member_added')


@require_POST
@superuser_required
def team_directory_remove_team_member(request, team_id: int, user_id: int):
    team = get_object_or_404(Group, id=team_id)
    User = get_user_model()
    user = get_object_or_404(User, id=user_id)
    team.user_set.remove(user)
    return _redirect_team_directory(tab='teams', status='team_member_removed')


@require_POST
@superuser_required
def team_directory_update_user_permissions(request, user_id: int):
    User = get_user_model()
    user = get_object_or_404(User, id=user_id)

    make_staff = _post_flag(request, 'is_staff')
    make_superuser = _post_flag(request, 'is_superuser')

    if user.id == request.user.id and not make_superuser:
        return _redirect_team_directory(tab='users', status='cannot_demote_self')
    if user.is_superuser and not make_superuser and User.objects.filter(is_superuser=True).count() <= 1:
        return _redirect_team_directory(tab='users', status='cannot_remove_last_superuser')

    user.is_superuser = make_superuser
    user.is_staff = True if make_superuser else make_staff
    user.save(update_fields=['is_staff', 'is_superuser'])
    return _redirect_team_directory(tab='users', status='user_permissions_updated')


@require_POST
@superuser_required
def team_directory_invite_user(request):
    username = (request.POST.get('username') or '').strip()
    email = (request.POST.get('email') or '').strip().lower()
    make_staff = _post_flag(request, 'is_staff')
    make_superuser = _post_flag(request, 'is_superuser')

    if not username or not email:
        return _redirect_team_directory(tab='users', status='invite_required_fields')

    User = get_user_model()
    user = User.objects.filter(username__iexact=username).first()
    if not user:
        user = User.objects.filter(email__iexact=email).first()

    temporary_password = get_random_string(14)
    if user:
        if not user.email:
            user.email = email
        user.set_password(temporary_password)
        user.is_active = True
        user.is_superuser = make_superuser
        user.is_staff = True if make_superuser else make_staff
        user.save()
    else:
        user = User.objects.create_user(
            username=username,
            email=email,
            password=temporary_password,
            is_active=True,
            is_staff=True if make_superuser else make_staff,
            is_superuser=make_superuser,
        )

    login_url = request.build_absolute_uri(settings.LOGIN_URL)
    subject = 'You are invited to Fefe'
    message = (
        "You have been invited to the Fefe console.\n\n"
        f"Username: {user.get_username()}\n"
        f"Temporary password: {temporary_password}\n"
        f"Login URL: {login_url}\n\n"
        "Please sign in and change your password immediately."
    )
    from_email = getattr(settings, 'DEFAULT_FROM_EMAIL', 'noreply@fefe.local')

    try:
        send_mail(subject, message, from_email, [email], fail_silently=False)
    except Exception:
        return _redirect_team_directory(tab='users', status='invite_created_email_failed')

    return _redirect_team_directory(tab='users', status='invite_sent')


@login_required
def resources(request):
    resources = list_resources(request.user)
    resource_alerts = _resource_alerts(resources)
    member_teams = _ssh_team_choices_for_user(request.user)
    local_ssh_credentials = list_ssh_credentials(request.user)
    global_ssh_credentials = list_global_ssh_credentials()
    ssh_credentials = []
    for item in local_ssh_credentials:
        ssh_credentials.append(
            {
                'id': item.id,
                'id_value': str(item.id),
                'name': item.name,
                'scope': item.scope,
                'scope_level': item.scope if item.scope in {'account', 'team'} else 'account',
                'team_name': item.team_name,
                'created_at': item.created_at,
                'is_global': False,
            }
        )
    for item in global_ssh_credentials:
        ssh_credentials.append(
            {
                'id': item.id,
                'id_value': f'global:{item.id}',
                'name': item.name,
                'scope': 'global_team',
                'scope_level': 'global',
                'team_name': item.team_name,
                'created_at': item.created_at,
                'is_global': True,
            }
        )
    account_ssh_keys = [item for item in ssh_credentials if item['scope'] == 'account']
    team_ssh_keys = [item for item in ssh_credentials if item['scope'] in {'team', 'global_team'}]
    context = {
        'resources': resources,
        'resource_alerts': resource_alerts,
        'member_teams': member_teams,
        'account_ssh_keys': account_ssh_keys,
        'team_ssh_keys': team_ssh_keys,
        'ssh_credential_choices': ssh_credentials,
        'global_ssh_keys': global_ssh_credentials,
    }
    return render(request, 'pages/resources.html', context)


@login_required
def resource_detail(request, resource_id: int):
    resource = get_resource(request.user, resource_id)
    if resource is None:
        return redirect('resources')
    last_checked_display = ''
    if resource.last_checked_at:
        try:
            from datetime import datetime

            parsed = datetime.fromisoformat(resource.last_checked_at.replace('Z', '+00:00'))
            last_checked_display = parsed.strftime('%b %d, %Y %H:%M UTC')
        except Exception:
            last_checked_display = resource.last_checked_at[:16]
    return render(
        request,
        'pages/resource_detail.html',
        {
            'resource': resource,
            'last_checked_display': last_checked_display or '—',
        },
    )


@login_required
@require_POST
def add_resource_item(request):
    name = (request.POST.get('name') or '').strip()
    resource_type = (request.POST.get('resource_type') or '').strip()
    target = (request.POST.get('target') or '').strip()
    address = (request.POST.get('address') or '').strip()
    port = (request.POST.get('port') or '').strip()
    db_type = (request.POST.get('db_type') or '').strip()
    healthcheck_url = (request.POST.get('healthcheck_url') or '').strip()
    resource_subtype = (request.POST.get('resource_subtype') or '').strip()
    notes = (request.POST.get('notes') or '').strip()
    resource_metadata = _resource_metadata_from_request(request)
    ssh_payload = _resolve_ssh_payload(request, default_key_name=name)
    target, address, port, healthcheck_url = _normalize_resource_target(resource_type, target, address, port, healthcheck_url)

    if name and resource_type and target:
        if resource_type != 'vm':
            ssh_payload = {
                'ssh_key_name': '',
                'ssh_username': '',
                'ssh_key_text': '',
                'ssh_port': '',
                'ssh_credential_id': '',
                'ssh_credential_scope': '',
            }
        add_resource(
            request.user,
            name,
            resource_type,
            target,
            notes,
            address,
            port,
            db_type,
            healthcheck_url,
            ssh_payload['ssh_key_name'],
            ssh_payload['ssh_username'],
            ssh_payload['ssh_key_text'],
            ssh_payload['ssh_port'],
            resource_subtype=resource_subtype,
            resource_metadata=resource_metadata,
            ssh_credential_id=ssh_payload['ssh_credential_id'],
            ssh_credential_scope=ssh_payload['ssh_credential_scope'],
        )

    return redirect('resources')


@login_required
@require_POST
def edit_resource_item(request, resource_id: int):
    name = (request.POST.get('name') or '').strip()
    resource_type = (request.POST.get('resource_type') or '').strip()
    target = (request.POST.get('target') or '').strip()
    address = (request.POST.get('address') or '').strip()
    port = (request.POST.get('port') or '').strip()
    db_type = (request.POST.get('db_type') or '').strip()
    healthcheck_url = (request.POST.get('healthcheck_url') or '').strip()
    resource_subtype = (request.POST.get('resource_subtype') or '').strip()
    notes = (request.POST.get('notes') or '').strip()
    resource_metadata = _resource_metadata_from_request(request)
    ssh_payload = _resolve_ssh_payload(request, default_key_name=name)
    target, address, port, healthcheck_url = _normalize_resource_target(resource_type, target, address, port, healthcheck_url)

    if name and resource_type and target:
        if resource_type != 'vm':
            ssh_payload = {
                'ssh_key_name': '',
                'ssh_username': '',
                'ssh_key_text': '',
                'ssh_port': '',
                'ssh_credential_id': '',
                'ssh_credential_scope': '',
                'clear_ssh_key': True,
            }
        update_resource(
            request.user,
            resource_id,
            name,
            resource_type,
            target,
            notes,
            address,
            port,
            db_type,
            healthcheck_url,
            ssh_payload['ssh_key_name'],
            ssh_payload['ssh_username'],
            ssh_payload['ssh_key_text'] if ssh_payload['ssh_key_text'] else None,
            clear_ssh_key=bool(ssh_payload['clear_ssh_key']),
            ssh_port=ssh_payload['ssh_port'],
            resource_subtype=resource_subtype,
            resource_metadata=resource_metadata,
            ssh_credential_id=ssh_payload['ssh_credential_id'],
            ssh_credential_scope=ssh_payload['ssh_credential_scope'],
        )

    return redirect('resources')


@login_required
@require_POST
def delete_resource_item(request, resource_id: int):
    delete_resource(request.user, resource_id)
    return redirect('resources')


@login_required
@require_POST
def check_resource_health(request, resource_id: int):
    result = check_health(resource_id, user=request.user)
    return JsonResponse(
        {
            'resource_id': result.resource_id,
            'status': result.status,
            'checked_at': result.checked_at,
            'target': result.target,
            'error': result.error,
        }
    )


@login_required
@require_POST
def add_ssh_credential_item(request):
    scope = (request.POST.get('scope') or 'account').strip()
    name = (request.POST.get('name') or '').strip()
    key_text = (request.POST.get('private_key_text') or '').strip()
    key_file = request.FILES.get('private_key_file')
    team_name = (request.POST.get('team_name') or '').strip()

    if key_file:
        key_text = key_file.read().decode('utf-8', errors='ignore').strip()
    if not (name and key_text):
        return redirect('resources')

    member_team_names = set(_ssh_team_choices_for_user(request.user))
    if scope == 'team':
        if not team_name or team_name not in member_team_names:
            return redirect('resources')
    elif scope not in {'global', 'team_global'}:
        team_name = ''

    if scope in {'global', 'team_global'}:
        if not request.user.is_superuser:
            return redirect('resources')
        add_global_ssh_credential(
            user=request.user,
            name=name,
            team_name=team_name,
            private_key_text=key_text,
        )
        return redirect('resources')

    add_ssh_credential(request.user, name, scope, team_name, key_text)

    return redirect('resources')


@login_required
@require_POST
def delete_ssh_credential_item(request, credential_id: int):
    delete_ssh_credential(request.user, credential_id)
    return redirect('resources')


@login_required
@require_POST
def delete_global_ssh_credential_item(request, credential_id: int):
    if not request.user.is_superuser:
        return redirect('resources')
    delete_global_ssh_credential(credential_id=credential_id)
    return redirect('resources')
