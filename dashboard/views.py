from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from urllib.parse import urlencode
import requests

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Group
from django.contrib.sites.models import Site
from django.core.exceptions import PermissionDenied
from django.core.mail import send_mail
from django.db import transaction
from django.db.models import Q
from django.db.utils import OperationalError, ProgrammingError
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import NoReverseMatch, reverse
from django.utils.crypto import get_random_string
from django.utils.text import slugify
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from allauth.socialaccount.models import SocialAccount, SocialApp

from .global_ssh_store import (
    add_global_ssh_credential,
    delete_global_ssh_credential,
    list_global_ssh_credentials,
)
from .models import ResourcePackageOwner, ResourceRouteAlias, ResourceTeamShare, UserFeatureAccess, UserNotificationSettings, WikiPage
from .health import check_health, dispatch_cloud_log_error_alerts
from .setup_state import (
    get_alshival_default_model,
    get_ingest_api_key,
    get_or_create_setup_state,
    get_setup_state,
    is_email_provider_configured,
    is_github_connector_configured,
    is_microsoft_connector_configured,
    is_twilio_configured,
    is_setup_complete,
)
from .wiki_markdown import render_markdown_fallback
from .request_auth import authenticate_api_key, user_can_access_resource
from .resources_store import (
    add_ask_chat_message,
    add_ask_chat_tool_event,
    add_resource,
    add_resource_note,
    clear_user_notifications,
    create_account_api_key,
    create_resource_api_key,
    add_ssh_credential,
    delete_resource,
    delete_ssh_credential,
    get_resource_note_attachment,
    get_resource,
    get_resource_alert_settings,
    get_resource_by_uuid,
    list_ask_chat_messages,
    list_resource_checks,
    list_resource_api_keys,
    list_resource_notes,
    list_user_notifications,
    list_user_api_keys,
    list_resource_logs,
    list_resources,
    list_ssh_credentials,
    _global_owner_dir,
    _user_owner_dir,
    mark_all_user_notifications_read,
    revoke_resource_api_key,
    store_resource_logs,
    upsert_resource_alert_settings,
    update_resource,
)


def _ensure_runtime_cache_dirs() -> None:
    base_dir = Path(getattr(settings, "BASE_DIR", Path(__file__).resolve().parent.parent))
    candidates = []
    current = str(os.getenv("XDG_CACHE_HOME") or "").strip()
    if current:
        candidates.append(Path(current))
    candidates.append(base_dir / "var" / "cache")
    candidates.append(Path("/tmp/alshival-cache"))

    for cache_root in candidates:
        try:
            cache_root.mkdir(parents=True, exist_ok=True)
            probe = cache_root / ".write_test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except Exception:
            continue
        os.environ["XDG_CACHE_HOME"] = str(cache_root)
        os.environ["CHROMA_CACHE_DIR"] = str(cache_root / "chroma")
        os.environ.setdefault("HF_HOME", str(cache_root / "huggingface"))
        current_home = str(os.getenv("HOME") or "").strip()
        if not current_home or current_home == "/":
            home_dir = cache_root / "home"
            try:
                home_dir.mkdir(parents=True, exist_ok=True)
                os.environ["HOME"] = str(home_dir)
            except Exception:
                pass
        return


_TEAM_DIRECTORY_STATUS = {
    'team_created': ('Team created.', 'success'),
    'team_renamed': ('Team renamed.', 'success'),
    'team_deleted': ('Team deleted.', 'success'),
    'team_member_added': ('User added to team.', 'success'),
    'team_member_removed': ('User removed from team.', 'success'),
    'team_members_updated': ('Team members updated.', 'success'),
    'user_permissions_updated': ('User access updated.', 'success'),
    'invite_sent': ('Invite sent.', 'success'),
    'invite_created_email_failed': ('User was created, but invite email could not be sent.', 'warning'),
    'team_name_required': ('Team name is required.', 'warning'),
    'team_name_exists': ('Team name already exists.', 'warning'),
    'invite_required_fields': ('Username and email are required to send an invite.', 'warning'),
    'cannot_demote_self': ('You cannot remove your own superuser access.', 'warning'),
    'cannot_remove_last_superuser': ('At least one superuser must remain.', 'warning'),
    'cannot_delete_self': ('You cannot delete your own account.', 'warning'),
    'cannot_delete_last_superuser': ('At least one superuser must remain.', 'warning'),
    'user_created': ('User account created.', 'success'),
    'user_updated': ('User details updated.', 'success'),
    'user_deleted': ('User deleted.', 'success'),
    'user_username_required': ('Username is required.', 'warning'),
    'user_username_invalid': ('Username does not meet format requirements.', 'warning'),
    'user_username_exists': ('Username already exists.', 'warning'),
    'user_password_required': ('Password is required for new user accounts.', 'warning'),
    'user_password_mismatch': ('Password confirmation does not match.', 'warning'),
    'user_password_too_short': ('Password must be at least 8 characters.', 'warning'),
}

_GITHUB_USERNAME_RE = re.compile(r"^(?!-)(?!.*--)[A-Za-z0-9-]{1,39}(?<!-)$")
_TEAM_DIRECTORY_FEATURES = [
    {
        "key": "codex",
        "label": "Codex",
        "description": "Access Codex workflows and assisted development tools.",
    },
]

_WIKI_STATUS = {
    "wiki_page_created": ("Wiki page created.", "success"),
    "wiki_page_updated": ("Wiki page updated.", "success"),
    "wiki_page_published": ("Wiki page published.", "success"),
    "wiki_draft_saved": ("Draft saved.", "success"),
    "wiki_page_deleted": ("Wiki page deleted.", "success"),
    "wiki_title_required": ("Title is required.", "warning"),
    "wiki_path_required": ("Path is required.", "warning"),
    "wiki_path_invalid": ("Path is invalid. Use letters, numbers, and dashes with optional / folders.", "warning"),
    "wiki_path_exists": ("A wiki page with this path already exists.", "warning"),
    "wiki_page_not_found": ("Wiki page not found.", "warning"),
    "wiki_no_access": ("You do not have access to this wiki page.", "warning"),
}


def _team_directory_status_context(status_code: str) -> tuple[str, str]:
    return _TEAM_DIRECTORY_STATUS.get(status_code, ('', 'info'))


def _wiki_status_context(status_code: str) -> tuple[str, str]:
    return _WIKI_STATUS.get(status_code, ("", "info"))


def _redirect_team_directory(
    *,
    tab: str,
    status: str = '',
    user_id: int | None = None,
    team_id: int | None = None,
):
    query = {'tab': tab}
    if status:
        query['status'] = status
    if user_id:
        query['user'] = str(int(user_id))
    if team_id:
        query['team'] = str(int(team_id))
    return redirect(f"{reverse('team_directory')}?{urlencode(query)}")


def _post_flag(payload, key: str) -> bool:
    value = str(payload.get(key) or "").strip().lower()
    return value in {"1", "true", "on", "yes"}


def _team_directory_feature_keys() -> set[str]:
    return {item["key"] for item in _TEAM_DIRECTORY_FEATURES}


def _normalize_team_names(raw_team_names: list[str]) -> list[str]:
    allowed = set(Group.objects.order_by("name").values_list("name", flat=True))
    resolved: list[str] = []
    for value in raw_team_names:
        team_name = str(value or "").strip()
        if team_name and team_name in allowed and team_name not in resolved:
            resolved.append(team_name)
    return resolved


def _normalize_feature_keys(raw_feature_keys: list[str]) -> list[str]:
    allowed = _team_directory_feature_keys()
    resolved: list[str] = []
    for value in raw_feature_keys:
        feature_key = str(value or "").strip().lower()
        if feature_key and feature_key in allowed and feature_key not in resolved:
            resolved.append(feature_key)
    return resolved


def _normalize_wiki_path(raw_path: str, raw_title: str) -> str:
    candidate = str(raw_path or "").strip().replace("\\", "/")
    candidate = re.sub(r"/+", "/", candidate).strip("/")
    if not candidate:
        candidate = slugify(raw_title or "").strip()

    parts: list[str] = []
    for part in candidate.split("/"):
        normalized = slugify(part).strip()
        if normalized:
            parts.append(normalized)
    return "/".join(parts)


def _normalize_wiki_team_names(user, raw_team_names: list[str]) -> list[str]:
    allowed = set(_ssh_team_choices_for_user(user))
    resolved: list[str] = []
    for value in raw_team_names:
        team_name = str(value or "").strip()
        if team_name and team_name in allowed and team_name not in resolved:
            resolved.append(team_name)
    return resolved


def _wiki_accessible_queryset(user):
    qs = WikiPage.objects.all().prefetch_related("team_access")
    if user.is_superuser:
        return qs

    draft_filter = Q(is_draft=True, created_by_id=user.id)
    published_filter = Q(is_draft=False)
    team_ids = list(user.groups.values_list("id", flat=True))
    if not team_ids:
        return qs.filter(draft_filter | (published_filter & Q(team_access__isnull=True))).distinct()
    return qs.filter(
        draft_filter
        | (published_filter & (Q(team_access__isnull=True) | Q(team_access__id__in=team_ids)))
    ).distinct()


def _can_edit_wiki_page(*, actor, page: WikiPage) -> bool:
    if actor.is_superuser:
        return True
    if page.is_draft:
        return page.created_by_id == actor.id

    page_team_ids = set(page.team_access.values_list("id", flat=True))
    if not page_team_ids:
        return True
    actor_team_ids = set(actor.groups.values_list("id", flat=True))
    return bool(actor_team_ids.intersection(page_team_ids))


def _redirect_wiki(*, status: str = "", page_path: str = ""):
    query: dict[str, str] = {}
    if status:
        query["status"] = status
    if page_path:
        query["page"] = page_path
    if not query:
        return redirect("wiki")
    return redirect(f"{reverse('wiki')}?{urlencode(query)}")


def _redirect_wiki_editor_new(*, status: str = ""):
    if not status:
        return redirect("wiki_editor_new")
    return redirect(f"{reverse('wiki_editor_new')}?{urlencode({'status': status})}")


def _redirect_wiki_editor(*, page_id: int, status: str = ""):
    if not status:
        return redirect("wiki_editor", page_id=page_id)
    return redirect(f"{reverse('wiki_editor', kwargs={'page_id': page_id})}?{urlencode({'status': status})}")


def _sync_user_feature_access(*, user, feature_keys: list[str], actor) -> None:
    allowed_keys = _team_directory_feature_keys()
    selected = set(feature_keys)
    existing = {
        item.feature_key: item
        for item in UserFeatureAccess.objects.filter(user=user, feature_key__in=allowed_keys)
    }
    for feature_key in sorted(allowed_keys):
        should_enable = feature_key in selected
        row = existing.get(feature_key)
        if row is None:
            if should_enable:
                UserFeatureAccess.objects.create(
                    user=user,
                    feature_key=feature_key,
                    is_enabled=True,
                    updated_by=actor,
                )
            continue
        if row.is_enabled == should_enable and row.updated_by_id == actor.id:
            continue
        row.is_enabled = should_enable
        row.updated_by = actor
        row.save(update_fields=["is_enabled", "updated_by", "updated_at"])


def _feature_access_lookup(user_ids: list[int]) -> dict[int, set[str]]:
    lookup: dict[int, set[str]] = {int(user_id): set() for user_id in user_ids}
    if not user_ids:
        return lookup
    rows = UserFeatureAccess.objects.filter(
        user_id__in=user_ids,
        is_enabled=True,
        feature_key__in=_team_directory_feature_keys(),
    ).values_list("user_id", "feature_key")
    for user_id, feature_key in rows:
        lookup.setdefault(int(user_id), set()).add(str(feature_key))
    return lookup


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


def _format_display_time(value: str) -> str:
    raw = (value or '').strip()
    if not raw:
        return '—'
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        return parsed.strftime('%b %d, %Y %H:%M UTC')
    except Exception:
        return raw[:19]


def _normalize_cloud_logs(logs: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    level_styles = {
        'critical': 'danger',
        'exception': 'danger',
        'error': 'danger',
        'warning': 'warning',
        'alert': 'warning',
        'debug': 'muted',
        'info': 'info',
    }
    for item in logs:
        level = str(item.get('level') or 'info').strip().lower() or 'info'
        metadata = item.get('metadata')
        if not isinstance(metadata, dict):
            metadata = {}
        normalized.append(
            {
                'level': level,
                'level_tone': level_styles.get(level, 'info'),
                'logger': str(item.get('logger') or 'alshival').strip() or 'alshival',
                'message': str(item.get('message') or '').strip() or '(no message)',
                'time_display': _format_display_time(str(item.get('timestamp') or '')),
                'metadata': metadata,
                'metadata_pretty': json.dumps(metadata, indent=2, sort_keys=True) if metadata else '',
            }
        )
    return normalized


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
                'resource_uuid': item.resource_uuid,
                'tone': tone,
                'label': label,
                'title': title,
                'text': detail,
                'time_label': _format_alert_time(item.last_checked_at),
            }
        )
    return alerts


def _parse_runtime_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except Exception:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_health_status(raw_status: str) -> str:
    status = str(raw_status or "").strip().lower()
    if status in {"healthy", "ok", "up", "success"}:
        return "healthy"
    if status in {"unhealthy", "down", "failed", "error", "critical"}:
        return "unhealthy"
    return "unknown"


def _status_tone_for_health(status: str) -> str:
    normalized = _normalize_health_status(status)
    if normalized == "healthy":
        return "success"
    if normalized == "unhealthy":
        return "danger"
    return "info"


def _normalize_log_level_bucket(raw_level: str) -> str:
    level = str(raw_level or "").strip().lower()
    if level in {"critical", "exception", "error", "fatal"}:
        return "error"
    if level in {"warning", "warn", "alert"}:
        return "warning"
    return "info"


def _normalize_ssh_scope_level(raw_scope: str, *, allow_global: bool) -> str:
    normalized = (raw_scope or "").strip().lower()
    if normalized == "team":
        return "team"
    if normalized in {"global", "global_team"} and allow_global:
        return "global"
    return "account"


def _normalize_resource_scope(raw_scope: str, *, allow_global: bool) -> str:
    normalized = (raw_scope or "").strip().lower()
    if normalized == "team":
        return "team"
    if normalized == "global" and allow_global:
        return "global"
    return "account"


def _resolve_resource_scope_payload(request) -> dict[str, list[str] | str]:
    scope = _normalize_resource_scope(
        request.POST.get("resource_scope") or "account",
        allow_global=request.user.is_superuser,
    )
    raw_team_names = request.POST.getlist("resource_team_names")
    allowed_team_names = set(_ssh_team_choices_for_user(request.user))
    team_names = []
    for value in raw_team_names:
        resolved = (value or "").strip()
        if resolved and resolved in allowed_team_names and resolved not in team_names:
            team_names.append(resolved)
    if scope == "team":
        if not team_names:
            scope = "account"
    else:
        team_names = []
    return {
        "scope": scope,
        "team_names": team_names,
    }


def _sync_resource_team_shares(*, owner, resource_uuid: str, resource_name: str, scope: str, team_names: list[str]) -> None:
    normalized_scope = _normalize_resource_scope(scope, allow_global=owner.is_superuser)
    ResourceTeamShare.objects.filter(
        owner=owner,
        resource_uuid=resource_uuid,
    ).delete()
    if normalized_scope != "team":
        return

    for team_name in team_names:
        team = Group.objects.filter(name=team_name).first()
        if not team:
            continue
        ResourceTeamShare.objects.create(
            owner=owner,
            resource_uuid=resource_uuid,
            resource_name=resource_name,
            team=team,
            granted_by=owner,
        )


def _resolve_resource_owner(username: str):
    owner_username = (username or "").strip()
    if not owner_username:
        return None
    User = get_user_model()
    return User.objects.filter(username__iexact=owner_username).first()


def _normalize_route_value(value: str) -> str:
    return str(value or "").strip()


def _pick_canonical_team_name(team_names: list[str]) -> str:
    cleaned = [str(item or "").strip() for item in team_names if str(item or "").strip()]
    if not cleaned:
        return ""
    return sorted(cleaned, key=lambda value: value.lower())[0]


def _resource_route_name(route_kind: str, endpoint_key: str) -> str:
    user_names = {
        "detail": "resource_detail",
        "check": "check_resource_health_detail",
        "notes_add": "resource_note_add",
        "notes_attachment": "resource_note_attachment",
        "alerts_update": "update_resource_alert_settings",
        "api_create": "create_resource_api_key_item",
        "api_revoke": "revoke_resource_api_key_item",
        "logs_ingest": "resource_logs_ingest",
    }
    team_names = {
        "detail": "team_resource_detail",
        "check": "team_check_resource_health_detail",
        "notes_add": "team_resource_note_add",
        "notes_attachment": "team_resource_note_attachment",
        "alerts_update": "team_update_resource_alert_settings",
        "api_create": "team_create_resource_api_key_item",
        "api_revoke": "team_revoke_resource_api_key_item",
        "logs_ingest": "team_resource_logs_ingest",
    }
    mapping = team_names if str(route_kind or "").strip().lower() == "team" else user_names
    return mapping[endpoint_key]


def _resource_route_reverse(
    *,
    route_kind: str,
    route_value: str,
    endpoint_key: str,
    resource_uuid: str,
    **kwargs,
) -> str:
    resolved_kind = str(route_kind or "").strip().lower()
    resolved_value = _normalize_route_value(route_value)
    resolved_uuid = str(resource_uuid or "").strip()
    route_kwargs = {"resource_uuid": resolved_uuid}
    if resolved_kind == "team":
        route_kwargs["team_name"] = resolved_value
    else:
        route_kwargs["username"] = resolved_value
    route_kwargs.update(kwargs)
    return reverse(_resource_route_name(resolved_kind, endpoint_key), kwargs=route_kwargs)


def _sync_resource_route_aliases(*, owner, resource_uuid: str, scope: str, team_names: list[str], actor=None) -> None:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid or owner is None:
        return
    resolved_scope = _normalize_resource_scope(scope, allow_global=bool(getattr(actor or owner, "is_superuser", False)))
    if resolved_scope == "team":
        canonical_kind = "team"
        canonical_value = _pick_canonical_team_name(team_names)
        if not canonical_value:
            canonical_kind = "user"
            canonical_value = str(owner.username or "").strip()
    else:
        canonical_kind = "user"
        canonical_value = str(owner.username or "").strip()
    if not canonical_value:
        return

    alias_actor = actor or owner
    with transaction.atomic():
        ResourceRouteAlias.objects.filter(resource_uuid=resolved_uuid, is_current=True).update(
            is_current=False,
            updated_by=alias_actor,
        )
        ResourceRouteAlias.objects.update_or_create(
            resource_uuid=resolved_uuid,
            route_kind=canonical_kind,
            route_value=canonical_value,
            defaults={
                "owner_user": owner,
                "is_current": True,
                "updated_by": alias_actor,
                "created_by": alias_actor,
            },
        )
        # Preserve a user alias for the owner route so legacy /u URLs keep resolving.
        owner_user_value = str(owner.username or "").strip()
        if owner_user_value:
            ResourceRouteAlias.objects.update_or_create(
                resource_uuid=resolved_uuid,
                route_kind="user",
                route_value=owner_user_value,
                defaults={
                    "owner_user": owner,
                    "is_current": canonical_kind == "user" and canonical_value == owner_user_value,
                    "updated_by": alias_actor,
                    "created_by": alias_actor,
                },
            )


def _resolve_resource_route_context(*, route_kind: str, route_value: str, resource_uuid: str) -> tuple[object | None, object | None, ResourceRouteAlias | None]:
    resolved_kind = str(route_kind or "").strip().lower() or "user"
    resolved_value = _normalize_route_value(route_value)
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_value or not resolved_uuid:
        return None, None, None

    current_alias = (
        ResourceRouteAlias.objects.select_related("owner_user")
        .filter(resource_uuid=resolved_uuid, is_current=True)
        .first()
    )
    matched_alias = (
        ResourceRouteAlias.objects.select_related("owner_user")
        .filter(
            resource_uuid=resolved_uuid,
            route_kind=resolved_kind,
            route_value__iexact=resolved_value,
        )
        .first()
    )

    owner = None
    if matched_alias and matched_alias.owner_user_id:
        owner = matched_alias.owner_user
    elif resolved_kind == "user":
        owner = _resolve_resource_owner(resolved_value)
    elif resolved_kind == "team":
        team = Group.objects.filter(name__iexact=resolved_value).first()
        if team:
            share = (
                ResourceTeamShare.objects.select_related("owner")
                .filter(resource_uuid=resolved_uuid, team=team)
                .order_by("-updated_at", "-created_at")
                .first()
            )
            if share and share.owner_id:
                owner = share.owner

    if owner is None and current_alias and current_alias.owner_user_id:
        owner = current_alias.owner_user

    resource = get_resource_by_uuid(owner, resolved_uuid) if owner is not None else None
    if resource is not None and owner is not None:
        if current_alias is None:
            _sync_resource_route_aliases(
                owner=owner,
                resource_uuid=resolved_uuid,
                scope=getattr(resource, "access_scope", "account"),
                team_names=list(getattr(resource, "team_names", []) or []),
                actor=owner,
            )
            current_alias = (
                ResourceRouteAlias.objects.select_related("owner_user")
                .filter(resource_uuid=resolved_uuid, is_current=True)
                .first()
            )
        if matched_alias is None:
            ResourceRouteAlias.objects.create(
                resource_uuid=resolved_uuid,
                route_kind=resolved_kind,
                route_value=resolved_value,
                owner_user=owner,
                is_current=False,
                created_by=owner,
                updated_by=owner,
            )
            matched_alias = (
                ResourceRouteAlias.objects.select_related("owner_user")
                .filter(
                    resource_uuid=resolved_uuid,
                    route_kind=resolved_kind,
                    route_value__iexact=resolved_value,
                )
                .first()
            )
    if current_alias is None and matched_alias is not None:
        current_alias = matched_alias

    return owner, resource, current_alias


def _resource_route_redirect_url(
    *,
    current_alias: ResourceRouteAlias | None,
    endpoint_key: str,
    resource_uuid: str,
    **kwargs,
) -> str:
    if not current_alias:
        return ""
    return _resource_route_reverse(
        route_kind=current_alias.route_kind,
        route_value=current_alias.route_value,
        endpoint_key=endpoint_key,
        resource_uuid=str(resource_uuid or "").strip(),
        **kwargs,
    )


def _resource_route_matches(*, current_alias: ResourceRouteAlias | None, route_kind: str, route_value: str) -> bool:
    if not current_alias:
        return False
    incoming_kind = str(route_kind or "").strip().lower() or "user"
    incoming_value = _normalize_route_value(route_value).lower()
    alias_kind = str(current_alias.route_kind or "").strip().lower()
    alias_value = _normalize_route_value(current_alias.route_value).lower()
    return incoming_kind == alias_kind and incoming_value == alias_value


def _resource_detail_anchor_url(
    *,
    current_alias: ResourceRouteAlias | None,
    route_kind: str,
    route_value: str,
    resource_uuid: str,
    anchor: str = "",
) -> str:
    url = _resource_route_redirect_url(
        current_alias=current_alias,
        endpoint_key="detail",
        resource_uuid=resource_uuid,
    )
    if not url:
        url = _resource_route_reverse(
            route_kind=route_kind,
            route_value=route_value,
            endpoint_key="detail",
            resource_uuid=resource_uuid,
        )
    cleaned_anchor = str(anchor or "").strip().lstrip("#")
    if cleaned_anchor:
        return f"{url}#{cleaned_anchor}"
    return url


def _resource_detail_url_for_uuid(*, actor, resource_uuid: str) -> str:
    resolved_uuid = str(resource_uuid or "").strip()
    current_alias = (
        ResourceRouteAlias.objects.filter(resource_uuid=resolved_uuid, is_current=True)
        .only("route_kind", "route_value")
        .first()
    )
    if current_alias:
        try:
            return _resource_route_reverse(
                route_kind=current_alias.route_kind,
                route_value=current_alias.route_value,
                endpoint_key="detail",
                resource_uuid=resolved_uuid,
            )
        except NoReverseMatch:
            pass
    return reverse(
        "resource_detail",
        kwargs={"username": actor.get_username(), "resource_uuid": resolved_uuid},
    )


def _can_access_owner_resource(*, actor, owner, resource_uuid: str) -> bool:
    if not actor.is_authenticated:
        return False
    if actor.is_superuser:
        return True
    if actor.id == owner.id:
        return True
    actor_team_ids = list(actor.groups.values_list("id", flat=True))
    if not actor_team_ids:
        return False
    return ResourceTeamShare.objects.filter(
        owner=owner,
        resource_uuid=str(resource_uuid or "").strip(),
        team_id__in=actor_team_ids,
    ).exists()


def _can_manage_owner_resource(*, actor, owner) -> bool:
    if not actor.is_authenticated:
        return False
    if actor.is_superuser:
        return True
    return int(actor.id) == int(owner.id)


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
    raw_ssh_team_names = request.POST.getlist('ssh_team_names')
    allowed_team_names = set(_ssh_team_choices_for_user(request.user))
    ssh_team_names = []
    for value in raw_ssh_team_names:
        resolved = (value or '').strip()
        if resolved and resolved in allowed_team_names and resolved not in ssh_team_names:
            ssh_team_names.append(resolved)
    ssh_credential_id = (request.POST.get('ssh_credential_id') or '').strip()
    ssh_credential_scope = ''
    clear_ssh_key = (request.POST.get('clear_ssh_key') or '') == '1'

    if ssh_mode == 'saved':
        if ssh_credential_id:
            local_items = list_ssh_credentials(request.user)
            local_credentials: dict[str, object] = {}
            for item in local_items:
                item_id = str(item.id or "").strip()
                if not item_id:
                    continue
                local_credentials[item_id] = item
                local_credentials[f"local:{item_id}"] = item
                if item_id.startswith("account:"):
                    legacy_id = item_id.split(":", 1)[1].strip()
                    if legacy_id:
                        local_credentials[legacy_id] = item
                        local_credentials[f"local:{legacy_id}"] = item
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
            if not ssh_team_names:
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
                    team_name='',
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
                    ssh_team_names,
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


def _connector_runtime_context(request) -> dict[str, str]:
    app_base_url = str(getattr(settings, "APP_BASE_URL", "") or "").strip().rstrip("/")
    if app_base_url.startswith("http://") or app_base_url.startswith("https://"):
        http_base_url = app_base_url
    else:
        http_base_url = request.build_absolute_uri("/").rstrip("/")

    if http_base_url.startswith("https://"):
        websocket_base_url = "wss://" + http_base_url[len("https://") :]
    elif http_base_url.startswith("http://"):
        websocket_base_url = "ws://" + http_base_url[len("http://") :]
    else:
        websocket_base_url = http_base_url

    return {
        "microsoft_redirect_uri": f"{http_base_url}/accounts/microsoft/login/callback/",
        "github_redirect_uri": f"{http_base_url}/accounts/github/login/callback/",
        "twilio_sms_webhook_uri": f"{http_base_url}/twilio/sms",
        "twilio_sms_group_webhook_uri": f"{http_base_url}/twilio/sms-group",
        "twilio_voice_webhook_uri": f"{http_base_url}/twilio/voice",
        "twilio_voice_stream_public_uri": (
            str(getattr(settings, "TWILIO_VOICE_STREAM_URL_PUBLIC", "") or "").strip()
            or f"{websocket_base_url}/twilio/voice-stream/public"
        ),
        "twilio_voice_stream_internal_uri": (
            str(getattr(settings, "TWILIO_VOICE_STREAM_URL_INTERNAL", "") or "").strip()
            or f"{websocket_base_url}/twilio/voice-stream/internal"
        ),
    }


def _social_app_for_provider(provider: str) -> tuple[SocialApp | None, Site | None]:
    try:
        site = Site.objects.get_current()
    except Exception:
        site = None
    app = None
    if site is not None:
        app = SocialApp.objects.filter(provider=provider, sites=site).order_by("id").first()
    if app is None:
        app = SocialApp.objects.filter(provider=provider).order_by("id").first()
    return app, site


def _connector_initial_values() -> dict[str, str]:
    initial = {
        "openai_api_key": "",
        "microsoft_tenant_id": "",
        "microsoft_client_id": "",
        "microsoft_client_secret": "",
        "github_client_id": "",
        "github_client_secret": "",
        "twilio_account_sid": "",
        "twilio_auth_token": "",
        "twilio_from_number": "",
        "admin_username": "",
    }

    setup = get_setup_state()
    if setup is not None:
        initial["openai_api_key"] = str(getattr(setup, "openai_api_key", "") or "").strip()
        initial["twilio_account_sid"] = str(getattr(setup, "twilio_account_sid", "") or "").strip()
        initial["twilio_auth_token"] = str(getattr(setup, "twilio_auth_token", "") or "").strip()
        initial["twilio_from_number"] = str(getattr(setup, "twilio_from_number", "") or "").strip()

    microsoft_app, _site = _social_app_for_provider("microsoft")
    if microsoft_app is not None:
        initial["microsoft_client_id"] = str(getattr(microsoft_app, "client_id", "") or "").strip()
        initial["microsoft_client_secret"] = str(getattr(microsoft_app, "secret", "") or "").strip()
        microsoft_settings = dict(getattr(microsoft_app, "settings", {}) or {})
        initial["microsoft_tenant_id"] = str(microsoft_settings.get("tenant") or "").strip()

    github_app, _site = _social_app_for_provider("github")
    if github_app is not None:
        initial["github_client_id"] = str(getattr(github_app, "client_id", "") or "").strip()
        initial["github_client_secret"] = str(getattr(github_app, "secret", "") or "").strip()

    return initial


def setup_welcome(request):
    if is_setup_complete():
        if request.user.is_authenticated:
            return redirect("home")
        return redirect(settings.LOGIN_URL)

    connector_runtime = _connector_runtime_context(request)

    User = get_user_model()
    try:
        users_exist = User.objects.exists()
    except (OperationalError, ProgrammingError):
        users_exist = False
        errors = ["Database is not initialized. Run `python manage.py migrate` first."]
        return render(
            request,
            "pages/setup_welcome.html",
            {
                "errors": errors,
                "users_exist": users_exist,
                "initial": {
                    "openai_api_key": "",
                    "microsoft_tenant_id": "",
                    "microsoft_client_id": "",
                    "microsoft_client_secret": "",
                    "github_client_id": "",
                    "github_client_secret": "",
                    "twilio_account_sid": "",
                    "twilio_auth_token": "",
                    "twilio_from_number": "",
                    "admin_username": "",
                },
                **connector_runtime,
            },
        )

    errors: list[str] = []
    initial_step = "1"
    initial = {
        "openai_api_key": "",
        "microsoft_tenant_id": "",
        "microsoft_client_id": "",
        "microsoft_client_secret": "",
        "github_client_id": "",
        "github_client_secret": "",
        "twilio_account_sid": "",
        "twilio_auth_token": "",
        "twilio_from_number": "",
        "admin_username": "",
    }

    if request.method == "POST":
        setup_action = (request.POST.get("setup_action") or "").strip().lower() or "complete"
        posted_step = (request.POST.get("setup_step") or "").strip()
        if posted_step in {"1", "2"}:
            initial_step = posted_step
        initial["openai_api_key"] = (request.POST.get("openai_api_key") or "").strip()
        initial["microsoft_tenant_id"] = (request.POST.get("microsoft_tenant_id") or "").strip()
        initial["microsoft_client_id"] = (request.POST.get("microsoft_client_id") or "").strip()
        initial["microsoft_client_secret"] = (request.POST.get("microsoft_client_secret") or "").strip()
        initial["github_client_id"] = (request.POST.get("github_client_id") or "").strip()
        initial["github_client_secret"] = (request.POST.get("github_client_secret") or "").strip()
        initial["twilio_account_sid"] = (request.POST.get("twilio_account_sid") or "").strip()
        initial["twilio_auth_token"] = (request.POST.get("twilio_auth_token") or "").strip()
        initial["twilio_from_number"] = (request.POST.get("twilio_from_number") or "").strip()
        initial["admin_username"] = (request.POST.get("admin_username") or "").strip()
        admin_password = (request.POST.get("admin_password") or "").strip()
        admin_password_confirm = (request.POST.get("admin_password_confirm") or "").strip()
        has_any_microsoft_values = any(
            [
                initial["microsoft_tenant_id"],
                initial["microsoft_client_id"],
                initial["microsoft_client_secret"],
            ]
        )
        has_full_microsoft_values = all(
            [
                initial["microsoft_tenant_id"],
                initial["microsoft_client_id"],
                initial["microsoft_client_secret"],
            ]
        )
        has_any_github_values = any([initial["github_client_id"], initial["github_client_secret"]])
        has_full_github_values = all([initial["github_client_id"], initial["github_client_secret"]])
        has_any_twilio_values = any(
            [
                initial["twilio_account_sid"],
                initial["twilio_auth_token"],
                initial["twilio_from_number"],
            ]
        )
        has_full_twilio_values = all(
            [
                initial["twilio_account_sid"],
                initial["twilio_auth_token"],
                initial["twilio_from_number"],
            ]
        )

        if not users_exist:
            if not initial["admin_username"]:
                errors.append("Admin username is required.")
            elif not _GITHUB_USERNAME_RE.fullmatch(initial["admin_username"]):
                errors.append(
                    "Admin username must follow GitHub rules: 1-39 letters/numbers/hyphens, "
                    "no leading/trailing hyphen, and no consecutive hyphens."
                )
            if not admin_password:
                errors.append("Admin password is required.")
            if admin_password and len(admin_password) < 8:
                errors.append("Admin password must be at least 8 characters.")
            if admin_password != admin_password_confirm:
                errors.append("Admin password confirmation does not match.")
            if initial["admin_username"] and User.objects.filter(username__iexact=initial["admin_username"]).exists():
                errors.append("That admin username already exists.")
        if has_any_microsoft_values and not has_full_microsoft_values:
            errors.append(
                "To configure Microsoft Entra, provide Tenant ID, Client ID, and Client Secret Value."
            )
        if has_any_github_values and not has_full_github_values:
            errors.append("To configure GitHub OAuth, provide Client ID and Client Secret.")
        if has_any_twilio_values and not has_full_twilio_values:
            errors.append("To configure Twilio alerts, provide Account SID, Auth Token, and a From number.")

        if not errors:
            setup = get_or_create_setup_state()
            if setup is None:
                errors.append("Setup database is not ready yet. Run migrations and try again.")
            else:
                setup.openai_api_key = initial["openai_api_key"]
                setup.twilio_account_sid = initial["twilio_account_sid"]
                setup.twilio_auth_token = initial["twilio_auth_token"]
                setup.twilio_from_number = initial["twilio_from_number"]
                setup.is_completed = True
                setup.save(
                    update_fields=[
                        "openai_api_key",
                        "twilio_account_sid",
                        "twilio_auth_token",
                        "twilio_from_number",
                        "is_completed",
                        "updated_at",
                    ]
                )
                if has_full_microsoft_values:
                    try:
                        microsoft_app, site = _social_app_for_provider("microsoft")
                        if microsoft_app is None:
                            microsoft_app = SocialApp(provider="microsoft", name="Microsoft Entra")
                        microsoft_app.client_id = initial["microsoft_client_id"]
                        microsoft_app.secret = initial["microsoft_client_secret"]
                        app_settings = dict(microsoft_app.settings or {})
                        app_settings["tenant"] = initial["microsoft_tenant_id"]
                        microsoft_app.settings = app_settings
                        microsoft_app.save()
                        if site is not None:
                            microsoft_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save Microsoft Entra social app settings.")
                if has_full_github_values:
                    try:
                        github_app, site = _social_app_for_provider("github")
                        if github_app is None:
                            github_app = SocialApp(provider="github", name="GitHub OAuth")
                        github_app.client_id = initial["github_client_id"]
                        github_app.secret = initial["github_client_secret"]
                        github_settings = dict(github_app.settings or {})
                        github_settings["scope"] = ["read:user", "user:email"]
                        github_app.settings = github_settings
                        github_app.save()
                        if site is not None:
                            github_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save GitHub social app settings.")

            if not errors and not users_exist:
                User.objects.create_superuser(
                    username=initial["admin_username"],
                    email="",
                    password=admin_password,
                )

            if not errors:
                if setup_action == "test_microsoft" and has_full_microsoft_values:
                    try:
                        microsoft_login_url = reverse("microsoft_login")
                    except NoReverseMatch:
                        microsoft_login_url = "/accounts/microsoft/login/"
                    messages.success(request, "Setup saved. Continue with Microsoft sign-in to test login.")
                    return redirect(f"{microsoft_login_url}?process=login")
                if setup_action == "test_github" and has_full_github_values:
                    try:
                        github_login_url = reverse("github_login")
                    except NoReverseMatch:
                        github_login_url = "/accounts/github/login/"
                    messages.success(request, "Setup saved. Continue with GitHub sign-in to test login.")
                    return redirect(f"{github_login_url}?process=login")
                messages.success(request, "Setup completed. Sign in with your admin account.")
                return redirect(settings.LOGIN_URL)

    return render(
        request,
        "pages/setup_welcome.html",
        {
            "errors": errors,
            "users_exist": users_exist,
            "initial": initial,
            "initial_step": initial_step,
            **connector_runtime,
        },
    )


@login_required
def home(request):
    resources = list_resources(request.user)
    total_resources = len(resources)
    now_utc = datetime.now(timezone.utc)
    timeline_hours = 12
    timeline_start = (now_utc - timedelta(hours=timeline_hours - 1)).replace(
        minute=0,
        second=0,
        microsecond=0,
    )
    day_start = now_utc - timedelta(hours=24)
    slot_seconds = 3600

    health_timeline: list[dict[str, int | str]] = []
    cloud_log_timeline: list[dict[str, int | str]] = []
    for idx in range(timeline_hours):
        bucket_time = timeline_start + timedelta(hours=idx)
        label = bucket_time.strftime("%H:%M")
        health_timeline.append(
            {
                "label": label,
                "healthy": 0,
                "unhealthy": 0,
                "unknown": 0,
            }
        )
        cloud_log_timeline.append(
            {
                "label": label,
                "error": 0,
                "warning": 0,
                "info": 0,
            }
        )

    status_counts = {"healthy": 0, "unhealthy": 0, "unknown": 0}
    log_counts_24h = {"error": 0, "warning": 0, "info": 0}
    checks_total_24h = 0
    logs_total_24h = 0
    latency_samples: list[float] = []
    resource_rows: list[dict[str, object]] = []

    for item in resources:
        resource_status = _normalize_health_status(item.last_status)
        status_counts[resource_status] += 1

        try:
            check_items = list_resource_checks(request.user, item.resource_uuid, limit=80)
        except Exception:
            check_items = []
        try:
            log_items = list_resource_logs(request.user, item.resource_uuid, limit=180)
        except Exception:
            log_items = []

        latest_checked_dt = _parse_runtime_timestamp(item.last_checked_at)
        latest_checked_raw = str(item.last_checked_at or "").strip()
        latest_latency_ms: float | None = None
        error_logs_24h = 0

        if check_items:
            first_check = check_items[0]
            candidate_checked_raw = str(first_check.checked_at or "").strip()
            if candidate_checked_raw:
                latest_checked_raw = candidate_checked_raw
            candidate_checked_dt = _parse_runtime_timestamp(candidate_checked_raw)
            if candidate_checked_dt is not None:
                latest_checked_dt = candidate_checked_dt

        for check in check_items:
            check_dt = _parse_runtime_timestamp(check.checked_at)
            if check_dt is not None and check_dt >= day_start:
                checks_total_24h += 1
            if check_dt is not None and check_dt >= timeline_start:
                slot = int((check_dt - timeline_start).total_seconds() // slot_seconds)
                if 0 <= slot < timeline_hours:
                    status_key = _normalize_health_status(check.status)
                    health_timeline[slot][status_key] = int(health_timeline[slot][status_key]) + 1

            if check.latency_ms is None:
                continue
            try:
                latency_value = float(check.latency_ms)
            except (TypeError, ValueError):
                continue
            if latency_value < 0:
                continue
            latency_samples.append(latency_value)
            if latest_latency_ms is None:
                latest_latency_ms = latency_value

        for entry in log_items:
            log_dt = _parse_runtime_timestamp(str(entry.get("timestamp") or ""))
            level_key = _normalize_log_level_bucket(str(entry.get("level") or "info"))
            if log_dt is not None and log_dt >= day_start:
                logs_total_24h += 1
                log_counts_24h[level_key] += 1
                if level_key == "error":
                    error_logs_24h += 1
            if log_dt is not None and log_dt >= timeline_start:
                slot = int((log_dt - timeline_start).total_seconds() // slot_seconds)
                if 0 <= slot < timeline_hours:
                    cloud_log_timeline[slot][level_key] = int(cloud_log_timeline[slot][level_key]) + 1

        checked_display_value = ""
        if latest_checked_dt is not None:
            checked_display_value = _format_display_time(latest_checked_dt.isoformat())
        elif latest_checked_raw:
            checked_display_value = _format_display_time(latest_checked_raw)
        if not checked_display_value:
            checked_display_value = "—"

        detail_url = _resource_detail_url_for_uuid(actor=request.user, resource_uuid=item.resource_uuid)
        resource_rows.append(
            {
                "name": str(item.name or "Unnamed resource"),
                "resource_type": str(item.resource_type or "").strip() or "resource",
                "status": resource_status,
                "status_label": resource_status.title(),
                "status_tone": _status_tone_for_health(resource_status),
                "latency_display": f"{latest_latency_ms:.1f} ms" if latest_latency_ms is not None else "—",
                "error_logs_24h": int(error_logs_24h),
                "last_checked_display": checked_display_value,
                "detail_url": detail_url,
                "target": str(item.target or "—"),
                "last_error": str(item.last_error or "").strip(),
                "sort_rank": 0 if resource_status == "unhealthy" else (1 if resource_status == "unknown" else 2),
                "sort_checked_ts": float(latest_checked_dt.timestamp()) if latest_checked_dt is not None else 0.0,
            }
        )

    resource_rows.sort(
        key=lambda row: (
            int(row.get("sort_rank", 2)),
            -int(row.get("error_logs_24h", 0)),
            -float(row.get("sort_checked_ts", 0.0)),
        )
    )

    top_rows = []
    for row in resource_rows[:8]:
        cleaned = dict(row)
        cleaned.pop("sort_rank", None)
        cleaned.pop("sort_checked_ts", None)
        top_rows.append(cleaned)

    attention_rows = []
    for row in resource_rows:
        if row.get("status") != "healthy" or int(row.get("error_logs_24h", 0)) > 0:
            cleaned = dict(row)
            cleaned.pop("sort_rank", None)
            cleaned.pop("sort_checked_ts", None)
            attention_rows.append(cleaned)
        if len(attention_rows) >= 5:
            break

    healthy_count = status_counts["healthy"]
    unhealthy_count = status_counts["unhealthy"]
    unknown_count = status_counts["unknown"]
    alerts_open = unhealthy_count + unknown_count
    reliability_pct = round((healthy_count / total_resources) * 100.0, 1) if total_resources else 0.0
    log_error_rate_pct = round((log_counts_24h["error"] / logs_total_24h) * 100.0, 1) if logs_total_24h else 0.0
    avg_latency_ms = round(sum(latency_samples) / len(latency_samples), 1) if latency_samples else None
    notification_snapshot = list_user_notifications(request.user, limit=8)

    return render(
        request,
        "pages/home.html",
        {
            "resources_total": total_resources,
            "healthy_resources": healthy_count,
            "unhealthy_resources": unhealthy_count,
            "unknown_resources": unknown_count,
            "alerts_open": alerts_open,
            "reliability_pct": reliability_pct,
            "avg_latency_ms": avg_latency_ms,
            "checks_total_24h": checks_total_24h,
            "logs_total_24h": logs_total_24h,
            "log_errors_24h": int(log_counts_24h["error"]),
            "log_warnings_24h": int(log_counts_24h["warning"]),
            "log_info_24h": int(log_counts_24h["info"]),
            "log_error_rate_pct": log_error_rate_pct,
            "resource_rows": top_rows,
            "attention_rows": attention_rows,
            "health_timeline": health_timeline,
            "cloud_log_timeline": cloud_log_timeline,
            "dashboard_generated_display": _format_display_time(now_utc.isoformat()),
            "notification_unread_count": int(notification_snapshot.get("unread_count") or 0),
        },
    )


@login_required
def notifications_feed(request):
    raw_limit = (request.GET.get("limit") or "").strip()
    try:
        limit = int(raw_limit or 12)
    except (TypeError, ValueError):
        limit = 12
    payload = list_user_notifications(request.user, limit=max(1, min(limit, 50)))
    return JsonResponse(payload)


@login_required
@require_POST
def notifications_mark_all_read(request):
    updated = mark_all_user_notifications_read(request.user)
    return JsonResponse({"status": "ok", "updated": updated})


@login_required
@require_POST
def notifications_clear_all(request):
    deleted = clear_user_notifications(request.user)
    return JsonResponse({"status": "ok", "deleted": deleted})


@login_required
def app_settings(request):
    active_tab = (request.GET.get('tab') or 'account').strip().lower()
    if active_tab not in {'account', 'api-key', 'connectors', 'admin'}:
        active_tab = 'account'
    if active_tab == "admin" and not request.user.is_superuser:
        active_tab = "account"

    notification_settings = UserNotificationSettings.objects.filter(user=request.user).first()
    account_phone_number = str(getattr(notification_settings, "phone_number", "") or "").strip()

    if request.method == "POST" and active_tab == "account":
        phone_number = (request.POST.get("phone_number") or "").strip()
        if len(phone_number) > 32:
            messages.warning(request, "Phone number is too long.")
            return redirect(f"{reverse('app_settings')}?tab=account")
        if phone_number and not re.match(r"^[0-9+()\\-\\s]{6,32}$", phone_number):
            messages.warning(request, "Use a valid phone number format.")
            return redirect(f"{reverse('app_settings')}?tab=account")
        settings_row, _created = UserNotificationSettings.objects.get_or_create(user=request.user)
        settings_row.phone_number = phone_number
        settings_row.save(update_fields=["phone_number", "updated_at"])
        messages.success(request, "Notification phone number updated.")
        return redirect(f"{reverse('app_settings')}?tab=account")

    admin_context = {
        "admin_setup_ready": False,
        "monitoring_enabled": True,
        "maintenance_mode": False,
        "maintenance_message": "",
        "default_model": get_alshival_default_model(),
        "microsoft_connector_configured": False,
        "microsoft_login_enabled": False,
        "github_connector_configured": False,
        "github_login_enabled": False,
        "ask_github_mcp_enabled": False,
    }
    if request.user.is_superuser:
        microsoft_connector_configured = is_microsoft_connector_configured()
        github_connector_configured = is_github_connector_configured()
        setup = get_or_create_setup_state()
        if setup is not None:
            admin_context.update(
                {
                    "admin_setup_ready": True,
                    "monitoring_enabled": bool(getattr(setup, "monitoring_enabled", True)),
                    "maintenance_mode": bool(getattr(setup, "maintenance_mode", False)),
                    "maintenance_message": str(getattr(setup, "maintenance_message", "") or "").strip(),
                    "default_model": str(getattr(setup, "default_model", "") or "").strip() or get_alshival_default_model(),
                    "microsoft_connector_configured": microsoft_connector_configured,
                    "microsoft_login_enabled": bool(getattr(setup, "microsoft_login_enabled", False)),
                    "github_connector_configured": github_connector_configured,
                    "github_login_enabled": bool(getattr(setup, "github_login_enabled", False)),
                    "ask_github_mcp_enabled": bool(getattr(setup, "ask_github_mcp_enabled", False)),
                }
            )

    if request.method == "POST" and active_tab == "admin":
        if not request.user.is_superuser:
            raise PermissionDenied("Superuser access required.")
        setup = get_or_create_setup_state()
        if setup is None:
            messages.warning(request, "Setup database is not ready yet. Run migrations first.")
            return redirect(f"{reverse('app_settings')}?tab=admin")

        monitoring_enabled = _post_flag(request.POST, "monitoring_enabled")
        maintenance_mode = _post_flag(request.POST, "maintenance_mode")
        maintenance_message = str(request.POST.get("maintenance_message") or "").strip()
        default_model = str(request.POST.get("default_model") or "").strip()
        microsoft_connector_configured = is_microsoft_connector_configured()
        microsoft_login_enabled = bool(getattr(setup, "microsoft_login_enabled", False))
        if microsoft_connector_configured:
            microsoft_login_enabled = _post_flag(request.POST, "microsoft_login_enabled")
        github_connector_configured = is_github_connector_configured()
        github_login_enabled = bool(getattr(setup, "github_login_enabled", False))
        ask_github_mcp_enabled = bool(getattr(setup, "ask_github_mcp_enabled", False))
        if github_connector_configured:
            github_login_enabled = _post_flag(request.POST, "github_login_enabled")
            ask_github_mcp_enabled = _post_flag(request.POST, "ask_github_mcp_enabled")
        if len(maintenance_message) > 255:
            maintenance_message = maintenance_message[:255].strip()
        if len(default_model) > 120:
            default_model = default_model[:120].strip()
        if not default_model:
            default_model = get_alshival_default_model()

        setup.monitoring_enabled = monitoring_enabled
        setup.maintenance_mode = maintenance_mode
        setup.maintenance_message = maintenance_message
        setup.default_model = default_model
        setup.microsoft_login_enabled = microsoft_login_enabled
        setup.github_login_enabled = github_login_enabled
        setup.ask_github_mcp_enabled = ask_github_mcp_enabled
        setup.save(
            update_fields=[
                "monitoring_enabled",
                "maintenance_mode",
                "maintenance_message",
                "default_model",
                "microsoft_login_enabled",
                "github_login_enabled",
                "ask_github_mcp_enabled",
                "updated_at",
            ]
        )
        messages.success(request, "Alshival admin settings updated.")
        return redirect(f"{reverse('app_settings')}?tab=admin")

    ingest_api_key = get_ingest_api_key()
    if ingest_api_key:
        if len(ingest_api_key) > 12:
            api_key_preview = f"{ingest_api_key[:6]}...{ingest_api_key[-4:]}"
        else:
            api_key_preview = "Configured"
    else:
        api_key_preview = "No key configured"

    account_api_keys = list_user_api_keys(request.user, "account")
    latest_api_key_value = str(request.session.pop("latest_created_api_key", "") or "").strip()
    latest_api_key_type = str(request.session.pop("latest_created_api_key_type", "") or "").strip()

    return render(
        request,
        'pages/settings.html',
        {
            'active_tab': active_tab,
            'api_key_preview': api_key_preview,
            'account_api_keys': account_api_keys,
            'latest_api_key_value': latest_api_key_value,
            'latest_api_key_type': latest_api_key_type,
            'account_phone_number': account_phone_number,
            **admin_context,
        },
    )


@superuser_required
def connector_settings(request):
    errors: list[str] = []
    initial = _connector_initial_values()
    connector_runtime = _connector_runtime_context(request)

    if request.method == "POST":
        setup_action = (request.POST.get("setup_action") or "").strip().lower() or "complete"
        initial["openai_api_key"] = (request.POST.get("openai_api_key") or "").strip()
        initial["microsoft_tenant_id"] = (request.POST.get("microsoft_tenant_id") or "").strip()
        initial["microsoft_client_id"] = (request.POST.get("microsoft_client_id") or "").strip()
        initial["microsoft_client_secret"] = (request.POST.get("microsoft_client_secret") or "").strip()
        initial["github_client_id"] = (request.POST.get("github_client_id") or "").strip()
        initial["github_client_secret"] = (request.POST.get("github_client_secret") or "").strip()
        initial["twilio_account_sid"] = (request.POST.get("twilio_account_sid") or "").strip()
        initial["twilio_auth_token"] = (request.POST.get("twilio_auth_token") or "").strip()
        initial["twilio_from_number"] = (request.POST.get("twilio_from_number") or "").strip()

        has_any_microsoft_values = any(
            [
                initial["microsoft_tenant_id"],
                initial["microsoft_client_id"],
                initial["microsoft_client_secret"],
            ]
        )
        has_full_microsoft_values = all(
            [
                initial["microsoft_tenant_id"],
                initial["microsoft_client_id"],
                initial["microsoft_client_secret"],
            ]
        )
        has_any_github_values = any([initial["github_client_id"], initial["github_client_secret"]])
        has_full_github_values = all([initial["github_client_id"], initial["github_client_secret"]])
        has_any_twilio_values = any(
            [
                initial["twilio_account_sid"],
                initial["twilio_auth_token"],
                initial["twilio_from_number"],
            ]
        )
        has_full_twilio_values = all(
            [
                initial["twilio_account_sid"],
                initial["twilio_auth_token"],
                initial["twilio_from_number"],
            ]
        )

        if has_any_microsoft_values and not has_full_microsoft_values:
            errors.append("To configure Microsoft Entra, provide Tenant ID, Client ID, and Client Secret Value.")
        if has_any_github_values and not has_full_github_values:
            errors.append("To configure GitHub OAuth, provide Client ID and Client Secret.")
        if has_any_twilio_values and not has_full_twilio_values:
            errors.append("To configure Twilio alerts, provide Account SID, Auth Token, and a From number.")

        if not errors:
            setup = get_or_create_setup_state()
            if setup is None:
                errors.append("Setup database is not ready yet. Run migrations and try again.")
            else:
                setup.openai_api_key = initial["openai_api_key"]
                setup.twilio_account_sid = initial["twilio_account_sid"]
                setup.twilio_auth_token = initial["twilio_auth_token"]
                setup.twilio_from_number = initial["twilio_from_number"]
                setup.save(
                    update_fields=[
                        "openai_api_key",
                        "twilio_account_sid",
                        "twilio_auth_token",
                        "twilio_from_number",
                        "updated_at",
                    ]
                )

                if has_full_microsoft_values:
                    try:
                        microsoft_app, site = _social_app_for_provider("microsoft")
                        if microsoft_app is None:
                            microsoft_app = SocialApp(provider="microsoft", name="Microsoft Entra")
                        microsoft_app.client_id = initial["microsoft_client_id"]
                        microsoft_app.secret = initial["microsoft_client_secret"]
                        app_settings = dict(microsoft_app.settings or {})
                        app_settings["tenant"] = initial["microsoft_tenant_id"]
                        microsoft_app.settings = app_settings
                        microsoft_app.save()
                        if site is not None:
                            microsoft_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save Microsoft Entra social app settings.")

                if has_full_github_values:
                    try:
                        github_app, site = _social_app_for_provider("github")
                        if github_app is None:
                            github_app = SocialApp(provider="github", name="GitHub OAuth")
                        github_app.client_id = initial["github_client_id"]
                        github_app.secret = initial["github_client_secret"]
                        github_settings = dict(github_app.settings or {})
                        github_settings["scope"] = ["read:user", "user:email"]
                        github_app.settings = github_settings
                        github_app.save()
                        if site is not None:
                            github_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save GitHub social app settings.")

        if not errors:
            if setup_action == "test_microsoft" and has_full_microsoft_values:
                try:
                    microsoft_login_url = reverse("microsoft_login")
                except NoReverseMatch:
                    microsoft_login_url = "/accounts/microsoft/login/"
                messages.success(request, "Connector settings saved. Continue with Microsoft sign-in to test login.")
                return redirect(f"{microsoft_login_url}?process=login")
            if setup_action == "test_github" and has_full_github_values:
                try:
                    github_login_url = reverse("github_login")
                except NoReverseMatch:
                    github_login_url = "/accounts/github/login/"
                messages.success(request, "Connector settings saved. Continue with GitHub sign-in to test login.")
                return redirect(f"{github_login_url}?process=login")

            messages.success(request, "Connector settings updated.")
            return redirect("connector_settings")

    return render(
        request,
        "pages/connectors_settings.html",
        {
            "errors": errors,
            "initial": initial,
            **connector_runtime,
        },
    )


@superuser_required
def alshival_admin(request):
    return redirect(f"{reverse('app_settings')}?tab=admin")


@login_required
@require_POST
def create_account_api_key_item(request):
    key_name = (request.POST.get("name") or "").strip()
    _key_id, raw_api_key = create_account_api_key(request.user, key_name)
    request.session["latest_created_api_key"] = raw_api_key
    request.session["latest_created_api_key_type"] = "account"
    messages.success(request, "Account API key created.")
    return redirect(f"{reverse('app_settings')}?tab=api-key")


@superuser_required
def team_directory(request):
    active_tab = (request.GET.get('tab') or 'users').strip().lower()
    if active_tab not in {'users', 'teams'}:
        active_tab = 'users'
    status_code = (request.GET.get('status') or '').strip()
    status_message, status_tone = _team_directory_status_context(status_code)
    selected_user_raw = (request.GET.get('user') or '').strip()
    selected_team_raw = (request.GET.get('team') or '').strip()

    User = get_user_model()
    teams = Group.objects.all().order_by('name').prefetch_related('user_set')
    users = User.objects.all().order_by('username', 'email').prefetch_related('groups')
    user_ids = [int(item.id) for item in users]
    feature_lookup = _feature_access_lookup(user_ids)

    user_rows: list[dict[str, object]] = []
    for item in users:
        team_names = sorted(
            [str(group.name) for group in item.groups.all()],
            key=lambda value: value.lower(),
        )
        feature_keys = sorted(feature_lookup.get(int(item.id), set()))
        if item.is_superuser:
            role_label = "Platform Admin"
            role_tone = "warning"
        elif item.is_staff:
            role_label = "Operations Staff"
            role_tone = "info"
        else:
            role_label = "Standard User"
            role_tone = "success"
        user_rows.append(
            {
                "id": int(item.id),
                "username": str(item.username or ""),
                "email": str(item.email or ""),
                "is_active": bool(item.is_active),
                "is_staff": bool(item.is_staff),
                "is_superuser": bool(item.is_superuser),
                "role_label": role_label,
                "role_tone": role_tone,
                "team_names": team_names,
                "feature_keys": feature_keys,
                "joined_display": _format_display_time(
                    item.date_joined.isoformat() if getattr(item, "date_joined", None) else ""
                ),
                "last_login_display": _format_display_time(
                    item.last_login.isoformat() if getattr(item, "last_login", None) else ""
                ),
            }
        )

    selected_user_id = 0
    if selected_user_raw.isdigit():
        selected_user_id = int(selected_user_raw)
    if user_rows and selected_user_id not in {int(row["id"]) for row in user_rows}:
        selected_user_id = int(user_rows[0]["id"])
    selected_user = None
    for row in user_rows:
        if int(row["id"]) == selected_user_id:
            selected_user = row
            break
    if selected_user is None and user_rows:
        selected_user = user_rows[0]
        selected_user_id = int(selected_user["id"])

    team_rows: list[dict[str, object]] = []
    for item in teams:
        members = sorted(
            list(item.user_set.all()),
            key=lambda member: (str(member.username or "").lower(), int(member.id)),
        )
        member_ids = [int(member.id) for member in members]
        member_names = [str(member.username or "") for member in members]
        team_rows.append(
            {
                "id": int(item.id),
                "name": str(item.name or ""),
                "member_ids": member_ids,
                "member_names": member_names,
                "member_count": len(member_ids),
            }
        )

    selected_team_id = 0
    if selected_team_raw.isdigit():
        selected_team_id = int(selected_team_raw)
    if team_rows and selected_team_id not in {int(row["id"]) for row in team_rows}:
        selected_team_id = int(team_rows[0]["id"])
    selected_team = None
    for row in team_rows:
        if int(row["id"]) == selected_team_id:
            selected_team = row
            break
    if selected_team is None and team_rows:
        selected_team = team_rows[0]
        selected_team_id = int(selected_team["id"])

    context = {
        'active_tab': active_tab,
        'teams': teams,
        'users': users,
        'user_rows': user_rows,
        'selected_user_id': selected_user_id,
        'selected_user': selected_user,
        'team_rows': team_rows,
        'selected_team_id': selected_team_id,
        'selected_team': selected_team,
        'available_features': _TEAM_DIRECTORY_FEATURES,
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

    created = Group.objects.create(name=name)
    return _redirect_team_directory(tab='teams', status='team_created', team_id=created.id)


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
    return _redirect_team_directory(tab='teams', status='team_renamed', team_id=team.id)


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
    return _redirect_team_directory(tab='teams', status='team_member_added', team_id=team.id)


@require_POST
@superuser_required
def team_directory_remove_team_member(request, team_id: int, user_id: int):
    team = get_object_or_404(Group, id=team_id)
    User = get_user_model()
    user = get_object_or_404(User, id=user_id)
    team.user_set.remove(user)
    return _redirect_team_directory(tab='teams', status='team_member_removed', team_id=team.id)


@require_POST
@superuser_required
def team_directory_update_team_members(request, team_id: int):
    team = get_object_or_404(Group, id=team_id)
    raw_user_ids = request.POST.getlist("user_ids")
    unique_user_ids: list[int] = []
    for value in raw_user_ids:
        cleaned = str(value or "").strip()
        if not cleaned.isdigit():
            continue
        resolved = int(cleaned)
        if resolved not in unique_user_ids:
            unique_user_ids.append(resolved)

    User = get_user_model()
    users = list(User.objects.filter(id__in=unique_user_ids))
    team.user_set.set(users)
    return _redirect_team_directory(tab='teams', status='team_members_updated', team_id=team.id)


@require_POST
@superuser_required
def team_directory_create_user(request):
    username = (request.POST.get("username") or "").strip()
    email = (request.POST.get("email") or "").strip().lower()
    password = (request.POST.get("password") or "").strip()
    password_confirm = (request.POST.get("password_confirm") or "").strip()
    make_staff = _post_flag(request, "is_staff")
    make_superuser = _post_flag(request, "is_superuser")
    is_active = _post_flag(request, "is_active")
    team_names = _normalize_team_names(request.POST.getlist("team_names"))
    feature_keys = _normalize_feature_keys(request.POST.getlist("feature_keys"))

    if not username:
        return _redirect_team_directory(tab='users', status='user_username_required')
    if not _GITHUB_USERNAME_RE.fullmatch(username):
        return _redirect_team_directory(tab='users', status='user_username_invalid')
    User = get_user_model()
    if User.objects.filter(username__iexact=username).exists():
        return _redirect_team_directory(tab='users', status='user_username_exists')
    if not password:
        return _redirect_team_directory(tab='users', status='user_password_required')
    if len(password) < 8:
        return _redirect_team_directory(tab='users', status='user_password_too_short')
    if password != password_confirm:
        return _redirect_team_directory(tab='users', status='user_password_mismatch')

    with transaction.atomic():
        created = User.objects.create_user(
            username=username,
            email=email,
            password=password,
            is_active=is_active,
            is_staff=True if make_superuser else make_staff,
            is_superuser=make_superuser,
        )
        groups = list(Group.objects.filter(name__in=team_names))
        created.groups.set(groups)
        _sync_user_feature_access(user=created, feature_keys=feature_keys, actor=request.user)

    return _redirect_team_directory(tab='users', status='user_created', user_id=int(created.id))


@require_POST
@superuser_required
def team_directory_update_user(request, user_id: int):
    User = get_user_model()
    user = get_object_or_404(User, id=user_id)

    username = (request.POST.get("username") or "").strip()
    email = (request.POST.get("email") or "").strip().lower()
    new_password = (request.POST.get("password") or "").strip()
    password_confirm = (request.POST.get("password_confirm") or "").strip()
    make_staff = _post_flag(request, "is_staff")
    make_superuser = _post_flag(request, "is_superuser")
    is_active = _post_flag(request, "is_active")
    team_names = _normalize_team_names(request.POST.getlist("team_names"))
    feature_keys = _normalize_feature_keys(request.POST.getlist("feature_keys"))

    if not username:
        return _redirect_team_directory(tab='users', status='user_username_required', user_id=user.id)
    if not _GITHUB_USERNAME_RE.fullmatch(username):
        return _redirect_team_directory(tab='users', status='user_username_invalid', user_id=user.id)
    if User.objects.exclude(id=user.id).filter(username__iexact=username).exists():
        return _redirect_team_directory(tab='users', status='user_username_exists', user_id=user.id)
    if user.id == request.user.id and not make_superuser:
        return _redirect_team_directory(tab='users', status='cannot_demote_self', user_id=user.id)
    if user.is_superuser and not make_superuser and User.objects.filter(is_superuser=True).count() <= 1:
        return _redirect_team_directory(tab='users', status='cannot_remove_last_superuser', user_id=user.id)
    if user.is_superuser and not is_active and User.objects.filter(is_superuser=True).count() <= 1:
        return _redirect_team_directory(tab='users', status='cannot_remove_last_superuser', user_id=user.id)
    if new_password:
        if len(new_password) < 8:
            return _redirect_team_directory(tab='users', status='user_password_too_short', user_id=user.id)
        if new_password != password_confirm:
            return _redirect_team_directory(tab='users', status='user_password_mismatch', user_id=user.id)

    with transaction.atomic():
        update_fields: list[str] = []
        if user.username != username:
            user.username = username
            update_fields.append("username")
        if user.email != email:
            user.email = email
            update_fields.append("email")
        resolved_is_staff = True if make_superuser else make_staff
        if user.is_staff != resolved_is_staff:
            user.is_staff = resolved_is_staff
            update_fields.append("is_staff")
        if user.is_superuser != make_superuser:
            user.is_superuser = make_superuser
            update_fields.append("is_superuser")
        if user.is_active != is_active:
            user.is_active = is_active
            update_fields.append("is_active")
        if new_password:
            user.set_password(new_password)
            update_fields.append("password")
        if update_fields:
            user.save(update_fields=update_fields)

        groups = list(Group.objects.filter(name__in=team_names))
        user.groups.set(groups)
        _sync_user_feature_access(user=user, feature_keys=feature_keys, actor=request.user)

    return _redirect_team_directory(tab='users', status='user_updated', user_id=user.id)


@require_POST
@superuser_required
def team_directory_delete_user(request, user_id: int):
    User = get_user_model()
    user = get_object_or_404(User, id=user_id)
    if user.id == request.user.id:
        return _redirect_team_directory(tab='users', status='cannot_delete_self', user_id=user.id)
    if user.is_superuser and User.objects.filter(is_superuser=True).count() <= 1:
        return _redirect_team_directory(tab='users', status='cannot_delete_last_superuser', user_id=user.id)

    user.delete()
    return _redirect_team_directory(tab='users', status='user_deleted')


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
    is_active = _post_flag(request, 'is_active')
    team_names = _normalize_team_names(request.POST.getlist("team_names"))
    feature_keys = _normalize_feature_keys(request.POST.getlist("feature_keys"))

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
        user.is_active = is_active
        user.is_superuser = make_superuser
        user.is_staff = True if make_superuser else make_staff
        user.save()
    else:
        user = User.objects.create_user(
            username=username,
            email=email,
            password=temporary_password,
            is_active=is_active,
            is_staff=True if make_superuser else make_staff,
            is_superuser=make_superuser,
        )

    groups = list(Group.objects.filter(name__in=team_names))
    user.groups.set(groups)
    _sync_user_feature_access(user=user, feature_keys=feature_keys, actor=request.user)

    login_url = request.build_absolute_uri(settings.LOGIN_URL)
    subject = 'You are invited to Alshival'
    message = (
        "You have been invited to the Alshival console.\n\n"
        f"Username: {user.get_username()}\n"
        f"Temporary password: {temporary_password}\n"
        f"Login URL: {login_url}\n\n"
        "Please sign in and change your password immediately."
    )
    from_email = getattr(settings, 'DEFAULT_FROM_EMAIL', 'noreply@alshival.local')

    try:
        send_mail(subject, message, from_email, [email], fail_silently=False)
    except Exception:
        return _redirect_team_directory(tab='users', status='invite_created_email_failed')

    return _redirect_team_directory(tab='users', status='invite_sent')


@login_required
def wiki(request):
    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)

    requested_page_raw = (request.GET.get("page") or "").strip()
    requested_path = _normalize_wiki_path(requested_page_raw, "")
    member_teams = _ssh_team_choices_for_user(request.user)

    pages = list(_wiki_accessible_queryset(request.user).order_by("path", "title"))
    wiki_pages: list[dict[str, object]] = []
    for item in pages:
        team_names = sorted([str(team.name) for team in item.team_access.all()], key=lambda value: value.lower())
        wiki_pages.append(
            {
                "id": int(item.id),
                "title": str(item.title or ""),
                "path": str(item.path or ""),
                "is_draft": bool(item.is_draft),
                "team_names": team_names,
                "team_keys": [slugify(name) for name in team_names],
                "is_public": not team_names,
                "can_edit": _can_edit_wiki_page(actor=request.user, page=item),
                "updated_display": _format_display_time(item.updated_at.isoformat() if getattr(item, "updated_at", None) else ""),
            }
        )

    selected_page = None
    if requested_path:
        selected_page = next((item for item in pages if item.path == requested_path), None)
    if selected_page is None and pages:
        selected_page = pages[0]

    if not status_message and requested_page_raw and selected_page is None:
        normalized_requested = _normalize_wiki_path(requested_page_raw, "")
        if normalized_requested and WikiPage.objects.filter(path=normalized_requested).exists():
            status_message, status_tone = _wiki_status_context("wiki_no_access")
        else:
            status_message, status_tone = _wiki_status_context("wiki_page_not_found")

    selected_page_payload: dict[str, object] = {}
    selected_page_html_fallback = ""
    if selected_page is not None:
        selected_team_names = sorted(
            [str(team.name) for team in selected_page.team_access.all()],
            key=lambda value: value.lower(),
        )
        selected_page_payload = {
            "id": int(selected_page.id),
            "title": str(selected_page.title or ""),
            "path": str(selected_page.path or ""),
            "markdown": str(selected_page.body_markdown or ""),
            "is_draft": bool(selected_page.is_draft),
            "team_names": selected_team_names,
            "updated_display": _format_display_time(selected_page.updated_at.isoformat() if getattr(selected_page, "updated_at", None) else ""),
            "created_display": _format_display_time(selected_page.created_at.isoformat() if getattr(selected_page, "created_at", None) else ""),
            "can_edit": _can_edit_wiki_page(actor=request.user, page=selected_page),
        }
        selected_page_html_fallback = render_markdown_fallback(selected_page.body_markdown)

    return render(
        request,
        "pages/wiki.html",
        {
            "wiki_pages": wiki_pages,
            "member_teams": member_teams,
            "selected_page": selected_page,
            "selected_page_payload": selected_page_payload,
            "selected_page_html_fallback": selected_page_html_fallback,
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
def wiki_editor_new(request):
    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)
    member_teams = _ssh_team_choices_for_user(request.user)
    return render(
        request,
        "pages/wiki_editor.html",
        {
            "editor_mode": "create",
            "editor_page": None,
            "editor_payload": {
                "title": "",
                "path": "",
                "markdown": "",
                "team_names": [],
                "is_draft": True,
            },
            "member_teams": member_teams,
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
def wiki_editor(request, page_id: int):
    page = get_object_or_404(WikiPage.objects.prefetch_related("team_access"), id=page_id)
    if not _can_edit_wiki_page(actor=request.user, page=page):
        return _redirect_wiki(status="wiki_no_access", page_path=page.path)

    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)
    member_teams = _ssh_team_choices_for_user(request.user)
    page_team_names = sorted([str(item.name) for item in page.team_access.all()], key=lambda value: value.lower())

    return render(
        request,
        "pages/wiki_editor.html",
        {
            "editor_mode": "edit",
            "editor_page": page,
            "editor_payload": {
                "id": int(page.id),
                "title": str(page.title or ""),
                "path": str(page.path or ""),
                "markdown": str(page.body_markdown or ""),
                "team_names": page_team_names,
                "is_draft": bool(page.is_draft),
                "updated_display": _format_display_time(page.updated_at.isoformat() if getattr(page, "updated_at", None) else ""),
                "created_display": _format_display_time(page.created_at.isoformat() if getattr(page, "created_at", None) else ""),
            },
            "member_teams": member_teams,
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
@require_POST
def wiki_create_page(request):
    title = (request.POST.get("title") or "").strip()
    path = _normalize_wiki_path(request.POST.get("path") or "", title)
    body_markdown = str(request.POST.get("body_markdown") or "").replace("\r\n", "\n").replace("\r", "\n")
    team_names = _normalize_wiki_team_names(request.user, request.POST.getlist("team_names"))
    save_intent = (request.POST.get("save_intent") or "publish").strip().lower()
    is_draft = save_intent != "publish"

    if not title:
        return _redirect_wiki_editor_new(status="wiki_title_required")
    if not path:
        return _redirect_wiki_editor_new(status="wiki_path_required")
    if len(path) > 220:
        return _redirect_wiki_editor_new(status="wiki_path_invalid")
    if WikiPage.objects.filter(path__iexact=path).exists():
        return _redirect_wiki_editor_new(status="wiki_path_exists")

    with transaction.atomic():
        page = WikiPage.objects.create(
            path=path,
            title=title,
            is_draft=is_draft,
            body_markdown=body_markdown,
            body_html_fallback=render_markdown_fallback(body_markdown),
            created_by=request.user,
            updated_by=request.user,
        )
        teams = list(Group.objects.filter(name__in=team_names))
        page.team_access.set(teams)

    if is_draft:
        return _redirect_wiki_editor(page_id=page.id, status="wiki_draft_saved")
    return _redirect_wiki(status="wiki_page_created", page_path=page.path)


@login_required
@require_POST
def wiki_update_page(request, page_id: int):
    page = get_object_or_404(WikiPage.objects.prefetch_related("team_access"), id=page_id)
    if not _can_edit_wiki_page(actor=request.user, page=page):
        return _redirect_wiki(status="wiki_no_access", page_path=page.path)

    title = (request.POST.get("title") or "").strip()
    path = _normalize_wiki_path(request.POST.get("path") or "", title)
    body_markdown = str(request.POST.get("body_markdown") or "").replace("\r\n", "\n").replace("\r", "\n")
    team_names = _normalize_wiki_team_names(request.user, request.POST.getlist("team_names"))
    save_intent = (request.POST.get("save_intent") or "publish").strip().lower()
    is_draft = save_intent != "publish"

    if not title:
        return _redirect_wiki_editor(page_id=page.id, status="wiki_title_required")
    if not path:
        return _redirect_wiki_editor(page_id=page.id, status="wiki_path_required")
    if len(path) > 220:
        return _redirect_wiki_editor(page_id=page.id, status="wiki_path_invalid")
    if WikiPage.objects.exclude(id=page.id).filter(path__iexact=path).exists():
        return _redirect_wiki_editor(page_id=page.id, status="wiki_path_exists")

    was_draft = bool(page.is_draft)
    with transaction.atomic():
        page.path = path
        page.title = title
        page.is_draft = is_draft
        page.body_markdown = body_markdown
        page.body_html_fallback = render_markdown_fallback(body_markdown)
        page.updated_by = request.user
        page.save(update_fields=["path", "title", "is_draft", "body_markdown", "body_html_fallback", "updated_by", "updated_at"])

        teams = list(Group.objects.filter(name__in=team_names))
        page.team_access.set(teams)

    if is_draft:
        return _redirect_wiki_editor(page_id=page.id, status="wiki_draft_saved")
    if was_draft:
        return _redirect_wiki(status="wiki_page_published", page_path=page.path)
    return _redirect_wiki(status="wiki_page_updated", page_path=page.path)


@require_POST
@superuser_required
def wiki_delete_page(request, page_id: int):
    page = get_object_or_404(WikiPage, id=page_id)
    page.delete()
    return _redirect_wiki(status="wiki_page_deleted")


@login_required
def resources(request):
    resources = list_resources(request.user)
    resource_alerts = _resource_alerts(resources)
    for item in resources:
        item.detail_url = _resource_detail_url_for_uuid(actor=request.user, resource_uuid=item.resource_uuid)
    for alert in resource_alerts:
        alert["detail_url"] = _resource_detail_url_for_uuid(
            actor=request.user,
            resource_uuid=str(alert.get("resource_uuid") or "").strip(),
        )
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
                'team_names': item.team_names,
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
                'team_names': [item.team_name] if item.team_name else [],
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
def resource_detail(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if not _can_access_owner_resource(actor=request.user, owner=owner, resource_uuid=resource.resource_uuid):
        raise PermissionDenied("You do not have access to this resource.")
    if current_alias and not _resource_route_matches(current_alias=current_alias, route_kind=route_kind, route_value=username):
        redirect_url = _resource_route_redirect_url(
            current_alias=current_alias,
            endpoint_key="detail",
            resource_uuid=resource.resource_uuid,
        )
        if redirect_url:
            return redirect(redirect_url)

    active_route_kind = str(current_alias.route_kind if current_alias else route_kind or "user").strip().lower() or "user"
    active_route_value = str(current_alias.route_value if current_alias else username or "").strip()

    cloud_logs = _normalize_cloud_logs(list_resource_logs(owner, resource.resource_uuid, limit=120))
    health_checks = list_resource_checks(owner, resource.resource_uuid, limit=30)
    health_history_chart = []
    for item in reversed(health_checks):
        health_history_chart.append(
            {
                "status": (item.status or "unknown").strip().lower() or "unknown",
                "checked_at": item.checked_at,
                "check_method": item.check_method or "",
                "latency_ms": item.latency_ms,
            }
        )
    resource_api_keys = list_resource_api_keys(owner, resource.resource_uuid)
    resource_detail_url_path = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="detail",
        resource_uuid=resource.resource_uuid,
    )
    health_check_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="check",
        resource_uuid=resource.resource_uuid,
    )
    notes_add_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="notes_add",
        resource_uuid=resource.resource_uuid,
    )
    alert_settings_update_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="alerts_update",
        resource_uuid=resource.resource_uuid,
    )
    api_key_create_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="api_create",
        resource_uuid=resource.resource_uuid,
    )
    resource_url = request.build_absolute_uri(resource_detail_url_path)
    alert_settings = get_resource_alert_settings(owner, resource.resource_uuid, int(request.user.id or 0))
    twilio_sms_available = is_twilio_configured()
    email_notifications_available = is_email_provider_configured()
    if not twilio_sms_available:
        alert_settings["health_alerts_sms_enabled"] = False
        alert_settings["cloud_log_errors_sms_enabled"] = False
    if not email_notifications_available:
        alert_settings["health_alerts_email_enabled"] = False
        alert_settings["cloud_log_errors_email_enabled"] = False
    can_manage_resource = _can_manage_owner_resource(actor=request.user, owner=owner)
    latest_resource_api_key_value = str(
        request.session.pop(f"latest_created_resource_api_key:{resource.resource_uuid}", "") or ""
    ).strip()
    raw_notes = list_resource_notes(owner, resource.resource_uuid, limit=300)
    note_author_ids = sorted({int(item.author_user_id) for item in raw_notes if int(item.author_user_id or 0) > 0})
    author_avatar_urls: dict[int, str] = {}
    if note_author_ids:
        social_rows = (
            SocialAccount.objects.filter(user_id__in=note_author_ids)
            .order_by("id")
            .values("user_id", "extra_data")
        )
        for row in social_rows:
            user_id = int(row.get("user_id") or 0)
            if user_id <= 0 or author_avatar_urls.get(user_id):
                continue
            extra_data = row.get("extra_data")
            if not isinstance(extra_data, dict):
                extra_data = {}
            avatar_url = (
                str(extra_data.get("avatar_url") or "")
                or str(extra_data.get("picture") or "")
                or str(extra_data.get("avatar") or "")
                or str(extra_data.get("profile_image") or "")
                or str(extra_data.get("photo") or "")
                or str(extra_data.get("image") or "")
            ).strip()
            if avatar_url:
                author_avatar_urls[user_id] = avatar_url

    note_items = []
    for note in raw_notes:
        attachment_url = ""
        if note.attachment_id:
            attachment_url = _resource_route_reverse(
                route_kind=active_route_kind,
                route_value=active_route_value,
                endpoint_key="notes_attachment",
                resource_uuid=resource.resource_uuid,
                attachment_id=int(note.attachment_id),
            )
        note_items.append(
            {
                "id": note.id,
                "body": note.body,
                "author_username": note.author_username,
                "author_user_id": note.author_user_id,
                "author_avatar_url": author_avatar_urls.get(int(note.author_user_id or 0), ""),
                "created_display": _format_display_time(note.created_at),
                "is_author": note.author_user_id == request.user.id,
                "attachment_name": note.attachment_name,
                "attachment_url": attachment_url,
                "attachment_content_type": note.attachment_content_type,
            }
        )
    return render(
        request,
        "pages/resource_detail.html",
        {
            "resource": resource,
            "created_display": _format_display_time(resource.created_at),
            "last_checked_display": _format_display_time(resource.last_checked_at),
            "health_history_chart": health_history_chart,
            "cloud_logs": cloud_logs,
            "note_items": note_items,
            "resource_owner_username": owner.username,
            "resource_api_keys": [
                {
                    "id": item.id,
                    "name": item.name,
                    "key_prefix": item.key_prefix,
                    "created_at": item.created_at,
                    "revoke_url": _resource_route_reverse(
                        route_kind=active_route_kind,
                        route_value=active_route_value,
                        endpoint_key="api_revoke",
                        resource_uuid=resource.resource_uuid,
                        key_id=int(item.id),
                    ),
                }
                for item in resource_api_keys
            ],
            "alert_settings": alert_settings,
            "resource_url": resource_url,
            "resource_env_value": f"ALSHIVAL_RESOURCE={resource_url}",
            "can_manage_resource": can_manage_resource,
            "latest_resource_api_key_value": latest_resource_api_key_value,
            "resource_check_url": health_check_url,
            "resource_note_add_url": notes_add_url,
            "resource_alert_settings_update_url": alert_settings_update_url,
            "resource_api_key_create_url": api_key_create_url,
            "twilio_sms_available": twilio_sms_available,
            "email_notifications_available": email_notifications_available,
        },
    )


@login_required
@require_POST
def update_resource_alert_settings(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if not _can_access_owner_resource(actor=request.user, owner=owner, resource_uuid=resource.resource_uuid):
        raise PermissionDenied("You do not have access to this resource.")

    twilio_sms_available = is_twilio_configured()
    email_notifications_available = is_email_provider_configured()
    payload = {
        "health_alerts_app_enabled": _post_flag(request.POST, "health_alerts_app_enabled"),
        "health_alerts_sms_enabled": _post_flag(request.POST, "health_alerts_sms_enabled") if twilio_sms_available else False,
        "health_alerts_email_enabled": _post_flag(request.POST, "health_alerts_email_enabled") if email_notifications_available else False,
        "cloud_log_errors_app_enabled": _post_flag(request.POST, "cloud_log_errors_app_enabled"),
        "cloud_log_errors_sms_enabled": _post_flag(request.POST, "cloud_log_errors_sms_enabled") if twilio_sms_available else False,
        "cloud_log_errors_email_enabled": _post_flag(request.POST, "cloud_log_errors_email_enabled") if email_notifications_available else False,
    }
    upsert_resource_alert_settings(
        owner,
        resource.resource_uuid,
        int(request.user.id or 0),
        payload,
    )
    messages.success(request, "Alert settings updated for this resource.")
    return redirect(
        _resource_detail_anchor_url(
            current_alias=current_alias,
            route_kind=route_kind,
            route_value=username,
            resource_uuid=resource.resource_uuid,
            anchor="alerts",
        )
    )


@login_required
@require_POST
def check_resource_health_detail(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, _current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return JsonResponse({"error": "invalid_resource"}, status=404)
    if not _can_access_owner_resource(actor=request.user, owner=owner, resource_uuid=resource.resource_uuid):
        raise PermissionDenied("You do not have access to this resource.")

    result = check_health(resource.id, user=owner)
    return JsonResponse(
        {
            "resource_id": result.resource_id,
            "status": result.status,
            "checked_at": result.checked_at,
            "target": result.target,
            "error": result.error,
            "check_method": result.check_method,
            "latency_ms": result.latency_ms,
            "packet_loss_pct": result.packet_loss_pct,
        }
    )


@login_required
@require_POST
def create_resource_api_key_item(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if not _can_manage_owner_resource(actor=request.user, owner=owner):
        raise PermissionDenied("You do not have permission to manage this resource.")

    key_name = (request.POST.get("name") or "").strip()
    _key_id, raw_api_key = create_resource_api_key(owner, key_name, resource.resource_uuid)
    request.session[f"latest_created_resource_api_key:{resource.resource_uuid}"] = raw_api_key
    messages.success(request, "Resource API key created.")
    return redirect(
        _resource_detail_anchor_url(
            current_alias=current_alias,
            route_kind=route_kind,
            route_value=username,
            resource_uuid=resource.resource_uuid,
            anchor="resource-api-keys",
        )
    )


@login_required
@require_POST
def revoke_resource_api_key_item(request, username: str, resource_uuid, key_id: int, route_kind: str = "user"):
    owner, resource, current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if not _can_manage_owner_resource(actor=request.user, owner=owner):
        raise PermissionDenied("You do not have permission to manage this resource.")

    revoke_resource_api_key(owner, key_id, resource.resource_uuid)
    messages.success(request, "Resource API key revoked.")
    return redirect(
        _resource_detail_anchor_url(
            current_alias=current_alias,
            route_kind=route_kind,
            route_value=username,
            resource_uuid=resource.resource_uuid,
            anchor="resource-api-keys",
        )
    )


@login_required
@require_POST
def resource_note_add(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect('resources')
    if not _can_access_owner_resource(actor=request.user, owner=owner, resource_uuid=resource.resource_uuid):
        raise PermissionDenied("You do not have access to this resource.")

    body = (request.POST.get("body") or "").strip()
    if len(body) > 6000:
        body = body[:6000]

    upload = request.FILES.get("note_attachment")
    attachment_name = ""
    attachment_content_type = ""
    attachment_blob: bytes | None = None
    if upload and getattr(upload, "size", 0):
        max_bytes = 8 * 1024 * 1024
        if int(upload.size) > max_bytes:
            messages.warning(request, "Image too large. Maximum size is 8 MB.")
            return redirect(
                _resource_detail_anchor_url(
                    current_alias=current_alias,
                    route_kind=route_kind,
                    route_value=username,
                    resource_uuid=resource.resource_uuid,
                    anchor="notes",
                )
            )
        attachment_content_type = (getattr(upload, "content_type", "") or "").strip().lower()
        if not attachment_content_type.startswith("image/"):
            messages.warning(request, "Only image uploads are supported for notes.")
            return redirect(
                _resource_detail_anchor_url(
                    current_alias=current_alias,
                    route_kind=route_kind,
                    route_value=username,
                    resource_uuid=resource.resource_uuid,
                    anchor="notes",
                )
            )
        attachment_blob = upload.read()
        attachment_name = (getattr(upload, "name", "") or "image").strip() or "image"

    if not body and not attachment_blob:
        return redirect(
            _resource_detail_anchor_url(
                current_alias=current_alias,
                route_kind=route_kind,
                route_value=username,
                resource_uuid=resource.resource_uuid,
                anchor="notes",
            )
        )

    add_resource_note(
        owner,
        resource.resource_uuid,
        body,
        author_user_id=int(request.user.id or 0),
        author_username=(request.user.get_username() or "").strip() or f"user-{request.user.id}",
        attachment_name=attachment_name,
        attachment_content_type=attachment_content_type,
        attachment_blob=attachment_blob,
    )
    return redirect(
        _resource_detail_anchor_url(
            current_alias=current_alias,
            route_kind=route_kind,
            route_value=username,
            resource_uuid=resource.resource_uuid,
            anchor="notes",
        )
    )


@login_required
def resource_note_attachment(request, username: str, resource_uuid, attachment_id: int, route_kind: str = "user"):
    owner, resource, _current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if not _can_access_owner_resource(actor=request.user, owner=owner, resource_uuid=resource.resource_uuid):
        raise PermissionDenied("You do not have access to this resource.")

    attachment = get_resource_note_attachment(owner, resource.resource_uuid, attachment_id)
    if not attachment:
        raise PermissionDenied("Attachment not found.")
    if str(attachment.get("resource_uuid") or "").strip() != resource.resource_uuid:
        raise PermissionDenied("Attachment does not belong to this resource.")

    content_type = str(attachment.get("content_type") or "application/octet-stream")
    response = HttpResponse(attachment.get("file_blob") or b"", content_type=content_type)
    response["Content-Length"] = str(int(attachment.get("file_size") or 0))
    file_name = str(attachment.get("file_name") or "attachment")
    response["Content-Disposition"] = f'inline; filename="{file_name}"'
    return response


@csrf_exempt
@require_POST
def resource_logs_ingest(request, username: str, resource_uuid: str, route_kind: str = "user"):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)
    if not isinstance(payload, dict):
        payload = {}

    owner, resource, _current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid).strip(),
    )
    if owner is None or resource is None:
        return JsonResponse({"error": "invalid_resource"}, status=404)

    headers = request.headers
    api_key = (
        (headers.get("x-api-key") or "").strip()
        or (str(payload.get("api_key") or "")).strip()
    )
    if not api_key:
        return JsonResponse({"error": "missing_credentials"}, status=400)
    auth = authenticate_api_key(
        api_key=api_key,
        username=(headers.get("x-user-username") or str(payload.get("username") or "")).strip(),
        email=(headers.get("x-user-email") or str(payload.get("email") or "")).strip(),
        phone=(headers.get("x-user-phone") or str(payload.get("phone") or "")).strip(),
        resource_uuid=resource.resource_uuid,
        resource_owner=owner,
        require_resource_access=True,
    )
    if not auth.ok:
        return JsonResponse({"error": "forbidden"}, status=403)

    safe_payload = dict(payload)
    safe_payload.pop("api_key", None)
    safe_payload.pop("username", None)
    safe_payload.pop("email", None)
    safe_payload["resource_id"] = resource.resource_uuid
    safe_payload["resource_uuid"] = resource.resource_uuid
    safe_payload["submitted_by_username"] = owner.username
    safe_payload["received_at"] = datetime.now(timezone.utc).isoformat()
    store_resource_logs(
        owner,
        resource.resource_uuid,
        safe_payload,
        request.META.get("REMOTE_ADDR"),
        request.META.get("HTTP_USER_AGENT"),
    )
    try:
        dispatch_cloud_log_error_alerts(
            user=owner,
            resource=resource,
            payload=safe_payload,
        )
    except Exception:
        pass
    return JsonResponse({"status": "ok", "resource_id": resource.resource_uuid})


@login_required
def team_resource_detail(request, team_name: str, resource_uuid):
    return resource_detail(request, team_name, resource_uuid, route_kind="team")


@login_required
@require_POST
def team_update_resource_alert_settings(request, team_name: str, resource_uuid):
    return update_resource_alert_settings(request, team_name, resource_uuid, route_kind="team")


@login_required
@require_POST
def team_check_resource_health_detail(request, team_name: str, resource_uuid):
    return check_resource_health_detail(request, team_name, resource_uuid, route_kind="team")


@login_required
@require_POST
def team_create_resource_api_key_item(request, team_name: str, resource_uuid):
    return create_resource_api_key_item(request, team_name, resource_uuid, route_kind="team")


@login_required
@require_POST
def team_revoke_resource_api_key_item(request, team_name: str, resource_uuid, key_id: int):
    return revoke_resource_api_key_item(request, team_name, resource_uuid, key_id, route_kind="team")


@login_required
@require_POST
def team_resource_note_add(request, team_name: str, resource_uuid):
    return resource_note_add(request, team_name, resource_uuid, route_kind="team")


@login_required
def team_resource_note_attachment(request, team_name: str, resource_uuid, attachment_id: int):
    return resource_note_attachment(request, team_name, resource_uuid, attachment_id, route_kind="team")


@csrf_exempt
@require_POST
def team_resource_logs_ingest(request, team_name: str, resource_uuid: str):
    return resource_logs_ingest(request, team_name, resource_uuid, route_kind="team")


def _extract_openai_responses_text(payload: dict) -> str:
    text = str(payload.get("output_text") or "").strip()
    if text:
        return text
    output_items = payload.get("output")
    if not isinstance(output_items, list):
        return ""
    chunks: list[str] = []
    for item in output_items:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if str(part.get("type") or "").strip().lower() not in {"output_text", "text"}:
                continue
            value = str(part.get("text") or part.get("value") or "").strip()
            if value:
                chunks.append(value)
    return "\n".join(chunks).strip()


def _extract_chat_completion_text(payload: dict) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else {}
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() not in {"text", "output_text"}:
                continue
            value = str(item.get("text") or item.get("value") or "").strip()
            if value:
                chunks.append(value)
        return "\n".join(chunks).strip()
    return ""


def _query_kb_resources(*, knowledge_path: Path, query: str, limit: int) -> tuple[list[dict], str]:
    _ensure_runtime_cache_dirs()
    try:
        import chromadb
    except Exception:
        return [], "chromadb package is not installed"
    if not knowledge_path.exists():
        return [], ""

    client = chromadb.PersistentClient(path=str(knowledge_path))
    try:
        collection = client.get_collection(name="resources")
    except Exception:
        return [], ""

    n_results = max(1, min(int(limit or 5), 50))
    where_filter = None
    resolved_query = str(query or "").strip()
    rows: list[dict] = []
    if resolved_query:
        try:
            payload = collection.query(query_texts=[resolved_query], n_results=n_results, where=where_filter)
        except Exception as exc:
            return [], f"chroma query failed: {exc}"
        ids = (payload.get("ids") or [[]])[0]
        docs = (payload.get("documents") or [[]])[0]
        metas = (payload.get("metadatas") or [[]])[0]
        dists = (payload.get("distances") or [[]])[0]
        for idx, item_id in enumerate(ids):
            rows.append(
                {
                    "id": str(item_id or ""),
                    "document": str(docs[idx] or "") if idx < len(docs) else "",
                    "metadata": metas[idx] if idx < len(metas) and isinstance(metas[idx], dict) else {},
                    "distance": dists[idx] if idx < len(dists) else None,
                }
            )
    else:
        try:
            payload = collection.get(where=where_filter, limit=n_results)
        except Exception as exc:
            return [], f"chroma get failed: {exc}"
        ids = payload.get("ids") or []
        docs = payload.get("documents") or []
        metas = payload.get("metadatas") or []
        for idx, item_id in enumerate(ids):
            rows.append(
                {
                    "id": str(item_id or ""),
                    "document": str(docs[idx] or "") if idx < len(docs) else "",
                    "metadata": metas[idx] if idx < len(metas) and isinstance(metas[idx], dict) else {},
                    "distance": None,
                }
            )
    return rows, ""


def _resolve_resource_owner_and_item(resource_uuid: str, actor) -> tuple[object | None, object | None]:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return None, None
    candidate_users: list[object] = []
    seen_user_ids: set[int] = set()

    owner_row = (
        ResourcePackageOwner.objects.select_related("owner_user")
        .filter(resource_uuid=resolved_uuid)
        .first()
    )
    if owner_row and owner_row.owner_user_id and owner_row.owner_user and bool(owner_row.owner_user.is_active):
        candidate_users.append(owner_row.owner_user)
        seen_user_ids.add(int(owner_row.owner_user_id))

    for row in (
        ResourceRouteAlias.objects.select_related("owner_user")
        .filter(resource_uuid=resolved_uuid, owner_user_id__isnull=False)
        .order_by("-is_current", "-updated_at")
    ):
        owner_user = row.owner_user
        if owner_user is None or not bool(owner_user.is_active):
            continue
        owner_user_id = int(owner_user.id)
        if owner_user_id in seen_user_ids:
            continue
        candidate_users.append(owner_user)
        seen_user_ids.add(owner_user_id)

    if actor is not None:
        actor_id = int(getattr(actor, "id", 0) or 0)
        if actor_id > 0 and actor_id not in seen_user_ids:
            candidate_users.append(actor)
            seen_user_ids.add(actor_id)

    User = get_user_model()
    for user in User.objects.filter(is_active=True).order_by("id"):
        user_id = int(user.id)
        if user_id in seen_user_ids:
            continue
        candidate_users.append(user)
        seen_user_ids.add(user_id)

    for owner_user in candidate_users:
        resource = get_resource_by_uuid(owner_user, resolved_uuid)
        if resource is not None:
            return owner_user, resource
    return None, None


def _tool_search_kb_for_actor(actor, args: dict) -> dict:
    query = str(args.get("query") or "").strip()
    user_path = _user_owner_dir(actor) / "knowledge.db"
    global_path = _global_owner_dir() / "knowledge.db"

    user_results, user_error = _query_kb_resources(
        knowledge_path=user_path,
        query=query,
        limit=4,
    )
    if user_error:
        return {"ok": False, "error": user_error, "results": []}

    global_results, global_error = _query_kb_resources(
        knowledge_path=global_path,
        query=query,
        limit=3,
    )
    if global_error:
        return {"ok": False, "error": global_error, "results": []}

    merged = list(user_results) + list(global_results)
    return {
        "ok": True,
        "collection": "resources",
        "knowledge_paths": {
            "user": str(user_path),
            "global": str(global_path),
        },
        "query": query,
        "user_limit": 4,
        "global_limit": 3,
        "user_result_count": len(user_results),
        "global_result_count": len(global_results),
        "result_count": len(merged),
        "user_results": user_results,
        "global_results": global_results,
        "results": merged,
    }


def _tool_resource_health_check_for_actor(actor, args: dict) -> dict:
    resource_uuid = str(args.get("resource_uuid") or "").strip()
    if not resource_uuid:
        return {"ok": False, "error": "resource_uuid is required"}

    owner_row = ResourcePackageOwner.objects.filter(resource_uuid=resource_uuid).first()
    is_global = bool(owner_row and owner_row.owner_scope == ResourcePackageOwner.OWNER_SCOPE_GLOBAL)
    if not is_global and not user_can_access_resource(user=actor, resource_uuid=resource_uuid):
        return {"ok": False, "error": f"user cannot access resource: {resource_uuid}"}

    owner_user, resource = _resolve_resource_owner_and_item(resource_uuid, actor)
    if owner_user is None or resource is None:
        return {"ok": False, "error": f"resource not found: {resource_uuid}"}

    result = check_health(int(resource.id), user=owner_user, emit_transition_log=True)
    return {
        "ok": True,
        "resource_uuid": resource_uuid,
        "resource_name": str(getattr(resource, "name", "") or ""),
        "owner_username": str(getattr(owner_user, "username", "") or ""),
        "status": str(result.status or ""),
        "checked_at": str(result.checked_at or ""),
        "target": str(result.target or ""),
        "error": str(result.error or ""),
        "check_method": str(result.check_method or ""),
        "latency_ms": result.latency_ms,
        "packet_loss_pct": result.packet_loss_pct,
    }


def _resolve_github_mcp_upstream_url() -> str:
    configured = str(
        os.getenv("ASK_GITHUB_MCP_UPSTREAM_URL")
        or os.getenv("MCP_GITHUB_UPSTREAM_URL")
        or ""
    ).strip()
    return configured or "http://github-mcp:8082/"


def _normalize_openai_tool_name(raw_name: str, *, used_names: set[str]) -> str:
    candidate = re.sub(r"[^A-Za-z0-9_]", "_", str(raw_name or "").strip().lower())
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    if not candidate:
        candidate = "github_mcp_tool"
    if len(candidate) > 64:
        candidate = candidate[:64].rstrip("_")
    if not candidate:
        candidate = "github_mcp_tool"
    if candidate not in used_names:
        used_names.add(candidate)
        return candidate
    suffix = 2
    while True:
        suffix_text = f"_{suffix}"
        max_base_len = max(1, 64 - len(suffix_text))
        fallback = f"{candidate[:max_base_len].rstrip('_')}{suffix_text}"
        if fallback not in used_names:
            used_names.add(fallback)
            return fallback
        suffix += 1


def _github_mcp_jsonrpc_request(*, method: str, params: dict | None = None, timeout: int = 30) -> dict:
    request_id = f"ask-{get_random_string(10)}"
    payload: dict[str, object] = {"jsonrpc": "2.0", "id": request_id, "method": str(method or "").strip()}
    if isinstance(params, dict):
        payload["params"] = params

    response = requests.post(
        _resolve_github_mcp_upstream_url(),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json=payload,
        timeout=max(5, int(timeout or 30)),
    )
    body_text = str(response.text or "").strip()
    if response.status_code >= 400:
        detail = body_text[:400] if body_text else f"status {response.status_code}"
        raise RuntimeError(f"github mcp http error: {detail}")
    try:
        decoded = response.json()
    except Exception as exc:
        snippet = body_text[:400] if body_text else "no body"
        raise RuntimeError(f"github mcp non-json response ({exc}): {snippet}") from exc

    candidate = decoded
    if isinstance(decoded, list):
        matching = [
            item
            for item in decoded
            if isinstance(item, dict) and str(item.get("id") or "") == request_id
        ]
        candidate = matching[0] if matching else (decoded[0] if decoded else {})
    if not isinstance(candidate, dict):
        raise RuntimeError("github mcp invalid json-rpc payload")

    rpc_error = candidate.get("error")
    if isinstance(rpc_error, dict):
        message = str(rpc_error.get("message") or "unknown error").strip() or "unknown error"
        raise RuntimeError(f"github mcp rpc error: {message}")
    return candidate


def _github_mcp_list_tools() -> tuple[list[dict], dict[str, str], str]:
    try:
        payload = _github_mcp_jsonrpc_request(method="tools/list", params={})
    except Exception:
        try:
            _github_mcp_jsonrpc_request(
                method="initialize",
                params={
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "alshival-ask", "version": "1.0"},
                },
            )
            payload = _github_mcp_jsonrpc_request(method="tools/list", params={})
        except Exception as exc:
            return [], {}, str(exc)

    result = payload.get("result") if isinstance(payload, dict) else {}
    tools = result.get("tools") if isinstance(result, dict) else []
    if not isinstance(tools, list):
        return [], {}, "github mcp tools/list returned invalid tool payload"

    specs: list[dict] = []
    name_map: dict[str, str] = {}
    used_tool_names = {"search_kb", "resource_health_check"}
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        source_name = str(tool.get("name") or "").strip()
        if not source_name:
            continue
        description = str(tool.get("description") or "").strip() or f"GitHub MCP tool: {source_name}"
        input_schema = tool.get("inputSchema")
        if not isinstance(input_schema, dict) or str(input_schema.get("type") or "").strip().lower() != "object":
            input_schema = {"type": "object", "properties": {}, "required": []}
        exposed_name = _normalize_openai_tool_name(
            f"github_mcp_{source_name}",
            used_names=used_tool_names,
        )
        specs.append(
            {
                "type": "function",
                "function": {
                    "name": exposed_name,
                    "description": description,
                    "parameters": input_schema,
                },
            }
        )
        name_map[exposed_name] = source_name
    return specs, name_map, ""


def _github_mcp_call_tool(*, tool_name: str, args: dict) -> dict:
    resolved_tool_name = str(tool_name or "").strip()
    if not resolved_tool_name:
        return {"ok": False, "error": "github mcp tool name is required"}
    try:
        payload = _github_mcp_jsonrpc_request(
            method="tools/call",
            params={
                "name": resolved_tool_name,
                "arguments": args if isinstance(args, dict) else {},
            },
            timeout=60,
        )
    except Exception as exc:
        return {"ok": False, "source": "github_mcp", "tool_name": resolved_tool_name, "error": str(exc)}
    result = payload.get("result") if isinstance(payload, dict) else {}
    content = result.get("content") if isinstance(result, dict) else []
    text_parts: list[str] = []
    if isinstance(content, list):
        for item in content:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() == "text":
                text_value = str(item.get("text") or "").strip()
                if text_value:
                    text_parts.append(text_value)
    return {
        "ok": not bool(result.get("isError", False)) if isinstance(result, dict) else True,
        "source": "github_mcp",
        "tool_name": resolved_tool_name,
        "text": "\n".join(text_parts).strip(),
        "result": result if isinstance(result, dict) else {},
    }


def _ask_alshival_tools_spec(*, extra_tools: list[dict] | None = None) -> list[dict]:
    tools: list[dict] = [
        {
            "type": "function",
            "function": {
                "name": "search_kb",
                "description": "Search personal and global knowledge base entries.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "resource_health_check",
                "description": "Run a health check for a resource the actor can access.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "resource_uuid": {"type": "string"},
                    },
                    "required": ["resource_uuid"],
                },
            },
        },
    ]
    if extra_tools:
        tools.extend(extra_tools)
    return tools


def _run_ask_tool_for_actor(*, actor, tool_name: str, args: dict) -> dict:
    if tool_name == "search_kb":
        return _tool_search_kb_for_actor(actor, args)
    if tool_name == "resource_health_check":
        return _tool_resource_health_check_for_actor(actor, args)
    return {"ok": False, "error": f"unknown tool: {tool_name}"}


@login_required
@require_POST
def ask_alshival_chat(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)
    if not isinstance(payload, dict):
        payload = {}

    raw_message = str(payload.get("message") or "").strip()
    if not raw_message:
        return JsonResponse({"error": "message_required"}, status=400)
    if len(raw_message) > 8000:
        raw_message = raw_message[:8000]
    conversation_id = str(payload.get("conversation_id") or "").strip() or "default"

    setup = get_setup_state()
    api_key = str(getattr(setup, "openai_api_key", "") or "").strip()
    if not api_key:
        return JsonResponse({"error": "openai_not_configured"}, status=503)

    model = (
        str(getattr(settings, "ALSHIVAL_OPENAI_CHAT_MODEL", "") or "").strip()
        or str(getattr(setup, "default_model", "") or "").strip()
        or get_alshival_default_model()
    )
    current_dt = datetime.now(timezone.utc).astimezone()
    current_dt_text = current_dt.strftime("%A, %B %d, %Y, %H:%M:%S %Z").strip()

    user = request.user
    is_superuser = bool(getattr(user, "is_superuser", False))
    team_names = list(user.groups.order_by("name").values_list("name", flat=True))
    team_text = ", ".join(team_names) if team_names else "(none)"
    user_email = str(getattr(user, "email", "") or "").strip() or "(none)"
    user_phone = (
        UserNotificationSettings.objects.filter(user=user)
        .values_list("phone_number", flat=True)
        .first()
        or ""
    )
    user_phone = str(user_phone).strip() or "(none)"

    github_tool_specs: list[dict] = []
    github_tool_name_map: dict[str, str] = {}
    github_mcp_enabled = bool(
        setup
        and bool(getattr(setup, "ask_github_mcp_enabled", False))
        and is_github_connector_configured()
    )
    github_mcp_error = ""
    if github_mcp_enabled:
        github_tool_specs, github_tool_name_map, github_mcp_error = _github_mcp_list_tools()

    mcp_tool_lines = [
        "- search_kb(query): searches personal KB (top 4) and global KB (top 3).",
        "- resource_health_check(resource_uuid): runs a health check on an accessible resource.",
    ]
    if github_mcp_enabled and github_tool_name_map:
        exposed_names = list(github_tool_name_map.keys())
        preview_names = ", ".join(exposed_names[:12])
        if len(exposed_names) > 12:
            preview_names += ", ..."
        mcp_tool_lines.append(
            f"- GitHub MCP tools enabled ({len(exposed_names)}): {preview_names}"
        )
    elif github_mcp_enabled and github_mcp_error:
        mcp_tool_lines.append(f"- GitHub MCP requested but unavailable: {github_mcp_error}")

    system_prompt = "\n".join(
        [
            "You are Alshival, a concise DevOps and platform assistant.",
            "Give practical steps and keep responses short unless asked for detail.",
            f"Current date and time: {current_dt_text}.",
            "",
            "User Context:",
            f"- username: {getattr(user, 'username', '')}",
            f"- user_id: {getattr(user, 'id', '')}",
            f"- email: {user_email}",
            f"- phone_number: {user_phone}",
            f"- is_superuser: {'true' if is_superuser else 'false'}",
            f"- teams: {team_text}",
            "",
            "MCP Tools:",
            *mcp_tool_lines,
        ]
    )
    history_items = list_ask_chat_messages(request.user, conversation_id=conversation_id, limit=24)
    chat_input: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    for item in history_items:
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            chat_input.append({"role": role, "content": content})
    chat_input.append({"role": "user", "content": raw_message})
    try:
        add_ask_chat_message(request.user, conversation_id=conversation_id, role="user", content=raw_message)
    except Exception:
        pass

    tools_spec = _ask_alshival_tools_spec(extra_tools=github_tool_specs)
    max_tool_rounds = 6
    messages = list(chat_input)

    for _ in range(max_tool_rounds):
        request_payload = {
            "model": model,
            "messages": messages,
            "tools": tools_spec,
            "tool_choice": "auto",
            "temperature": 0.2,
        }
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=request_payload,
                timeout=45,
            )
        except requests.RequestException:
            return JsonResponse({"error": "openai_unreachable"}, status=502)
        if response.status_code >= 400:
            return JsonResponse({"error": "openai_error", "status_code": response.status_code}, status=502)

        data = response.json() if response.content else {}
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            return JsonResponse({"error": "openai_empty_response"}, status=502)
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        if not isinstance(message, dict):
            return JsonResponse({"error": "openai_invalid_response"}, status=502)

        tool_calls = message.get("tool_calls")
        if isinstance(tool_calls, list) and tool_calls:
            messages.append(
                {
                    "role": "assistant",
                    "content": message.get("content") or "",
                    "tool_calls": tool_calls,
                }
            )

            def _execute_tool_call(call_item: dict) -> tuple[str, str, str, str, dict]:
                call_id = str(call_item.get("id") or "").strip()
                function_obj = call_item.get("function") if isinstance(call_item, dict) else {}
                tool_name = str((function_obj or {}).get("name") or "").strip()
                raw_args = str((function_obj or {}).get("arguments") or "").strip() or "{}"
                try:
                    parsed_args = json.loads(raw_args)
                    if not isinstance(parsed_args, dict):
                        parsed_args = {}
                except Exception:
                    parsed_args = {}
                github_tool_name = github_tool_name_map.get(tool_name, "")
                if github_tool_name:
                    result = _github_mcp_call_tool(tool_name=github_tool_name, args=parsed_args)
                else:
                    result = _run_ask_tool_for_actor(actor=request.user, tool_name=tool_name, args=parsed_args)
                return call_id, tool_name, raw_args, json.dumps(result), result

            results_by_call_id: dict[str, tuple[str, str, str, dict]] = {}
            with ThreadPoolExecutor(max_workers=max(1, min(6, len(tool_calls)))) as executor:
                future_map = {executor.submit(_execute_tool_call, item): item for item in tool_calls}
                for future in as_completed(future_map):
                    call_item = future_map[future]
                    try:
                        call_id, tool_name, raw_args, result_json, result_obj = future.result()
                    except Exception as exc:
                        call_id = str((call_item.get("id") or "")).strip()
                        function_obj = call_item.get("function") if isinstance(call_item, dict) else {}
                        tool_name = str((function_obj or {}).get("name") or "").strip()
                        raw_args = str((function_obj or {}).get("arguments") or "").strip() or "{}"
                        result_obj = {"ok": False, "error": f"tool execution failure: {exc}"}
                        result_json = json.dumps(result_obj)
                    results_by_call_id[call_id] = (tool_name, raw_args, result_json, result_obj)

            for call_item in tool_calls:
                call_id = str(call_item.get("id") or "").strip()
                tool_name, raw_args, result_json, _result_obj = results_by_call_id.get(
                    call_id,
                    ("", "{}", json.dumps({"ok": False, "error": "missing_tool_result"}), {"ok": False}),
                )
                try:
                    add_ask_chat_tool_event(
                        request.user,
                        conversation_id=conversation_id,
                        kind="tool_call",
                        tool_name=tool_name or "unknown",
                        tool_call_id=call_id,
                        tool_args_json=raw_args,
                        content=f"[tool_call] {tool_name or 'unknown'}",
                    )
                    add_ask_chat_tool_event(
                        request.user,
                        conversation_id=conversation_id,
                        kind="tool_result",
                        tool_name=tool_name or "unknown",
                        tool_call_id=call_id,
                        tool_result_json=result_json,
                        content=f"[tool_result] {tool_name or 'unknown'}",
                    )
                except Exception:
                    pass

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": result_json,
                    }
                )
            continue

        reply = _extract_chat_completion_text(data)
        if not reply:
            reply = "I couldn't generate a response right now. Please try again."
        try:
            add_ask_chat_message(
                request.user,
                conversation_id=conversation_id,
                role="assistant",
                content=reply,
            )
        except Exception:
            pass
        return JsonResponse({"reply": reply, "conversation_id": conversation_id})

    return JsonResponse(
        {
            "error": "tool_loop_limit_reached",
            "conversation_id": conversation_id,
        },
        status=502,
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
    resource_scope_payload = _resolve_resource_scope_payload(request)
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
        resource_id = add_resource(
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
            access_scope=resource_scope_payload['scope'],
            team_names=resource_scope_payload['team_names'],
        )
        created_resource = get_resource(request.user, resource_id)
        if created_resource:
            _sync_resource_team_shares(
                owner=request.user,
                resource_uuid=created_resource.resource_uuid,
                resource_name=created_resource.name,
                scope=created_resource.access_scope,
                team_names=created_resource.team_names,
            )
            _sync_resource_route_aliases(
                owner=request.user,
                resource_uuid=created_resource.resource_uuid,
                scope=created_resource.access_scope,
                team_names=created_resource.team_names,
                actor=request.user,
            )
        try:
            check_health(resource_id, user=request.user)
        except Exception:
            # The resource should still be created even if first health check fails.
            pass

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
    resource_scope_payload = _resolve_resource_scope_payload(request)
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
            access_scope=resource_scope_payload['scope'],
            team_names=resource_scope_payload['team_names'],
        )
        updated_resource = get_resource(request.user, resource_id)
        if updated_resource:
            _sync_resource_team_shares(
                owner=request.user,
                resource_uuid=updated_resource.resource_uuid,
                resource_name=updated_resource.name,
                scope=updated_resource.access_scope,
                team_names=updated_resource.team_names,
            )
            _sync_resource_route_aliases(
                owner=request.user,
                resource_uuid=updated_resource.resource_uuid,
                scope=updated_resource.access_scope,
                team_names=updated_resource.team_names,
                actor=request.user,
            )

    return redirect('resources')


@login_required
@require_POST
def delete_resource_item(request, resource_id: int):
    resource = get_resource(request.user, resource_id)
    if resource:
        ResourceTeamShare.objects.filter(
            owner=request.user,
            resource_uuid=resource.resource_uuid,
        ).delete()
        ResourceRouteAlias.objects.filter(resource_uuid=str(resource.resource_uuid or "").strip()).delete()
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
            'check_method': result.check_method,
            'latency_ms': result.latency_ms,
            'packet_loss_pct': result.packet_loss_pct,
        }
    )


@login_required
@require_POST
def add_ssh_credential_item(request):
    scope = (request.POST.get('scope') or 'account').strip()
    name = (request.POST.get('name') or '').strip()
    key_text = (request.POST.get('private_key_text') or '').strip()
    key_file = request.FILES.get('private_key_file')
    raw_team_names = request.POST.getlist('team_names')

    if key_file:
        key_text = key_file.read().decode('utf-8', errors='ignore').strip()
    if not (name and key_text):
        return redirect('resources')

    member_team_names = set(_ssh_team_choices_for_user(request.user))
    team_names = []
    for value in raw_team_names:
        resolved = (value or '').strip()
        if resolved and resolved in member_team_names and resolved not in team_names:
            team_names.append(resolved)
    if scope == 'team':
        if not team_names:
            return redirect('resources')
    elif scope not in {'global', 'team_global'}:
        team_names = []

    if scope in {'global', 'team_global'}:
        if not request.user.is_superuser:
            return redirect('resources')
        add_global_ssh_credential(
            user=request.user,
            name=name,
            team_name='',
            private_key_text=key_text,
        )
        return redirect('resources')

    add_ssh_credential(request.user, name, scope, team_names, key_text)

    return redirect('resources')


@login_required
@require_POST
def delete_ssh_credential_item(request, credential_id: str):
    delete_ssh_credential(request.user, credential_id)
    return redirect('resources')


@login_required
@require_POST
def delete_global_ssh_credential_item(request, credential_id: int):
    if not request.user.is_superuser:
        return redirect('resources')
    delete_global_ssh_credential(credential_id=credential_id)
    return redirect('resources')
