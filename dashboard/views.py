from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
import html
import hashlib
import hmac
import json
import mimetypes
import os
import re
import shlex
import subprocess
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
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
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import NoReverseMatch, reverse
from django.utils.crypto import get_random_string
from django.utils.text import slugify
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from allauth.socialaccount.models import SocialAccount, SocialApp, SocialToken

from .global_ssh_store import (
    add_global_ssh_credential,
    delete_global_ssh_credential,
    list_global_ssh_credentials,
)
from .email_branding import build_alshival_branded_email, build_alshival_branded_email_from_html
from .models import ResourcePackageOwner, ResourceRouteAlias, ResourceTeamShare, UserFeatureAccess, UserInvite, UserNotificationSettings, WikiPage
from .health import _alert_filter_allowed_channels, check_health, dispatch_cloud_log_error_alerts, probe_resource_ping
from .knowledge_store import upsert_resource_health_knowledge
from .user_knowledge_store import get_user_record_by_user_id, query_user_records
from .setup_state import (
    get_alshival_default_model,
    get_ingest_api_key,
    get_or_create_setup_state,
    get_setup_state,
    is_asana_connector_configured,
    is_github_connector_configured,
    is_microsoft_connector_configured,
    is_support_inbox_email_alerts_enabled,
    is_twilio_configured,
    is_setup_complete,
)
from .user_avatar import resolve_user_avatar_urls
from .wiki_markdown import render_markdown_fallback
from .request_auth import authenticate_api_key, get_twilio_auth_token, resolve_user_by_phone, user_can_access_resource
from .resource_ssh_exec import execute_resource_ssh_command
from .resources_store import (
    REMINDER_VALID_STATUSES,
    add_ask_chat_context_event,
    add_ask_chat_message,
    add_ask_chat_tool_event,
    add_team_chat_message,
    add_user_notification,
    add_resource,
    add_resource_note,
    add_ssh_credential,
    clear_user_notifications,
    create_account_api_key,
    create_reminder,
    create_resource_api_key,
    delete_reminder,
    delete_resource,
    delete_ssh_credential,
    get_reminder,
    get_resource_note_attachment,
    get_resource,
    get_team_chat_attachment,
    get_user_alert_filter_prompt,
    get_user_calendar_notification_settings,
    get_team_chat_notification_settings,
    get_resource_alert_settings,
    get_user_asana_task_cache,
    list_user_agenda_item_resource_mappings,
    list_user_asana_board_resource_mappings,
    list_user_asana_task_resource_mappings,
    get_resource_by_uuid,
    list_ask_chat_messages,
    list_resource_checks,
    list_resource_api_keys,
    list_resource_notes,
    list_reminders,
    list_team_chat_messages,
    list_user_notifications,
    list_user_api_keys,
    list_user_calendar_event_cache,
    list_user_outlook_mail_cache,
    list_resource_logs,
    list_resources,
    list_ssh_credentials,
    _global_owner_dir,
    _user_db_path,
    _user_knowledge_db_path,
    _user_owner_dir,
    get_resource_owner_context,
    mark_all_user_notifications_read,
    revoke_resource_api_key,
    replace_user_calendar_event_cache,
    set_user_asana_board_resource_mapping,
    set_user_agenda_item_resource_mapping,
    set_user_asana_task_cache,
    set_user_asana_task_resource_mapping,
    store_resource_logs,
    upsert_user_outlook_mail_cache,
    get_user_outlook_mail_cache_message,
    update_reminder,
    update_user_alert_filter_prompt,
    update_user_calendar_event_completion,
    upsert_user_calendar_notification_settings,
    upsert_team_chat_notification_settings,
    upsert_resource_alert_settings,
    update_resource,
)
from .calendar_sync_service import refresh_calendar_cache_for_user
from .support_inbox import send_support_inbox_email


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
    'invite_method_required': ('Select at least one sign-in method.', 'warning'),
    'invite_channel_required': ('Select email or SMS delivery.', 'warning'),
    'invite_email_required': ('Email delivery requires an email address.', 'warning'),
    'invite_phone_required': ('SMS delivery requires a phone number.', 'warning'),
    'invite_sms_not_configured': ('Twilio is not configured. Set it in Connector Settings.', 'warning'),
    'invite_send_failed': ('Invite was created but could not be delivered.', 'warning'),
    'invite_invalid_or_expired': ('Invite link is invalid or expired.', 'warning'),
    'invite_already_claimed': ('Invite was already used.', 'warning'),
    'invite_identity_mismatch': ('This account does not match the invite target.', 'warning'),
    'invite_method_not_allowed': ('This invite requires a different sign-in method.', 'warning'),
    'invite_applied': ('Invite accepted. Access permissions were applied.', 'success'),
    'team_name_required': ('Team name is required.', 'warning'),
    'team_name_exists': ('Team name already exists.', 'warning'),
    'invite_required_fields': ('Provide at least an email or phone target for the invite.', 'warning'),
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
_INVITE_SIGNUP_METHODS = [
    {
        "key": "local",
        "label": "Username + Password",
        "description": "Create or use a local account.",
    },
    {
        "key": "github",
        "label": "GitHub",
        "description": "Sign in with your GitHub identity.",
    },
    {
        "key": "microsoft",
        "label": "Microsoft",
        "description": "Sign in with your Microsoft account.",
    },
    {
        "key": "asana",
        "label": "Asana",
        "description": "Sign in with your Asana account.",
    },
]
_INVITE_TOKEN_MAX_AGE_DAYS = 14

_WIKI_SCOPE_WORKSPACE = WikiPage.SCOPE_WORKSPACE
_WIKI_SCOPE_RESOURCE = WikiPage.SCOPE_RESOURCE
_WIKI_SCOPE_TEAM = getattr(WikiPage, "SCOPE_TEAM", "team")

_WIKI_STATUS = {
    "wiki_page_created": ("Wiki page created.", "success"),
    "wiki_page_updated": ("Wiki page updated.", "success"),
    "wiki_page_published": ("Wiki page published.", "success"),
    "wiki_draft_saved": ("Draft saved.", "success"),
    "wiki_page_deleted": ("Wiki page deleted.", "success"),
    "wiki_title_required": ("Add a markdown # title before saving.", "warning"),
    "wiki_path_required": ("Path is required.", "warning"),
    "wiki_path_invalid": ("Path is invalid. Use letters, numbers, and dashes with optional / folders.", "warning"),
    "wiki_path_exists": ("A wiki page with this path already exists.", "warning"),
    "wiki_page_not_found": ("Wiki page not found.", "warning"),
    "wiki_no_access": ("You do not have access to this wiki page.", "warning"),
    "wiki_resource_required": ("Select a resource to open the resource wiki.", "warning"),
    "wiki_resource_no_access": ("You do not have access to this resource wiki.", "warning"),
    "wiki_team_required": ("Select a team to open the team wiki.", "warning"),
    "wiki_team_no_access": ("You do not have access to this team wiki.", "warning"),
}

_RESOURCES_UPTIME_WINDOW_DAYS = 7
_RESOURCES_UPTIME_CHECK_LIMIT = 1200


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


def _trigger_docker_app_restart() -> tuple[bool, str]:
    base_dir = Path(getattr(settings, "BASE_DIR", Path(__file__).resolve().parent.parent))
    restart_cmd = str(os.getenv("ALSHIVAL_DOCKER_RESTART_CMD", "") or "").strip()

    if not restart_cmd:
        compose_files = [
            base_dir / "docker-compose.yml",
            base_dir / "docker-compose-http.yml",
            base_dir / "docker-compose-https.yml",
        ]
        compose_file = next((path for path in compose_files if path.exists()), None)
        if compose_file is None:
            return False, "No docker compose file found."
        restart_cmd = f"docker compose -f {shlex.quote(str(compose_file))} restart"

    # Run detached so this request returns before service restarts.
    detached_cmd = f"sleep 1; {restart_cmd}"
    try:
        with open(os.devnull, "wb") as devnull:
            subprocess.Popen(
                ["bash", "-lc", detached_cmd],
                cwd=str(base_dir),
                stdout=devnull,
                stderr=devnull,
                start_new_session=True,
                close_fds=True,
            )
    except Exception as exc:
        return False, f"Unable to schedule restart: {exc}"

    return True, "Restart scheduled."


def _post_flag(payload_or_request, key: str) -> bool:
    source = getattr(payload_or_request, "POST", None)
    if source is None and hasattr(payload_or_request, "get"):
        source = payload_or_request
    if source is None:
        return False
    try:
        raw_value = source.get(key)
    except Exception:
        return False
    value = str(raw_value or "").strip().lower()
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


def _normalize_phone(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return ""
    return f"+{digits}"


def _invite_enabled_signup_methods() -> set[str]:
    enabled = {"local"}
    if is_github_connector_configured():
        enabled.add("github")
    if is_microsoft_connector_configured():
        enabled.add("microsoft")
    if is_asana_connector_configured():
        enabled.add("asana")
    return enabled


def _normalize_invite_signup_methods(raw_methods: list[str], *, fallback_to_local: bool = False) -> list[str]:
    allowed = {item["key"] for item in _INVITE_SIGNUP_METHODS}
    enabled = _invite_enabled_signup_methods()
    selected: list[str] = []
    for value in raw_methods:
        method = str(value or "").strip().lower()
        if not method or method not in allowed or method not in enabled or method in selected:
            continue
        selected.append(method)
    if not selected and fallback_to_local and "local" in enabled:
        selected.append("local")
    return selected


def _visible_invite_signup_methods() -> list[dict[str, str]]:
    enabled = _invite_enabled_signup_methods()
    rows: list[dict[str, str]] = []
    for item in _INVITE_SIGNUP_METHODS:
        key = str(item.get("key") or "").strip().lower()
        if key not in enabled:
            continue
        rows.append(
            {
                "key": key,
                "label": str(item.get("label") or key.title()).strip(),
                "description": str(item.get("description") or "").strip(),
            }
        )
    return rows


def _invite_token() -> str:
    return get_random_string(72)


def _invite_expiry_datetime() -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=_INVITE_TOKEN_MAX_AGE_DAYS)


def _invite_absolute_url(request, token: str) -> str:
    return request.build_absolute_uri(reverse("accept_user_invite", kwargs={"token": token}))


def _twilio_sms_credentials() -> tuple[str, str, str]:
    setup = get_setup_state()
    account_sid = str(getattr(setup, "twilio_account_sid", "") or "").strip() if setup else ""
    auth_token = str(getattr(setup, "twilio_auth_token", "") or "").strip() if setup else ""
    from_number = str(getattr(setup, "twilio_from_number", "") or "").strip() if setup else ""
    if not account_sid:
        account_sid = str(os.getenv("TWILIO_ACCOUNT_SID", "") or "").strip()
    if not auth_token:
        auth_token = str(os.getenv("TWILIO_AUTH_TOKEN", "") or "").strip()
    if not from_number:
        from_number = str(os.getenv("TWILIO_FROM_NUMBER", "") or "").strip()
    return account_sid, auth_token, from_number


def _send_invite_sms(*, to_number: str, message: str) -> tuple[bool, str]:
    account_sid, auth_token, from_number = _twilio_sms_credentials()
    if not (account_sid and auth_token and from_number):
        return False, "twilio_not_configured"
    if not to_number:
        return False, "missing_phone_number"
    try:
        response = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            data={
                "To": str(to_number or "").strip(),
                "From": from_number,
                "Body": str(message or "").strip()[:1200],
            },
            auth=(account_sid, auth_token),
            timeout=10,
        )
    except requests.RequestException:
        return False, "twilio_request_failed"
    if 200 <= int(response.status_code) < 300:
        return True, ""
    return False, f"twilio_status_{int(response.status_code)}"


def _looks_like_html(value: str) -> bool:
    content = str(value or "").strip()
    if not content:
        return False
    return bool(re.search(r"<[a-zA-Z][^>]*>", content))


def _html_to_text_for_email(value: str) -> str:
    content = str(value or "").strip()
    if not content:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", content)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?i)</div\s*>", "\n", text)
    text = re.sub(r"(?i)</li\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _plain_text_to_email_html_fragment(value: str) -> str:
    content = str(value or "").strip()
    if not content:
        return ""
    paragraphs: list[str] = []
    for block in content.split("\n\n"):
        lines = [html.escape(line.strip()) for line in block.split("\n") if line.strip()]
        if not lines:
            continue
        paragraphs.append("<br>".join(lines))
    return "".join(
        f'<p style="margin:0 0 14px;font-size:15px;line-height:1.7;color:#334155;">{paragraph}</p>'
        for paragraph in paragraphs
    )


def _absolute_invite_related_url(invite_url: str, target_url: str) -> str:
    candidate = str(target_url or "").strip()
    if not candidate:
        return ""
    lowered = candidate.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return candidate
    invite_split = urlsplit(str(invite_url or "").strip())
    target_split = urlsplit(candidate)
    if not (invite_split.scheme and invite_split.netloc):
        return candidate
    resolved_path = str(target_split.path or "").strip() or "/"
    if not resolved_path.startswith("/"):
        resolved_path = f"/{resolved_path}"
    return urlunsplit(
        (
            invite_split.scheme,
            invite_split.netloc,
            resolved_path,
            str(target_split.query or "").strip(),
            str(target_split.fragment or "").strip(),
        )
    )


def _invite_email_signup_option_links(
    *,
    invite_token: str,
    invite_url: str,
    signup_methods: list[str],
    invited_email: str,
) -> list[dict[str, str]]:
    methods = _normalize_invite_signup_methods(signup_methods, fallback_to_local=False)
    if not methods:
        methods = _normalize_invite_signup_methods(["local"], fallback_to_local=True)
    complete_path = reverse("complete_user_invite", kwargs={"token": invite_token})
    options: list[dict[str, str]] = []

    if "local" in methods:
        local_login_url = f"{reverse('account_login')}?{urlencode({'next': complete_path})}"
        signup_params = {"next": complete_path}
        normalized_email = str(invited_email or "").strip().lower()
        if normalized_email:
            signup_params["email"] = normalized_email
        local_signup_url = f"{reverse('account_signup')}?{urlencode(signup_params)}"
        options.append(
            {
                "label": "Email Sign In",
                "url": _absolute_invite_related_url(invite_url, local_login_url),
            }
        )
        options.append(
            {
                "label": "Create Local Account",
                "url": _absolute_invite_related_url(invite_url, local_signup_url),
            }
        )

    for method in methods:
        if method == "local":
            continue
        login_base = _social_login_path(method)
        login_url = f"{login_base}?{urlencode({'process': 'login', 'next': complete_path})}"
        options.append(
            {
                "label": f"Continue with {_invite_method_label(method)}",
                "url": _absolute_invite_related_url(invite_url, login_url),
            }
        )
    return options


def _invite_email_signup_footer_text(*, invite_url: str, option_links: list[dict[str, str]]) -> str:
    lines = ["Additional sign-up options:"]
    resolved_invite_url = str(invite_url or "").strip()
    if resolved_invite_url:
        lines.append(f"- Open Invitation: {resolved_invite_url}")
    for item in option_links:
        label = str(item.get("label") or "").strip()
        link = str(item.get("url") or "").strip()
        if not (label and link):
            continue
        lines.append(f"- {label}: {link}")
    return "\n".join(lines).strip()


def _invite_email_signup_footer_html(*, invite_url: str, option_links: list[dict[str, str]]) -> str:
    buttons: list[str] = []
    resolved_invite_url = str(invite_url or "").strip()
    if resolved_invite_url:
        safe_invite_url = html.escape(resolved_invite_url, quote=True)
        buttons.append(
            (
                f'<a href="{safe_invite_url}" '
                'style="display:inline-block;margin:0 10px 10px 0;padding:10px 16px;'
                'background:#0f172a;color:#ffffff;text-decoration:none;border-radius:8px;'
                'font-weight:600;font-size:14px;">Open Invitation</a>'
            )
        )
    for item in option_links:
        label = str(item.get("label") or "").strip()
        link = str(item.get("url") or "").strip()
        if not (label and link):
            continue
        safe_label = html.escape(label)
        safe_link = html.escape(link, quote=True)
        buttons.append(
            (
                f'<a href="{safe_link}" '
                'style="display:inline-block;margin:0 10px 10px 0;padding:10px 16px;'
                'background:#e2e8f0;color:#0f172a;text-decoration:none;border-radius:8px;'
                'font-weight:600;font-size:14px;border:1px solid #cbd5e1;">'
                f"{safe_label}</a>"
            )
        )
    if not buttons:
        return ""
    return (
        '<div style="margin-top:18px;padding-top:18px;border-top:1px solid #e2e8f0;">'
        '<p style="margin:0 0 10px;font-size:13px;line-height:1.5;color:#475569;'
        'font-weight:600;">Additional sign-up options</p>'
        f"{''.join(buttons)}"
        "</div>"
    )


def _decorate_invite_email_message(
    *,
    message: str,
    invite_url: str,
    invite_token: str,
    signup_methods: list[str],
    invited_email: str,
) -> tuple[str, str]:
    base_message = str(message or "").strip()
    base_is_html = _looks_like_html(base_message)
    option_links = _invite_email_signup_option_links(
        invite_token=invite_token,
        invite_url=invite_url,
        signup_methods=signup_methods,
        invited_email=invited_email,
    )

    base_text = _html_to_text_for_email(base_message) if base_is_html else base_message
    footer_text = _invite_email_signup_footer_text(invite_url=invite_url, option_links=option_links)
    if footer_text:
        resolved_text = f"{base_text}\n\n{footer_text}".strip() if base_text else footer_text
    else:
        resolved_text = base_text

    base_html = base_message if base_is_html else _plain_text_to_email_html_fragment(base_text)
    footer_html = _invite_email_signup_footer_html(invite_url=invite_url, option_links=option_links)
    resolved_html = f"{base_html}\n{footer_html}".strip() if footer_html else str(base_html or "").strip()
    return resolved_text, resolved_html


def _send_invite_email(*, recipient_email: str, subject: str, message: str, message_html: str = "") -> tuple[bool, str]:
    to_email = str(recipient_email or "").strip().lower()
    if not to_email:
        return False, "missing_email_address"
    from_email = getattr(settings, "DEFAULT_FROM_EMAIL", "noreply@alshival.local")
    resolved_html_input = str(message_html or "").strip()
    if resolved_html_input:
        resolved_subject, resolved_text_body, resolved_html_body = build_alshival_branded_email_from_html(
            subject,
            message,
            resolved_html_input,
        )
    else:
        resolved_subject, resolved_text_body, resolved_html_body = build_alshival_branded_email(subject, message)
    try:
        send_mail(
            str(resolved_subject or "").strip()[:255] or "You are invited to Alshival",
            resolved_text_body,
            from_email,
            [to_email],
            fail_silently=False,
            html_message=resolved_html_body,
        )
    except Exception:
        # Fallback to support inbox email delivery when enabled.
        if is_support_inbox_email_alerts_enabled():
            support_ok, support_error = send_support_inbox_email(
                recipient_email=to_email,
                subject=str(resolved_subject or "").strip()[:255] or "You are invited to Alshival",
                body_text=str(message or "").strip(),
            )
            if support_ok:
                return True, ""
            return False, f"email_send_failed_support_fallback:{support_error or 'unknown'}"
        return False, "email_send_failed"
    return True, ""


def _send_team_chat_sms(*, recipient, message: str) -> tuple[bool, str]:
    phone_raw = (
        UserNotificationSettings.objects.filter(user=recipient)
        .values_list("phone_number", flat=True)
        .first()
        or ""
    )
    to_number = _normalize_phone(phone_raw)
    if not to_number:
        return False, "missing_phone_number"
    return _send_invite_sms(
        to_number=to_number,
        message=str(message or "").strip()[:1200],
    )


def _send_team_chat_email(*, recipient, subject: str, message: str) -> tuple[bool, str]:
    recipient_email = str(getattr(recipient, "email", "") or "").strip().lower()
    if not recipient_email:
        return False, "missing_email_address"
    if not is_support_inbox_email_alerts_enabled():
        return False, "support_inbox_email_disabled"
    return send_support_inbox_email(
        recipient_email=recipient_email,
        subject=str(subject or "").strip(),
        body_text=str(message or "").strip(),
    )


_TEAM_CHAT_ATTACHMENT_MAX_BYTES = 12 * 1024 * 1024
_TEAM_CHAT_ALLOWED_ATTACHMENT_EXTENSIONS = {
    ".txt",
    ".md",
    ".markdown",
    ".csv",
    ".tsv",
    ".json",
    ".xml",
    ".yaml",
    ".yml",
    ".log",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".odt",
    ".ods",
    ".odp",
    ".rtf",
}
_TEAM_CHAT_ALLOWED_ATTACHMENT_CONTENT_TYPES = {
    "text/plain",
    "text/markdown",
    "text/csv",
    "text/tab-separated-values",
    "application/json",
    "application/xml",
    "text/xml",
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.oasis.opendocument.text",
    "application/vnd.oasis.opendocument.spreadsheet",
    "application/vnd.oasis.opendocument.presentation",
    "application/rtf",
}


def _team_chat_attachment_allowed(*, file_name: str, content_type: str) -> bool:
    resolved_content_type = str(content_type or "").strip().lower()
    if resolved_content_type.startswith("image/"):
        return True
    if resolved_content_type in _TEAM_CHAT_ALLOWED_ATTACHMENT_CONTENT_TYPES:
        return True
    suffix = Path(str(file_name or "").strip()).suffix.lower()
    return suffix in _TEAM_CHAT_ALLOWED_ATTACHMENT_EXTENSIONS


def _team_chat_attachment_is_inline(*, content_type: str) -> bool:
    resolved_content_type = str(content_type or "").strip().lower()
    if resolved_content_type.startswith("image/"):
        return True
    if resolved_content_type in {"application/pdf", "text/plain", "text/markdown", "text/csv"}:
        return True
    return False


def _invite_method_label(method_key: str) -> str:
    for item in _INVITE_SIGNUP_METHODS:
        if str(item.get("key") or "").strip().lower() == str(method_key or "").strip().lower():
            return str(item.get("label") or "").strip() or str(method_key or "").strip().lower()
    return str(method_key or "").strip().lower()


def _invite_user_matched_methods(user, *, allowed_methods: list[str]) -> set[str]:
    resolved_allowed = [str(item or "").strip().lower() for item in allowed_methods if str(item or "").strip()]
    if not resolved_allowed:
        return set()

    matched: set[str] = set()
    if "local" in resolved_allowed:
        matched.add("local")

    provider_methods = [item for item in resolved_allowed if item in {"github", "microsoft", "asana"}]
    if not provider_methods:
        return matched

    try:
        providers = set(
            SocialAccount.objects.filter(user=user, provider__in=provider_methods)
            .values_list("provider", flat=True)
        )
    except Exception:
        providers = set()

    for method in provider_methods:
        if method in providers:
            matched.add(method)
    return matched


def _invite_phone_for_user(user) -> str:
    phone_raw = (
        UserNotificationSettings.objects.filter(user=user)
        .values_list("phone_number", flat=True)
        .first()
        or ""
    )
    return _normalize_phone(str(phone_raw or ""))


def _first_nonempty_string(candidates: list[object]) -> str:
    for raw_value in candidates:
        value = str(raw_value or "").strip()
        if value:
            return value
    return ""


def _first_valid_email(candidates: list[object]) -> str:
    for raw_value in candidates:
        value = str(raw_value or "").strip().lower()
        if value and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value):
            return value
    return ""


def _invite_creator_profile_context(actor) -> dict[str, object]:
    actor_id = int(getattr(actor, "id", 0) or 0)
    username = str(getattr(actor, "username", "") or "").strip()
    full_name = " ".join(str(getattr(actor, "get_full_name", lambda: "")() or "").split()).strip()
    display_name = full_name or username
    account_email = str(getattr(actor, "email", "") or "").strip().lower()
    phone_number = _invite_phone_for_user(actor)

    social_accounts_by_provider: dict[str, SocialAccount] = {}
    if actor_id > 0:
        try:
            linked_accounts = list(
                SocialAccount.objects.filter(
                    user=actor,
                    provider__in=("microsoft", "github", "asana"),
                ).order_by("id")
            )
        except (OperationalError, ProgrammingError):
            linked_accounts = []
        except Exception:
            linked_accounts = []
        for account in linked_accounts:
            provider_key = str(getattr(account, "provider", "") or "").strip().lower()
            if provider_key in {"microsoft", "github", "asana"} and provider_key not in social_accounts_by_provider:
                social_accounts_by_provider[provider_key] = account

    microsoft_account = social_accounts_by_provider.get("microsoft")
    github_account = social_accounts_by_provider.get("github")
    asana_account = social_accounts_by_provider.get("asana")

    microsoft_data = dict(getattr(microsoft_account, "extra_data", {}) or {}) if microsoft_account else {}
    github_data = dict(getattr(github_account, "extra_data", {}) or {}) if github_account else {}
    asana_data = dict(getattr(asana_account, "extra_data", {}) or {}) if asana_account else {}

    microsoft_email = _first_valid_email(
        [
            microsoft_data.get("mail"),
            microsoft_data.get("email"),
            microsoft_data.get("userPrincipalName"),
            microsoft_data.get("preferred_username"),
            getattr(microsoft_account, "uid", "") if microsoft_account else "",
        ]
    )
    microsoft_identity = _first_nonempty_string(
        [
            microsoft_data.get("preferred_username"),
            microsoft_data.get("mail"),
            microsoft_data.get("userPrincipalName"),
            microsoft_data.get("name"),
            getattr(microsoft_account, "uid", "") if microsoft_account else "",
        ]
    )
    github_username = _first_nonempty_string(
        [
            github_data.get("login"),
            github_data.get("username"),
            getattr(github_account, "uid", "") if github_account else "",
        ]
    )
    github_identity = _first_nonempty_string(
        [
            github_data.get("name"),
            github_data.get("login"),
            getattr(github_account, "uid", "") if github_account else "",
        ]
    )
    github_profile_url = str(github_data.get("html_url") or "").strip()
    asana_email = _first_valid_email(
        [
            asana_data.get("email"),
            getattr(asana_account, "uid", "") if asana_account else "",
        ]
    )
    asana_identity = _first_nonempty_string(
        [
            asana_data.get("name"),
            asana_data.get("email"),
            getattr(asana_account, "uid", "") if asana_account else "",
        ]
    )

    knowledge_user: dict[str, object] = {}
    knowledge_record, _knowledge_error = get_user_record_by_user_id(actor_id)
    if knowledge_record:
        user_document = knowledge_record.get("user_document")
        if isinstance(user_document, dict):
            raw_user = user_document.get("user")
            if isinstance(raw_user, dict):
                knowledge_user = raw_user
    if knowledge_user:
        if not display_name:
            display_name = str(knowledge_user.get("full_name") or "").strip() or username
        if not account_email:
            account_email = str(knowledge_user.get("email") or "").strip().lower()
        if not phone_number:
            phone_number = _normalize_phone(str(knowledge_user.get("phone_number") or ""))

    if microsoft_email:
        account_email = microsoft_email

    return {
        "user_id": actor_id,
        "username": username,
        "display_name": display_name,
        "full_name": full_name,
        "email": account_email,
        "phone_number": phone_number,
        "microsoft_email": microsoft_email,
        "microsoft_identity": microsoft_identity,
        "github_username": github_username,
        "github_identity": github_identity,
        "github_profile_url": github_profile_url,
        "asana_email": asana_email,
        "asana_identity": asana_identity,
        "knowledge_user_record": knowledge_user,
    }


def _ensure_invite_note_in_message(*, message: str, invite_note: str, invite_channel: str) -> str:
    resolved_message = str(message or "").strip()
    resolved_note = str(invite_note or "").strip()
    if not resolved_message or not resolved_note:
        return resolved_message

    note_text = re.sub(r"\s+", " ", resolved_note).strip().lower()
    message_plain_text = _html_to_text_for_email(resolved_message) if _looks_like_html(resolved_message) else resolved_message
    message_text = re.sub(r"\s+", " ", message_plain_text).strip().lower()
    if note_text and note_text in message_text:
        return resolved_message

    if str(invite_channel or "").strip().lower() == UserInvite.CHANNEL_SMS:
        return f"{resolved_message}\nNote: {resolved_note}"[:1200]
    if _looks_like_html(resolved_message):
        escaped_note = html.escape(resolved_note)
        return (
            f"{resolved_message}\n"
            f'<p style="margin:16px 0 0;font-size:14px;line-height:1.6;color:#334155;">'
            f'<strong>Invite note:</strong> {escaped_note}'
            "</p>"
        )
    return f"{resolved_message}\n\nInvite note:\n{resolved_note}"


def _invite_candidate_resources_for_teams(*, actor, team_names: list[str], limit: int = 8) -> list[dict[str, str]]:
    resolved_team_names = [str(item or "").strip() for item in team_names if str(item or "").strip()]
    if not resolved_team_names:
        return []

    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    max_rows = max(1, min(int(limit or 8), 20))

    team_resources = (
        ResourcePackageOwner.objects.select_related("owner_team")
        .filter(
            owner_scope=ResourcePackageOwner.OWNER_SCOPE_TEAM,
            owner_team__name__in=resolved_team_names,
        )
        .order_by("-updated_at", "-created_at")
    )
    for item in team_resources:
        resource_uuid = str(getattr(item, "resource_uuid", "") or "").strip().lower()
        if not resource_uuid or resource_uuid in seen:
            continue
        if actor is not None and not user_can_access_resource(user=actor, resource_uuid=resource_uuid):
            continue
        owner_user, resource = _resolve_resource_owner_and_item(resource_uuid, actor)
        resource_name = str(getattr(resource, "name", "") or "").strip() if resource is not None else ""
        team_name = str(getattr(getattr(item, "owner_team", None), "name", "") or "").strip()
        rows.append(
            {
                "resource_uuid": resource_uuid,
                "resource_name": resource_name or resource_uuid,
                "team_name": team_name,
                "owner_username": str(getattr(owner_user, "username", "") or "").strip(),
            }
        )
        seen.add(resource_uuid)
        if len(rows) >= max_rows:
            return rows

    shared_resources = (
        ResourceTeamShare.objects.select_related("owner", "team")
        .filter(team__name__in=resolved_team_names)
        .order_by("-updated_at", "-created_at")
    )
    for item in shared_resources:
        resource_uuid = str(getattr(item, "resource_uuid", "") or "").strip().lower()
        if not resource_uuid or resource_uuid in seen:
            continue
        if actor is not None and not user_can_access_resource(user=actor, resource_uuid=resource_uuid):
            continue
        owner_user, resource = _resolve_resource_owner_and_item(resource_uuid, actor)
        resource_name = str(getattr(resource, "name", "") or "").strip() if resource is not None else ""
        team_name = str(getattr(getattr(item, "team", None), "name", "") or "").strip()
        rows.append(
            {
                "resource_uuid": resource_uuid,
                "resource_name": resource_name or str(getattr(item, "resource_name", "") or "").strip() or resource_uuid,
                "team_name": team_name,
                "owner_username": str(getattr(getattr(item, "owner", None), "username", "") or "").strip(),
            }
        )
        seen.add(resource_uuid)
        if len(rows) >= max_rows:
            return rows
    return rows


def _tool_invite_resource_health_check_for_actor(*, actor, args: dict, allowed_resource_uuids: set[str]) -> dict:
    resource_uuid = str(args.get("resource_uuid") or "").strip().lower()
    if not resource_uuid:
        return {"ok": False, "error": "resource_uuid is required"}
    if resource_uuid not in allowed_resource_uuids:
        return {"ok": False, "error": f"resource is not in the invite scope: {resource_uuid}"}
    result = _tool_resource_health_check_for_actor(actor, {"resource_uuid": resource_uuid})
    if not bool(result.get("ok")):
        return result
    return {
        "ok": True,
        "resource_uuid": resource_uuid,
        "resource_name": str(result.get("resource_name") or ""),
        "status": str(result.get("status") or ""),
        "checked_at": str(result.get("checked_at") or ""),
        "target": str(result.get("target") or ""),
        "error": str(result.get("error") or ""),
        "latency_ms": result.get("latency_ms"),
    }


def _tool_invite_resource_recent_error_log_for_actor(*, actor, args: dict, allowed_resource_uuids: set[str]) -> dict:
    resource_uuid = str(args.get("resource_uuid") or "").strip().lower()
    if not resource_uuid:
        return {"ok": False, "error": "resource_uuid is required"}
    if resource_uuid not in allowed_resource_uuids:
        return {"ok": False, "error": f"resource is not in the invite scope: {resource_uuid}"}

    owner_user, resource = _resolve_resource_owner_and_item(resource_uuid, actor)
    if owner_user is None or resource is None:
        return {"ok": False, "error": f"resource not found: {resource_uuid}"}

    logs = list_resource_logs(owner_user, resource_uuid, limit=160)
    latest_error = None
    for row in logs:
        if not isinstance(row, dict):
            continue
        level = str(row.get("level") or "").strip().lower()
        if level in {"error", "alert", "critical", "exception"}:
            latest_error = row
            break

    if latest_error is None:
        return {
            "ok": True,
            "resource_uuid": resource_uuid,
            "resource_name": str(getattr(resource, "name", "") or ""),
            "has_error": False,
            "message": "No recent error logs found.",
        }
    metadata = latest_error.get("metadata") if isinstance(latest_error.get("metadata"), dict) else {}
    return {
        "ok": True,
        "resource_uuid": resource_uuid,
        "resource_name": str(getattr(resource, "name", "") or ""),
        "has_error": True,
        "level": str(latest_error.get("level") or "").strip().lower(),
        "timestamp": str(latest_error.get("timestamp") or "").strip(),
        "logger": str(latest_error.get("logger") or "").strip(),
        "message": str(latest_error.get("message") or "").strip(),
        "metadata": metadata,
    }


def _generate_invite_delivery_message_with_agent(
    *,
    actor,
    invite_channel: str,
    invite_url: str,
    allowed_labels: str,
    expiry_text: str,
    invite_note: str,
    invited_username: str,
    invited_email: str,
    invited_phone: str,
    team_names: list[str],
    feature_keys: list[str],
    signup_methods: list[str],
) -> str:
    setup = get_setup_state()
    api_key = str(getattr(setup, "openai_api_key", "") or "").strip()
    if not api_key:
        return ""

    model = (
        str(getattr(settings, "ALSHIVAL_OPENAI_CHAT_MODEL", "") or "").strip()
        or str(getattr(setup, "default_model", "") or "").strip()
        or get_alshival_default_model()
    )

    invite_resources = _invite_candidate_resources_for_teams(
        actor=actor,
        team_names=team_names,
        limit=8,
    )
    invite_resource_uuids = {str(item.get("resource_uuid") or "").strip().lower() for item in invite_resources}
    invite_resource_uuids.discard("")

    tools_spec: list[dict] = []
    if invite_resources:
        tools_spec = [
            {
                "type": "function",
                "function": {
                    "name": "invite_list_resources",
                    "description": "List resources available to this invite from the selected teams.",
                    "parameters": {"type": "object", "properties": {}, "required": []},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "invite_resource_health_check",
                    "description": "Run a health check for one invite-scoped resource.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "resource_uuid": {"type": "string"},
                        },
                        "required": ["resource_uuid"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "invite_resource_recent_error_log",
                    "description": "Fetch one recent error/alert log sample for an invite-scoped resource.",
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

    creator_profile = _invite_creator_profile_context(actor)
    actor_username = str(getattr(actor, "username", "") or "").strip()
    context_payload = {
        "delivery_channel": str(invite_channel or "").strip().lower(),
        "invited_username": invited_username,
        "invited_email": invited_email,
        "invited_phone": invited_phone,
        "allowed_signup_methods": [str(item or "").strip() for item in signup_methods if str(item or "").strip()],
        "allowed_signup_methods_text": allowed_labels,
        "team_names": [str(item or "").strip() for item in team_names if str(item or "").strip()],
        "feature_keys": [str(item or "").strip().lower() for item in feature_keys if str(item or "").strip()],
        "invite_note": invite_note,
        "invite_url": invite_url,
        "invite_expires": expiry_text,
        "created_by_username": actor_username,
        "created_by_display_name": str(creator_profile.get("display_name") or ""),
        "created_by_email": str(creator_profile.get("email") or ""),
        "created_by_phone_number": str(creator_profile.get("phone_number") or ""),
        "created_by_microsoft_email": str(creator_profile.get("microsoft_email") or ""),
        "created_by_github_username": str(creator_profile.get("github_username") or ""),
        "created_by_asana_email": str(creator_profile.get("asana_email") or ""),
        "inviter_profile": creator_profile,
    }

    style_rule = (
        "For email delivery, output email-compatible HTML only (no markdown), use inline styles, and avoid scripts/forms/external CSS."
        if str(invite_channel or "").strip().lower() == UserInvite.CHANNEL_EMAIL
        else "Output plain text only, concise SMS under 550 characters."
    )

    messages: list[dict] = [
        {
            "role": "system",
            "content": "\n".join(
                [
                    "You are Alshival and are inviting a new user to the platform where you will help them monitor resources and project management.",
                    "Write in first person as Alshival so the message feels like it came directly from the site agent.",
                    "For email invites, personalize and decorate the invitation with tasteful, email-safe HTML.",
                    "Be clear, concise, and professional.",
                    style_rule,
                    "Always include invite URL, allowed sign-in methods, and invite expiry.",
                    "For email invites, include a primary Open Invitation button using invite_url.",
                    "For email invites, add a bottom section titled Additional sign-up options with one button-style link per allowed sign-in method.",
                    "Use inviter identity context to identify who created the invite and how the recipient can follow up with them.",
                    "If an invite note is provided, include it explicitly in the message.",
                    "Explicitly tell the recipient that Alshival personally decorated this welcome invitation.",
                    "If resource tools are available, call at least one tool and mention one factual resource health/log detail when useful.",
                    "Remind them at the end that you are there to answer their questions.",
                    "Do not invent resource data; only use tool output.",
                ]
            ),
        },
        {
            "role": "user",
            "content": f"Compose invite message from this JSON context:\n{json.dumps(context_payload)}",
        },
    ]

    def _run_invite_tool(tool_name: str, args: dict) -> dict:
        if tool_name == "invite_list_resources":
            return {
                "ok": True,
                "resource_count": len(invite_resources),
                "resources": invite_resources,
            }
        if tool_name == "invite_resource_health_check":
            return _tool_invite_resource_health_check_for_actor(
                actor=actor,
                args=args if isinstance(args, dict) else {},
                allowed_resource_uuids=invite_resource_uuids,
            )
        if tool_name == "invite_resource_recent_error_log":
            return _tool_invite_resource_recent_error_log_for_actor(
                actor=actor,
                args=args if isinstance(args, dict) else {},
                allowed_resource_uuids=invite_resource_uuids,
            )
        return {"ok": False, "error": f"unknown invite tool: {tool_name}"}

    max_rounds = 4
    for _ in range(max_rounds):
        request_payload: dict[str, object] = {
            "model": model,
            "messages": messages,
            "temperature": 0.3,
        }
        if tools_spec:
            request_payload["tools"] = tools_spec
            request_payload["tool_choice"] = "auto"
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=request_payload,
                timeout=40,
            )
        except requests.RequestException:
            return ""
        if int(response.status_code) >= 400:
            return ""

        payload = response.json() if response.content else {}
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        if not isinstance(message, dict):
            return ""

        tool_calls = message.get("tool_calls")
        if isinstance(tool_calls, list) and tool_calls:
            messages.append(
                {
                    "role": "assistant",
                    "content": message.get("content") or "",
                    "tool_calls": tool_calls,
                }
            )
            for call_item in tool_calls:
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
                result_obj = _run_invite_tool(tool_name, parsed_args)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": json.dumps(result_obj),
                    }
                )
            continue

        reply = _extract_chat_completion_text(payload)
        return _ensure_invite_note_in_message(
            message=str(reply or "").strip(),
            invite_note=invite_note,
            invite_channel=invite_channel,
        )
    return ""


def _default_invite_delivery_message(
    *,
    invite_channel: str,
    invite_url: str,
    allowed_labels: str,
    expiry_text: str,
    invite_note: str,
) -> str:
    resolved_channel = str(invite_channel or "").strip().lower()
    resolved_url = str(invite_url or "").strip() or "[invite link generated when sent]"
    resolved_methods = str(allowed_labels or "").strip() or "Local account"
    resolved_expiry = str(expiry_text or "").strip() or "(set at send time)"
    resolved_note = str(invite_note or "").strip()

    if resolved_channel == UserInvite.CHANNEL_SMS:
        sms_message = (
            "You are invited to Alshival.\n"
            "Alshival personally decorated this welcome invite.\n"
            f"Sign in here: {resolved_url}\n"
            f"Methods: {resolved_methods}\n"
            f"Expires: {resolved_expiry}"
        )
        if resolved_note:
            sms_message = f"{sms_message}\nNote: {resolved_note}"
        return sms_message[:1200]

    return (
        "You have been invited to Alshival.\n\n"
        "Alshival personally decorated this welcome invitation for you.\n\n"
        f"Invite link: {resolved_url}\n"
        f"Allowed sign-in methods: {resolved_methods}\n"
        f"Invite expires: {resolved_expiry}\n\n"
        + (f"Invite note:\n{resolved_note}\n\n" if resolved_note else "")
        + "Open the link and choose how to sign in."
    )


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


def _extract_wiki_title_from_markdown(raw_markdown: str) -> str:
    markdown = str(raw_markdown or "").replace("\r\n", "\n").replace("\r", "\n")
    in_fence = False
    for raw_line in markdown.split("\n"):
        line = str(raw_line or "")
        stripped = line.strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = re.match(r"^\s{0,3}#\s+(.+?)\s*$", line)
        if not match:
            continue
        heading = re.sub(r"\s+#+\s*$", "", match.group(1)).strip()
        if heading:
            return heading
    return ""


def _normalize_wiki_scope(raw_scope: str) -> str:
    scope = str(raw_scope or "").strip().lower()
    if scope == _WIKI_SCOPE_RESOURCE:
        return _WIKI_SCOPE_RESOURCE
    if scope == _WIKI_SCOPE_TEAM:
        return _WIKI_SCOPE_TEAM
    return _WIKI_SCOPE_WORKSPACE


def _normalize_resource_uuid(raw_value: str) -> str:
    return str(raw_value or "").strip().lower()


def _normalize_team_id(raw_value: str) -> str:
    candidate = str(raw_value or "").strip()
    if not candidate:
        return ""
    if candidate.isdigit():
        return str(int(candidate))
    digits = re.sub(r"[^0-9]+", "", candidate)
    if not digits:
        return ""
    return str(int(digits))


def _normalize_wiki_scope_target(
    *,
    raw_scope: str,
    raw_resource_uuid: str = "",
    raw_team_id: str = "",
) -> tuple[str, str, str]:
    scope = _normalize_wiki_scope(raw_scope)
    resource_uuid = ""
    team_id = ""
    if scope == _WIKI_SCOPE_RESOURCE:
        resource_uuid = _normalize_resource_uuid(raw_resource_uuid)
    elif scope == _WIKI_SCOPE_TEAM:
        team_id = _normalize_team_id(raw_team_id or raw_resource_uuid)
    return scope, resource_uuid, team_id


def _normalize_wiki_scope_resource(*, raw_scope: str, raw_resource_uuid: str) -> tuple[str, str]:
    scope, resource_uuid, _ = _normalize_wiki_scope_target(
        raw_scope=raw_scope,
        raw_resource_uuid=raw_resource_uuid,
    )
    return scope, resource_uuid


def _wiki_query_params(
    *,
    scope: str,
    resource_uuid: str = "",
    team_id: str = "",
    status: str = "",
    page_path: str = "",
) -> dict[str, str]:
    normalized_scope, normalized_resource_uuid, normalized_team_id = _normalize_wiki_scope_target(
        raw_scope=scope,
        raw_resource_uuid=resource_uuid,
        raw_team_id=team_id,
    )
    query: dict[str, str] = {}
    if normalized_scope == _WIKI_SCOPE_RESOURCE:
        query["scope"] = _WIKI_SCOPE_RESOURCE
        if normalized_resource_uuid:
            query["resource_uuid"] = normalized_resource_uuid
    elif normalized_scope == _WIKI_SCOPE_TEAM:
        query["scope"] = _WIKI_SCOPE_TEAM
        if normalized_team_id:
            query["team_id"] = normalized_team_id
    if status:
        query["status"] = status
    if page_path:
        query["page"] = page_path
    return query


def _wiki_resource_options_for_user(user) -> list[dict[str, str]]:
    seen: set[str] = set()
    options: list[dict[str, str]] = []

    try:
        for item in list_resources(user):
            resource_uuid = _normalize_resource_uuid(getattr(item, "resource_uuid", ""))
            if not resource_uuid or resource_uuid in seen:
                continue
            seen.add(resource_uuid)
            resource_name = str(getattr(item, "name", "") or "").strip() or resource_uuid
            options.append({"resource_uuid": resource_uuid, "resource_name": resource_name})
    except Exception:
        pass

    team_ids = list(user.groups.values_list("id", flat=True))
    if team_ids:
        shared_rows = (
            ResourceTeamShare.objects.filter(team_id__in=team_ids)
            .order_by("-updated_at", "-created_at")
            .values("resource_uuid", "resource_name")
        )
        for row in shared_rows:
            resource_uuid = _normalize_resource_uuid(row.get("resource_uuid"))
            if not resource_uuid or resource_uuid in seen:
                continue
            seen.add(resource_uuid)
            resource_name = str(row.get("resource_name") or "").strip() or resource_uuid
            options.append({"resource_uuid": resource_uuid, "resource_name": resource_name})

    options.sort(key=lambda item: (item["resource_name"].lower(), item["resource_uuid"]))
    return options


def _wiki_resource_name_for_user(*, actor, resource_uuid: str, options_lookup: dict[str, str]) -> str:
    normalized_uuid = _normalize_resource_uuid(resource_uuid)
    if not normalized_uuid:
        return ""
    if normalized_uuid in options_lookup:
        return options_lookup[normalized_uuid]

    team_ids = list(actor.groups.values_list("id", flat=True))
    if team_ids:
        share = (
            ResourceTeamShare.objects.filter(team_id__in=team_ids, resource_uuid=normalized_uuid)
            .order_by("-updated_at", "-created_at")
            .first()
        )
        if share is not None:
            share_name = str(getattr(share, "resource_name", "") or "").strip()
            if share_name:
                return share_name

    resolved_resource = get_resource_by_uuid(actor, normalized_uuid)
    if resolved_resource is not None:
        return str(getattr(resolved_resource, "name", "") or "").strip() or normalized_uuid

    owner_row = (
        ResourcePackageOwner.objects.select_related("owner_user")
        .filter(resource_uuid=normalized_uuid)
        .first()
    )
    owner_user = getattr(owner_row, "owner_user", None)
    if owner_user is not None and user_can_access_resource(user=actor, resource_uuid=normalized_uuid):
        owner_resource = get_resource_by_uuid(owner_user, normalized_uuid)
        if owner_resource is not None:
            return str(getattr(owner_resource, "name", "") or "").strip() or normalized_uuid
    return normalized_uuid


def _wiki_team_options_for_user(user) -> list[dict[str, str]]:
    if user.is_superuser:
        qs = Group.objects.all()
    else:
        qs = user.groups.all()
    return [
        {
            "team_id": str(int(item.id)),
            "team_name": str(item.name or "").strip() or str(int(item.id)),
        }
        for item in qs.order_by("name")
    ]


def _user_can_access_team(*, actor, team_id: str) -> bool:
    normalized_team_id = _normalize_team_id(team_id)
    if not normalized_team_id:
        return False
    team_qs = Group.objects.filter(id=int(normalized_team_id))
    if not team_qs.exists():
        return False
    if actor.is_superuser:
        return True
    return actor.groups.filter(id=int(normalized_team_id)).exists()


def _wiki_team_name_for_user(*, actor, team_id: str, options_lookup: dict[str, str]) -> str:
    normalized_team_id = _normalize_team_id(team_id)
    if not normalized_team_id:
        return ""
    if normalized_team_id in options_lookup:
        return options_lookup[normalized_team_id]
    if not _user_can_access_team(actor=actor, team_id=normalized_team_id):
        return ""
    team_name = (
        Group.objects.filter(id=int(normalized_team_id))
        .values_list("name", flat=True)
        .first()
    )
    return str(team_name or "").strip() or normalized_team_id


def _resolve_wiki_scope_context(
    *,
    actor,
    raw_scope: str,
    raw_resource_uuid: str,
    raw_team_id: str = "",
) -> dict[str, object]:
    scope, resource_uuid, team_id = _normalize_wiki_scope_target(
        raw_scope=raw_scope,
        raw_resource_uuid=raw_resource_uuid,
        raw_team_id=raw_team_id,
    )
    resource_options = _wiki_resource_options_for_user(actor)
    resource_lookup = {item["resource_uuid"]: item["resource_name"] for item in resource_options}
    team_options = _wiki_team_options_for_user(actor)
    team_lookup = {item["team_id"]: item["team_name"] for item in team_options}
    status_code = ""
    resource_name = ""
    team_name = ""

    if scope == _WIKI_SCOPE_RESOURCE:
        if not resource_uuid:
            if resource_options:
                resource_uuid = resource_options[0]["resource_uuid"]
            else:
                status_code = "wiki_resource_required"
                scope = _WIKI_SCOPE_WORKSPACE
        if scope == _WIKI_SCOPE_RESOURCE and resource_uuid:
            if not user_can_access_resource(user=actor, resource_uuid=resource_uuid):
                status_code = "wiki_resource_no_access"
                scope = _WIKI_SCOPE_WORKSPACE
                resource_uuid = ""
            else:
                resource_name = _wiki_resource_name_for_user(
                    actor=actor,
                    resource_uuid=resource_uuid,
                    options_lookup=resource_lookup,
                )
    if scope == _WIKI_SCOPE_TEAM:
        if not team_id:
            if team_options:
                team_id = team_options[0]["team_id"]
            else:
                status_code = "wiki_team_required"
                scope = _WIKI_SCOPE_WORKSPACE
        if scope == _WIKI_SCOPE_TEAM and team_id:
            if not _user_can_access_team(actor=actor, team_id=team_id):
                status_code = "wiki_team_no_access"
                scope = _WIKI_SCOPE_WORKSPACE
                team_id = ""
            else:
                team_name = _wiki_team_name_for_user(
                    actor=actor,
                    team_id=team_id,
                    options_lookup=team_lookup,
                )
    if scope != _WIKI_SCOPE_RESOURCE:
        resource_uuid = ""
        resource_name = ""
    if scope != _WIKI_SCOPE_TEAM:
        team_id = ""
        team_name = ""

    return {
        "scope": scope,
        "resource_uuid": resource_uuid,
        "resource_name": resource_name,
        "resource_options": resource_options,
        "resource_lookup": resource_lookup,
        "team_id": team_id,
        "team_name": team_name,
        "team_options": team_options,
        "team_lookup": team_lookup,
        "status_code": status_code,
    }


def _normalize_wiki_team_names(user, raw_team_names: list[str]) -> list[str]:
    allowed = set(_ssh_team_choices_for_user(user))
    resolved: list[str] = []
    for value in raw_team_names:
        team_name = str(value or "").strip()
        if team_name and team_name in allowed and team_name not in resolved:
            resolved.append(team_name)
    return resolved


def _wiki_accessible_queryset(user, *, scope: str, resource_uuid: str = "", team_id: str = ""):
    resolved_scope, resolved_resource_uuid, resolved_team_id = _normalize_wiki_scope_target(
        raw_scope=scope,
        raw_resource_uuid=resource_uuid,
        raw_team_id=team_id,
    )
    scope_key = ""
    if resolved_scope == _WIKI_SCOPE_RESOURCE:
        scope_key = resolved_resource_uuid
    elif resolved_scope == _WIKI_SCOPE_TEAM:
        scope_key = resolved_team_id
    qs = WikiPage.objects.filter(
        scope=resolved_scope,
        resource_uuid=scope_key,
    ).prefetch_related("team_access")
    if user.is_superuser:
        return qs

    draft_filter = Q(is_draft=True, created_by_id=user.id)
    published_filter = Q(is_draft=False)
    team_ids = list(user.groups.values_list("id", flat=True))
    if resolved_scope == _WIKI_SCOPE_TEAM:
        if not team_ids:
            return qs.filter(draft_filter).distinct()
        return qs.filter(
            draft_filter
            | (published_filter & Q(team_access__id__in=team_ids))
        ).distinct()
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
    page_scope = _normalize_wiki_scope(getattr(page, "scope", ""))
    if page_scope == _WIKI_SCOPE_TEAM and not page_team_ids:
        return False
    if not page_team_ids:
        return True
    actor_team_ids = set(actor.groups.values_list("id", flat=True))
    return bool(actor_team_ids.intersection(page_team_ids))


def _global_workspace_wiki_record_id(page_id: int) -> str:
    resolved_id = int(page_id or 0)
    if resolved_id <= 0:
        return ""
    return f"workspace_wiki:{resolved_id}"


def _is_global_workspace_wiki_page(page: WikiPage) -> bool:
    if page is None:
        return False
    if _normalize_wiki_scope(getattr(page, "scope", "")) != _WIKI_SCOPE_WORKSPACE:
        return False
    if _normalize_resource_uuid(getattr(page, "resource_uuid", "") or ""):
        return False
    if bool(getattr(page, "is_draft", False)):
        return False
    try:
        return not page.team_access.exists()
    except Exception:
        return False


def _stable_json_hash(value: object) -> str:
    try:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        payload = str(value or "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _workspace_wiki_context_hash(page: WikiPage) -> str:
    team_names: list[str] = []
    try:
        team_names = sorted(
            [
                str(team.name or "").strip()
                for team in page.team_access.all()
                if str(team.name or "").strip()
            ]
        )
    except Exception:
        team_names = []
    payload = {
        "scope": str(getattr(page, "scope", "") or "").strip().lower(),
        "resource_uuid": str(getattr(page, "resource_uuid", "") or "").strip().lower(),
        "path": str(getattr(page, "path", "") or "").strip().lower(),
        "title": str(getattr(page, "title", "") or "").strip(),
        "is_draft": bool(getattr(page, "is_draft", False)),
        "body_markdown": str(getattr(page, "body_markdown", "") or ""),
        "team_names": team_names,
    }
    return _stable_json_hash(payload)


def _collection_metadata_value(collection, *, record_id: str, key: str) -> str:
    if collection is None or not record_id or not key:
        return ""
    try:
        payload = collection.get(ids=[record_id], include=["metadatas"])
    except Exception:
        return ""
    metadatas = payload.get("metadatas") if isinstance(payload, dict) else None
    if not isinstance(metadatas, list) or not metadatas:
        return ""
    first = metadatas[0]
    if not isinstance(first, dict):
        return ""
    return str(first.get(key) or "").strip()


def _collection_metadata_values(collection, *, record_ids: list[str], key: str) -> dict[str, str]:
    resolved_ids = [str(item or "").strip() for item in record_ids if str(item or "").strip()]
    if collection is None or not resolved_ids or not key:
        return {}
    try:
        payload = collection.get(ids=resolved_ids, include=["metadatas"])
    except Exception:
        return {}
    payload_ids = payload.get("ids") if isinstance(payload, dict) else []
    payload_metas = payload.get("metadatas") if isinstance(payload, dict) else []
    if isinstance(payload_ids, list) and payload_ids and isinstance(payload_ids[0], list):
        payload_ids = payload_ids[0]
    if isinstance(payload_metas, list) and payload_metas and isinstance(payload_metas[0], list):
        payload_metas = payload_metas[0]
    if not isinstance(payload_ids, list) or not isinstance(payload_metas, list):
        return {}
    results: dict[str, str] = {}
    for idx, item_id in enumerate(payload_ids):
        resolved_id = str(item_id or "").strip()
        if not resolved_id:
            continue
        metadata = payload_metas[idx] if idx < len(payload_metas) and isinstance(payload_metas[idx], dict) else {}
        results[resolved_id] = str(metadata.get(key) or "").strip()
    return results


def _sync_global_workspace_wiki_kb_page(*, page: WikiPage, force_delete: bool = False) -> None:
    record_id = _global_workspace_wiki_record_id(int(getattr(page, "id", 0) or 0))
    if not record_id:
        return

    _ensure_runtime_cache_dirs()
    try:
        import chromadb
    except Exception:
        return

    global_kb_path = _global_owner_dir() / "knowledge.db"
    try:
        client = chromadb.PersistentClient(path=str(global_kb_path))
        collection = client.get_or_create_collection(name="resources")
    except Exception:
        return

    should_upsert = (not force_delete) and _is_global_workspace_wiki_page(page)
    if not should_upsert:
        try:
            collection.delete(ids=[record_id])
        except Exception:
            pass
        return

    title = str(getattr(page, "title", "") or "").strip()
    path = str(getattr(page, "path", "") or "").strip()
    body_markdown = str(getattr(page, "body_markdown", "") or "").strip()
    doc_parts = [title, path, body_markdown]
    document = " | ".join(part for part in doc_parts if part)
    if not document:
        document = title or path or f"workspace_wiki_page_{int(getattr(page, 'id', 0) or 0)}"
    updated_value = str(getattr(page, "updated_at", "") or "")
    updated_at = updated_value.strip() if isinstance(updated_value, str) else ""
    if not updated_at and getattr(page, "updated_at", None) is not None:
        try:
            updated_at = page.updated_at.isoformat()
        except Exception:
            updated_at = str(page.updated_at)

    metadata = {
        "source": "workspace_wiki",
        "collection_name": "resources",
        "owner_scope": "global",
        "owner_user_id": 0,
        "owner_team_id": 0,
        "resource_uuid": "",
        "wiki_page_id": int(getattr(page, "id", 0) or 0),
        "title": title,
        "path": path,
        "is_draft": bool(getattr(page, "is_draft", False)),
        "updated_at": updated_at,
    }
    context_hash = _workspace_wiki_context_hash(page)
    metadata["workspace_context_hash"] = context_hash
    existing_context_hash = _collection_metadata_value(
        collection,
        record_id=record_id,
        key="workspace_context_hash",
    )
    if existing_context_hash and existing_context_hash == context_hash:
        return
    try:
        collection.upsert(
            ids=[record_id],
            documents=[document],
            metadatas=[metadata],
        )
    except Exception:
        pass


_DEFAULT_SDK_WIKI_MARKDOWN = """# Alshival SDK / Cloud Logs / MCP Servers

Use this page as a quick start for integrating the Alshival Python SDK with this self-deployed instance.

## Common Use Cases
- Send structured cloud logs from your apps and workers into Alshival resource timelines.
- Route error and alert events to app/SMS/email notifications.
- Reuse Alshival MCP tools in other agents via SDK tool specs.

## 1) Install the SDK
```bash
pip install git+https://github.com/Alshival-Ai/alshival.git@main
```

## 2) Configure for this self-hosted deployment
```bash
export ALSHIVAL_USERNAME="<your-username>"
export ALSHIVAL_API_KEY="<account-api-key>"
export ALSHIVAL_RESOURCE="https://<your-domain>/u/<resource-owner>/resources/<resource-uuid>/"
```

`ALSHIVAL_RESOURCE` is preferred because it auto-derives the correct base URL and path prefix for your deployment.

## 3) Send Cloud Logs
```python
import alshival

alshival.log.info("service started")
alshival.log.warning("cache miss", extra={"key": "user:42"})
alshival.log.error("db connection failed")
alshival.log.alert("incident detected", extra={"service": "payments"})
```

## 4) MCP Servers in Other Agents
```python
import alshival

tools = [
    alshival.mcp,
    alshival.mcp.github,
]
```

Example (OpenAI Responses):
```python
from openai import OpenAI
import alshival

client = OpenAI()
response = client.responses.create(
    model="gpt-4.1",
    input="Summarize incidents and suggest runbook updates.",
    tools=[alshival.mcp, alshival.mcp.github],
)
print(response.output_text)
```

## Notes
- For shared resources, keep your own `ALSHIVAL_USERNAME` and API key, but target the owner resource URL in `ALSHIVAL_RESOURCE`.
- If logs do not appear, verify:
  1. Resource URL owner/UUID match an existing resource
  2. API key and username identity are valid
  3. Outbound network/TLS settings allow access to this deployment
"""

_DEFAULT_SDK_WIKI_ALERTS_MARKDOWN = """# Cloud Logs Alerts and Notifications

Use this page to configure how cloud log errors and alerts notify your team.

## What Triggers Alerts
- Log entries with level `error`
- Log entries with level `alert`

These can be generated from:
- `alshival.log.error(...)`
- `alshival.log.alert(...)`

## Notification Channels
Per resource, users can choose:
- App notifications
- SMS notifications (Twilio required)
- Email notifications (Microsoft support inbox + monitoring enabled)

## Example
```python
import alshival

alshival.log.error(
    "payment webhook timeout",
    extra={"service": "billing", "request_id": "req_12345"},
)
```

## Recommended Team Workflow
1. Set production services to at least `ALSHIVAL_CLOUD_LEVEL=INFO`
2. Add actionable metadata (`service`, `request_id`, `tenant`, `region`)
3. Route `error` and `alert` to app + one external channel (SMS or email)
4. Keep runbook links in resource wiki pages and reference them in alerts

## UI + SDK
- SDK sends cloud logs from remote code/services.
- UI receives and displays those logs per resource timeline.
- UI notification settings control how `error`/`alert` events reach your team.
- For full end-to-end setup, see: `/alshival-sdk/cloud-logs/ui-and-sdk-monitoring-workflow`
"""

_DEFAULT_SDK_WIKI_UI_WORKFLOW_MARKDOWN = """# UI + SDK Monitoring Workflow

This workflow combines SDK instrumentation in your remote code with Alshival UI operations.

## 1) Instrument Remote Services with the SDK
Set environment:
```bash
export ALSHIVAL_USERNAME="<your-username>"
export ALSHIVAL_API_KEY="<account-api-key>"
export ALSHIVAL_RESOURCE="https://<your-domain>/u/<resource-owner>/resources/<resource-uuid>/"
```

Add logs:
```python
import alshival

alshival.log.info("worker online", extra={"service": "jobs"})
alshival.log.error("queue timeout", extra={"service": "jobs", "queue": "payments"})
```

## 2) Use the UI for Resource Visibility
In the UI:
- Open **Resources** and select the target resource.
- Review **Cloud Logs** for timeline and error context.
- Run **Health Check** to compare telemetry vs. live status.
- Add wiki/runbook notes under the same resource.

## 3) Configure Notifications in UI
Per resource alert settings:
- `Cloud log errors` to App/SMS/Email
- `Health status transitions` to App/SMS/Email

Requirements:
- SMS: Twilio connector configured
- Email: Microsoft support inbox configured and support inbox monitoring enabled

## 4) Operational Loop (Recommended)
1. SDK emits rich logs (`service`, `request_id`, `tenant`, `region`)
2. UI raises alert on `error` / `alert` levels
3. On-call opens resource details, reviews logs, runs health check
4. Team updates wiki runbook and closes the incident

## 5) MCP with Other Agents
If you use other agents, expose Alshival MCP tools via SDK:
```python
import alshival

tools = [alshival.mcp, alshival.mcp.github]
```

This lets agents query context and act with the same monitored resource data.
"""

_DEFAULT_SDK_WIKI_UI_OVERVIEW_MARKDOWN = """# UI Overview

This page explains where to operate day-to-day in the Alshival UI.

## Core Areas
- **Overview**: high-level status, alert counts, and trend snapshots.
- **Resources**: infrastructure/service inventory, health checks, and cloud logs.
- **Wiki**: runbooks, onboarding notes, architecture references, and incident playbooks.
- **Team**: shared team chat, team-scoped resource access, and team wiki views.
- **Directory** (admin): users, teams, permissions, and invites.

## Recommended Daily Routine
1. Start in **Overview** for current alert pressure.
2. Open affected items in **Resources** for logs + health checks.
3. Update **Wiki** pages with new findings/runbook steps.
4. Use **Team** chat to coordinate action and handoff.

## UI + SDK Together
- SDK emits structured events from remote code.
- UI stores and displays those events under the mapped resource.
- Alerts route based on resource notification settings.
"""

_DEFAULT_SDK_WIKI_ACCOUNT_SETTINGS_MARKDOWN = """# Account Settings

Account Settings is where each user connects identity providers and personal integrations.

## What Users Can Do
- Connect/disconnect supported social providers from their own account.
- Verify identity connection status and linked profile.
- Control account-level defaults used by integrated workflows.

## What Admins Configure Separately
- Connector app credentials (client IDs/secrets, keys, webhooks) in Connector Settings.
- Platform-wide behavior in Alshival Admin.

## Important Distinction
- **Configured connector**: platform credentials are set by admin.
- **Connected account**: user has linked their own identity/token.

Many features require both.
"""

_DEFAULT_SDK_WIKI_CONNECTORS_OVERVIEW_MARKDOWN = """# Connectors Overview

Connectors are split across two layers:

## Layer 1: Platform Connector Configuration (Admin)
- OpenAI API key/model access
- Microsoft app credentials + mailbox (for support inbox)
- GitHub OAuth app
- Asana OAuth app
- Twilio SMS credentials/webhooks

## Layer 2: User Account Connection
- Microsoft/GitHub/Asana are connected per user in Account Settings.
- Twilio and OpenAI are platform-level and not user OAuth connections.

## Quick Impact Summary
- **OpenAI**: powers Ask/AI generation features.
- **Microsoft**: user delegated access (calendar/email) + support inbox app mailbox.
- **GitHub**: GitHub identity + optional Ask GitHub MCP tools when enabled.
- **Asana**: Asana identity + planner/task/board flows.
- **Twilio**: SMS delivery for invites, alerts, and chat notifications.
"""

_DEFAULT_SDK_WIKI_CONNECTOR_OPENAI_MARKDOWN = """# Connector: OpenAI

## What It Does
- Enables Ask Alshival chat and other AI-assisted generation flows.
- Powers invite message generation and related content generation paths.

## Scope
- Platform-level configuration.
- Not a per-user OAuth connection.

## If You Configure It
- AI features can run for users with access to those UI features.
- If missing, AI-dependent actions fall back or return unavailable errors.
"""

_DEFAULT_SDK_WIKI_CONNECTOR_MICROSOFT_MARKDOWN = """# Connector: Microsoft

## What It Does
- Enables Microsoft account connection for users.
- Supports delegated token workflows (for example, user-scoped mail/calendar operations).
- Supports support inbox mailbox sending/ingestion using app credentials.

## Scope
- Admin config: app credentials + tenant + support inbox mailbox.
- User connection: Microsoft account OAuth link in Account Settings.

## If a User Connects Their Microsoft Account
- Their delegated identity can be used for user-scoped Microsoft actions.
- They can use Microsoft-backed personal integration features exposed in UI.

## Support Inbox Notes
- Email alerts and support mailbox workflows depend on support inbox settings being enabled by admin.
"""

_DEFAULT_SDK_WIKI_CONNECTOR_GITHUB_MARKDOWN = """# Connector: GitHub

## What It Does
- Enables GitHub account login/connection.
- Allows optional GitHub MCP tools in Ask agent flows when enabled by admin.

## Scope
- Admin config: GitHub OAuth app credentials.
- User connection: GitHub OAuth in Account Settings.

## If a User Connects GitHub
- Their account can use GitHub-linked flows.
- Ask agent attaches GitHub MCP tools only when:
  1. Connector is configured
  2. Admin enabled GitHub MCP for Ask
  3. User has connected GitHub
"""

_DEFAULT_SDK_WIKI_CONNECTOR_ASANA_MARKDOWN = """# Connector: Asana

## What It Does
- Connects user Asana identity.
- Enables Asana task/board/calendar integrations and mapping into resource workflows.

## Scope
- Admin config: Asana OAuth app credentials.
- User connection: Asana OAuth link in Account Settings.

## If a User Connects Asana
- Their Asana tasks can be synced/displayed in planner views.
- Task/board actions can be mapped to Alshival resources where supported.
"""

_DEFAULT_SDK_WIKI_CONNECTOR_TWILIO_MARKDOWN = """# Connector: Twilio

## What It Does
- Enables SMS delivery and webhook handling for messaging workflows.

## Scope
- Platform-level credentials and webhook config only.
- No per-user OAuth connection.

## If It Is Configured
- SMS invites can be delivered.
- SMS alert channels become available where supported.
- Team/chat and notification workflows can route via SMS when enabled by users/admin.
"""

_DEFAULT_SDK_WIKI_PAGES = [
    {
        "path": "alshival-sdk/cloud-logs/mcp-servers",
        "title": "Alshival SDK / Cloud Logs / MCP Servers",
        "markdown": _DEFAULT_SDK_WIKI_MARKDOWN,
    },
    {
        "path": "alshival-sdk/cloud-logs/alerts-and-notifications",
        "title": "Cloud Logs Alerts and Notifications",
        "markdown": _DEFAULT_SDK_WIKI_ALERTS_MARKDOWN,
    },
    {
        "path": "alshival-sdk/cloud-logs/ui-and-sdk-monitoring-workflow",
        "title": "UI + SDK Monitoring Workflow",
        "markdown": _DEFAULT_SDK_WIKI_UI_WORKFLOW_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/ui-overview",
        "title": "UI Overview",
        "markdown": _DEFAULT_SDK_WIKI_UI_OVERVIEW_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/account-settings",
        "title": "Account Settings",
        "markdown": _DEFAULT_SDK_WIKI_ACCOUNT_SETTINGS_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/connectors/overview",
        "title": "Connectors Overview",
        "markdown": _DEFAULT_SDK_WIKI_CONNECTORS_OVERVIEW_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/connectors/openai",
        "title": "Connector: OpenAI",
        "markdown": _DEFAULT_SDK_WIKI_CONNECTOR_OPENAI_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/connectors/microsoft",
        "title": "Connector: Microsoft",
        "markdown": _DEFAULT_SDK_WIKI_CONNECTOR_MICROSOFT_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/connectors/github",
        "title": "Connector: GitHub",
        "markdown": _DEFAULT_SDK_WIKI_CONNECTOR_GITHUB_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/connectors/asana",
        "title": "Connector: Asana",
        "markdown": _DEFAULT_SDK_WIKI_CONNECTOR_ASANA_MARKDOWN,
    },
    {
        "path": "alshival-sdk/platform/connectors/twilio",
        "title": "Connector: Twilio",
        "markdown": _DEFAULT_SDK_WIKI_CONNECTOR_TWILIO_MARKDOWN,
    },
]


def _ensure_default_sdk_workspace_wiki_page(*, actor) -> None:
    if actor is None or not bool(getattr(actor, "is_authenticated", False)):
        return

    for item in _DEFAULT_SDK_WIKI_PAGES:
        path = str(item.get("path") or "").strip().lower()
        title = str(item.get("title") or "").strip()
        markdown = str(item.get("markdown") or "").strip()
        if not path or not title or not markdown:
            continue

        try:
            existing = (
                WikiPage.objects.filter(
                    scope=_WIKI_SCOPE_WORKSPACE,
                    resource_uuid="",
                    path__iexact=path,
                )
                .order_by("id")
                .first()
            )
        except (OperationalError, ProgrammingError):
            return
        except Exception:
            continue

        if existing is not None:
            page_updated = False
            try:
                # One-time upgrade for previously seeded alerts doc to include
                # the newer UI + SDK guidance.
                if (
                    path == "alshival-sdk/cloud-logs/alerts-and-notifications"
                    and str(existing.title or "").strip() == title
                ):
                    existing_body = str(existing.body_markdown or "")
                    legacy_seed_phrase = "Use this page to configure how cloud log errors and alerts notify your team."
                    if legacy_seed_phrase in existing_body and "## UI + SDK" not in existing_body:
                        existing.body_markdown = markdown
                        existing.body_html_fallback = render_markdown_fallback(markdown)
                        existing.updated_by = actor
                        existing.save(
                            update_fields=[
                                "body_markdown",
                                "body_html_fallback",
                                "updated_by",
                                "updated_at",
                            ]
                        )
                        page_updated = True
            except Exception:
                pass
            if page_updated:
                try:
                    if _is_global_workspace_wiki_page(existing):
                        _sync_global_workspace_wiki_kb_page(page=existing)
                except Exception:
                    pass
            continue

        try:
            with transaction.atomic():
                page = WikiPage.objects.create(
                    scope=_WIKI_SCOPE_WORKSPACE,
                    resource_uuid="",
                    resource_name="",
                    path=path,
                    title=title,
                    is_draft=False,
                    body_markdown=markdown,
                    body_html_fallback=render_markdown_fallback(markdown),
                    created_by=actor,
                    updated_by=actor,
                )
        except Exception:
            continue

        try:
            _sync_global_workspace_wiki_kb_page(page=page)
        except Exception:
            pass


def _upsert_resource_kb_after_wiki_mutation(*, actor, resource_uuid: str) -> None:
    resolved_uuid = _normalize_resource_uuid(resource_uuid)
    if not resolved_uuid:
        return

    owner_user = None
    resource = None
    if actor is not None and bool(getattr(actor, "is_authenticated", False)):
        owner_user = actor
        try:
            resource = get_resource_by_uuid(owner_user, resolved_uuid)
        except Exception:
            resource = None

    if resource is None:
        owner_user, resource = _resolve_resource_owner_and_item(resolved_uuid, actor)
    if owner_user is None or resource is None:
        return

    status = str(getattr(resource, "last_status", "") or "").strip().lower() or "unknown"
    checked_at = str(getattr(resource, "last_checked_at", "") or "").strip()
    error = str(getattr(resource, "last_error", "") or "").strip()
    check_method = ""
    latency_ms = None
    packet_loss_pct = None

    try:
        latest_checks = list_resource_checks(owner_user, resolved_uuid, limit=1)
        if latest_checks:
            latest = latest_checks[0]
            latest_status = str(getattr(latest, "status", "") or "").strip().lower()
            if latest_status:
                status = latest_status
            check_method = str(getattr(latest, "check_method", "") or "").strip()
            latency_ms = getattr(latest, "latency_ms", None)
            packet_loss_pct = getattr(latest, "packet_loss_pct", None)
            if not checked_at:
                checked_at = str(getattr(latest, "checked_at", "") or "").strip()
            if not error:
                error = str(getattr(latest, "error", "") or "").strip()
    except Exception:
        pass

    if not checked_at:
        checked_at = datetime.now(timezone.utc).isoformat()
    if not check_method:
        check_method = "wiki_sync"

    try:
        upsert_resource_health_knowledge(
            user=owner_user,
            resource=resource,
            status=status,
            checked_at=checked_at,
            error=error,
            check_method=check_method,
            latency_ms=latency_ms,
            packet_loss_pct=packet_loss_pct,
        )
    except Exception:
        pass


def _redirect_wiki(
    *,
    status: str = "",
    page_path: str = "",
    scope: str = _WIKI_SCOPE_WORKSPACE,
    resource_uuid: str = "",
    team_id: str = "",
):
    query = _wiki_query_params(
        scope=scope,
        resource_uuid=resource_uuid,
        team_id=team_id,
        status=status,
        page_path=page_path,
    )
    if not query:
        return redirect("wiki")
    return redirect(f"{reverse('wiki')}?{urlencode(query)}")


def _redirect_wiki_editor_new(
    *,
    status: str = "",
    scope: str = _WIKI_SCOPE_WORKSPACE,
    resource_uuid: str = "",
    team_id: str = "",
):
    query = _wiki_query_params(
        scope=scope,
        resource_uuid=resource_uuid,
        team_id=team_id,
        status=status,
    )
    if not query:
        return redirect("wiki_editor_new")
    return redirect(f"{reverse('wiki_editor_new')}?{urlencode(query)}")


def _redirect_wiki_editor(
    *,
    page_id: int,
    status: str = "",
    scope: str = _WIKI_SCOPE_WORKSPACE,
    resource_uuid: str = "",
    team_id: str = "",
):
    query = _wiki_query_params(
        scope=scope,
        resource_uuid=resource_uuid,
        team_id=team_id,
        status=status,
    )
    if not query:
        return redirect("wiki_editor", page_id=page_id)
    return redirect(f"{reverse('wiki_editor', kwargs={'page_id': page_id})}?{urlencode(query)}")


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


def _resources_overview_metrics(*, user, resources) -> dict[str, object]:
    now_utc = datetime.now(timezone.utc)
    window_days = int(_RESOURCES_UPTIME_WINDOW_DAYS)
    window_start = now_utc - timedelta(days=window_days)
    api_status_counts = {"healthy": 0, "unhealthy": 0, "unknown": 0}
    api_total = 0
    api_with_healthcheck = 0
    api_latency_samples: list[float] = []
    api_unhealthy_checks_by_name: dict[str, int] = {}

    uptime_checks_total = 0
    uptime_checks_healthy = 0
    uptime_resources_covered = 0
    uptime_latest_check_dt: datetime | None = None

    for item in resources:
        resource_type = str(getattr(item, "resource_type", "") or "").strip().lower()
        resource_name = str(getattr(item, "name", "") or "").strip() or "Unnamed resource"
        resource_uuid = str(getattr(item, "resource_uuid", "") or "").strip()
        resource_status = _normalize_health_status(str(getattr(item, "last_status", "") or ""))
        is_api_resource = resource_type == "api"

        if is_api_resource:
            api_total += 1
            api_status_counts[resource_status] += 1
            if str(getattr(item, "healthcheck_url", "") or "").strip():
                api_with_healthcheck += 1

        if not resource_uuid:
            continue

        last_checked_raw = str(getattr(item, "last_checked_at", "") or "").strip()
        if not last_checked_raw:
            continue
        last_checked_dt = _parse_runtime_timestamp(last_checked_raw)
        if last_checked_dt is not None and last_checked_dt < window_start:
            continue

        try:
            check_items = list_resource_checks(user, resource_uuid, limit=_RESOURCES_UPTIME_CHECK_LIMIT)
        except Exception:
            check_items = []

        has_window_checks = False
        api_unhealthy_in_window = 0
        for check in check_items:
            checked_dt = _parse_runtime_timestamp(str(getattr(check, "checked_at", "") or ""))
            if checked_dt is None or checked_dt < window_start:
                continue

            has_window_checks = True
            if uptime_latest_check_dt is None or checked_dt > uptime_latest_check_dt:
                uptime_latest_check_dt = checked_dt

            check_status = _normalize_health_status(str(getattr(check, "status", "") or ""))
            uptime_checks_total += 1
            if check_status == "healthy":
                uptime_checks_healthy += 1
            if not is_api_resource:
                continue

            if check_status == "unhealthy":
                api_unhealthy_in_window += 1

            latency_raw = getattr(check, "latency_ms", None)
            if latency_raw is None:
                continue
            try:
                latency_value = float(latency_raw)
            except (TypeError, ValueError):
                continue
            if latency_value < 0:
                continue
            api_latency_samples.append(latency_value)

        if has_window_checks:
            uptime_resources_covered += 1
        if is_api_resource and api_unhealthy_in_window > 0:
            api_unhealthy_checks_by_name[resource_name] = (
                int(api_unhealthy_checks_by_name.get(resource_name) or 0) + api_unhealthy_in_window
            )

    uptime_pct: float | None = None
    if uptime_checks_total > 0:
        uptime_pct = round((uptime_checks_healthy / uptime_checks_total) * 100.0, 1)

    uptime_progress_pct = 0
    if uptime_pct is not None:
        uptime_progress_pct = int(max(0, min(100, round(uptime_pct))))

    api_avg_latency_ms: float | None = None
    if api_latency_samples:
        api_avg_latency_ms = round(sum(api_latency_samples) / len(api_latency_samples), 1)

    top_unstable_api_name = ""
    top_unstable_api_failures = 0
    if api_unhealthy_checks_by_name:
        top_unstable_api_name, top_unstable_api_failures = max(
            api_unhealthy_checks_by_name.items(),
            key=lambda item: (int(item[1]), str(item[0]).lower()),
        )

    return {
        "window_days": window_days,
        "window_start_display": _format_display_time(window_start.isoformat()),
        "window_end_display": _format_display_time(now_utc.isoformat()),
        "api_total": api_total,
        "api_healthy": int(api_status_counts["healthy"]),
        "api_degraded": int(api_status_counts["unknown"]),
        "api_down": int(api_status_counts["unhealthy"]),
        "api_with_healthcheck": api_with_healthcheck,
        "api_latency_avg_ms": api_avg_latency_ms,
        "api_unhealthy_checks_total": int(sum(api_unhealthy_checks_by_name.values())),
        "api_top_unstable_name": top_unstable_api_name,
        "api_top_unstable_failures": int(top_unstable_api_failures),
        "uptime_pct": uptime_pct,
        "uptime_progress_pct": uptime_progress_pct,
        "uptime_checks_total": uptime_checks_total,
        "uptime_checks_healthy": uptime_checks_healthy,
        "uptime_checks_unhealthy": max(uptime_checks_total - uptime_checks_healthy, 0),
        "uptime_resources_covered": uptime_resources_covered,
        "uptime_latest_check_display": _format_display_time(
            uptime_latest_check_dt.isoformat() if uptime_latest_check_dt is not None else ""
        ),
    }


def _normalize_log_level_bucket(raw_level: str) -> str:
    level = str(raw_level or "").strip().lower()
    if level in {"critical", "exception", "error", "fatal"}:
        return "error"
    if level in {"warning", "warn", "alert"}:
        return "warning"
    return "info"


_ASANA_API_BASE_URL = "https://app.asana.com/api/1.0"
_ASANA_OVERVIEW_CACHE_KEY = "overview"
_ASANA_FULL_IMPORT_CACHE_KEY = "full-import"
_ASANA_OVERVIEW_CACHE_MAX_AGE_SECONDS = 300
_ASANA_OVERVIEW_TASK_FETCH_LIMIT = 1000
_ASANA_FULL_IMPORT_TASK_FETCH_LIMIT = 10000
_ASANA_OVERVIEW_PER_REQUEST_LIMIT = 100
_ASANA_API_TIMEOUT_SECONDS = 20
_ASANA_TOKEN_REFRESH_URL = "https://app.asana.com/-/oauth_token"
_ASANA_AGENDA_COMPLETED_WINDOW_DAYS = 14
_ASANA_TASK_COMMENT_FETCH_LIMIT = 60
_ASANA_AUTO_ASSIGN_MAX_TASKS = 12
_ASANA_AUTO_ASSIGN_KB_RESULTS_PER_TASK = 4
_ASANA_AUTO_ASSIGN_COMMENT_LIMIT = 4
_ASANA_TASK_OPT_FIELDS = ",".join(
    [
        "gid",
        "name",
        "notes",
        "completed",
        "completed_at",
        "permalink_url",
        "due_on",
        "due_at",
        "created_at",
        "modified_at",
        "memberships.section.name",
        "memberships.project.gid",
        "memberships.project.name",
        "memberships.project.permalink_url",
        "workspace.gid",
        "workspace.name",
        "assignee.gid",
        "assignee.name",
        "num_subtasks",
    ]
)
_MICROSOFT_CONNECTOR_SCOPES = [
    "openid",
    "profile",
    "email",
    "offline_access",
    "User.Read",
    "Calendars.Read",
    "Mail.Read",
    "Mail.Send",
]


def _asana_identity_display(account: SocialAccount | None) -> str:
    if account is None:
        return ""
    extra_data = dict(getattr(account, "extra_data", {}) or {})
    candidates = [
        extra_data.get("name"),
        extra_data.get("email"),
        getattr(account, "uid", ""),
    ]
    for raw_value in candidates:
        value = str(raw_value or "").strip()
        if value:
            return value
    return ""


def _asana_error_message_from_payload(payload: dict[str, object], status_code: int) -> str:
    if status_code in {401, 403}:
        return "Asana authorization expired. Reconnect your Asana account from Settings."
    errors = payload.get("errors")
    if isinstance(errors, list):
        for item in errors:
            if not isinstance(item, dict):
                continue
            message = str(item.get("message") or "").strip()
            if message:
                return f"Asana API error: {message}"
    return f"Asana API error (HTTP {status_code})."


def _asana_error_requires_refresh(error: str | None) -> bool:
    message = str(error or "").strip().lower()
    if not message:
        return False
    if "authorization expired" in message:
        return True
    if "oauth token" in message:
        return True
    return "reconnect your asana account" in message


def _asana_api_request_json(
    *,
    method: str,
    access_token: str,
    path: str,
    params: dict[str, object] | None = None,
    body: dict[str, object] | None = None,
) -> tuple[dict[str, object] | None, str | None]:
    token = str(access_token or "").strip()
    if not token:
        return None, "Asana token is not available."
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    url = f"{_ASANA_API_BASE_URL}{path}"
    try:
        response = requests.request(
            str(method or "GET").strip().upper() or "GET",
            url,
            headers=headers,
            params=params or {},
            json=body if isinstance(body, dict) else None,
            timeout=_ASANA_API_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        return None, "Unable to reach Asana right now."

    try:
        payload = response.json() if response.content else {}
    except ValueError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    if response.status_code >= 400:
        return None, _asana_error_message_from_payload(payload, int(response.status_code))
    return payload, None


def _asana_refresh_access_token(token_row: SocialToken) -> tuple[str, str | None]:
    refresh_token = str(getattr(token_row, "token_secret", "") or "").strip()
    if not refresh_token:
        return "", "Asana authorization expired. Reconnect your Asana account from Settings."

    app = getattr(token_row, "app", None)
    client_id = str(getattr(app, "client_id", "") or "").strip()
    client_secret = str(getattr(app, "secret", "") or "").strip()
    if not client_id or not client_secret:
        return "", "Asana connector is missing client credentials. Update it in Settings."

    form = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    try:
        response = requests.post(
            _ASANA_TOKEN_REFRESH_URL,
            data=form,
            headers={"Accept": "application/json"},
            timeout=_ASANA_API_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        return "", "Unable to refresh Asana authorization right now."

    try:
        payload = response.json() if response.content else {}
    except ValueError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    if response.status_code >= 400:
        return "", _asana_error_message_from_payload(payload, int(response.status_code))

    nested = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    access_token = str(payload.get("access_token") or nested.get("access_token") or "").strip()
    if not access_token:
        return "", "Asana refresh response did not include an access token."

    refresh_token_next = str(payload.get("refresh_token") or nested.get("refresh_token") or "").strip()
    expires_in_raw = payload.get("expires_in", nested.get("expires_in"))
    expires_at = None
    try:
        expires_in = int(expires_in_raw or 0)
    except (TypeError, ValueError):
        expires_in = 0
    if expires_in > 0:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    token_row.token = access_token
    if refresh_token_next:
        token_row.token_secret = refresh_token_next
    token_row.expires_at = expires_at
    update_fields = ["token", "expires_at"]
    if refresh_token_next:
        update_fields.append("token_secret")
    token_row.save(update_fields=update_fields)
    return access_token, None


def _asana_api_get_json(
    *,
    access_token: str,
    path: str,
    params: dict[str, object] | None = None,
) -> tuple[dict[str, object] | None, str | None]:
    return _asana_api_request_json(
        method="GET",
        access_token=access_token,
        path=path,
        params=params,
    )


def _asana_api_list(
    *,
    access_token: str,
    path: str,
    params: dict[str, object],
    max_items: int,
) -> tuple[list[dict[str, object]], bool, str | None]:
    items: list[dict[str, object]] = []
    next_offset = ""
    seen_offsets: set[str] = set()
    truncated = False

    while True:
        request_params = dict(params)
        if next_offset:
            request_params["offset"] = next_offset
        payload, error = _asana_api_get_json(
            access_token=access_token,
            path=path,
            params=request_params,
        )
        if error:
            return [], truncated, error
        if payload is None:
            return [], truncated, "Unexpected empty response from Asana."

        data_rows = payload.get("data")
        if isinstance(data_rows, list):
            for row in data_rows:
                if not isinstance(row, dict):
                    continue
                items.append(row)
                if len(items) >= max_items:
                    truncated = True
                    return items[:max_items], truncated, None

        next_page = payload.get("next_page")
        if not isinstance(next_page, dict):
            break
        raw_offset = str(next_page.get("offset") or "").strip()
        if not raw_offset:
            break
        if raw_offset in seen_offsets:
            break
        seen_offsets.add(raw_offset)
        next_offset = raw_offset

    return items, truncated, None


def _asana_story_rows_for_task(access_token: str, task_gid: str) -> tuple[list[dict[str, object]], str | None]:
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return [], "Task id is required."
    stories, _truncated, fetch_error = _asana_api_list(
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}/stories",
        params={
            "limit": _ASANA_OVERVIEW_PER_REQUEST_LIMIT,
            "opt_fields": ",".join(
                [
                    "gid",
                    "text",
                    "resource_subtype",
                    "type",
                    "created_at",
                    "created_by.gid",
                    "created_by.name",
                    "created_by.email",
                ]
            ),
        },
        max_items=_ASANA_TASK_COMMENT_FETCH_LIMIT,
    )
    if fetch_error:
        return [], fetch_error
    filtered_rows: list[dict[str, object]] = []
    for row in stories:
        if not isinstance(row, dict):
            continue
        subtype = str(row.get("resource_subtype") or "").strip().lower()
        story_type = str(row.get("type") or "").strip().lower()
        if subtype != "comment_added" and story_type != "comment":
            continue
        filtered_rows.append(row)
    filtered_rows.sort(
        key=lambda row: (
            str(row.get("created_at") or "").strip(),
            str(row.get("gid") or "").strip(),
        )
    )
    return filtered_rows, None


def _asana_comment_rows_for_task(access_token: str, task_gid: str) -> tuple[list[dict[str, object]], str | None]:
    story_rows, error = _asana_story_rows_for_task(access_token, task_gid)
    if error:
        return [], error
    comments: list[dict[str, object]] = []
    for row in story_rows:
        if not isinstance(row, dict):
            continue
        gid = str(row.get("gid") or "").strip()
        text = str(row.get("text") or "").strip()
        if not gid or not text:
            continue
        created_at = str(row.get("created_at") or "").strip()
        author_obj = row.get("created_by") if isinstance(row.get("created_by"), dict) else {}
        comments.append(
            {
                "gid": gid,
                "text": text,
                "created_at": created_at,
                "created_display": _format_display_time(created_at),
                "author_gid": str(author_obj.get("gid") or "").strip(),
                "author_name": str(author_obj.get("name") or "").strip() or "Asana user",
            }
        )
    return comments, None


def _asana_comment_snippets_for_task(
    access_token: str,
    task_gid: str,
    *,
    limit: int = _ASANA_AUTO_ASSIGN_COMMENT_LIMIT,
) -> list[str]:
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return []
    comments, error = _asana_comment_rows_for_task(access_token, resolved_task_gid)
    if error:
        return []
    snippets: list[str] = []
    for row in comments:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        if len(text) > 600:
            text = text[:600]
        snippets.append(text)
        if len(snippets) >= max(1, int(limit or _ASANA_AUTO_ASSIGN_COMMENT_LIMIT)):
            break
    return snippets


def _json_object_from_text(raw_text: str) -> dict[str, object]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"decisions": parsed}
    except Exception:
        pass

    start_idx = text.find("{")
    end_idx = text.rfind("}")
    if start_idx >= 0 and end_idx > start_idx:
        candidate = text[start_idx : end_idx + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"decisions": parsed}
        except Exception:
            return {}
    return {}


def _asana_auto_assign_query_text(task_row: dict[str, object], comment_snippets: list[str]) -> str:
    title = str(task_row.get("name") or "").strip()
    notes = str(task_row.get("notes") or "").strip()
    section = str(task_row.get("section_name") or "").strip()
    workspace = str(task_row.get("workspace_name") or "").strip()
    project_names = ", ".join(
        str(project.get("name") or "").strip()
        for project in (task_row.get("project_links") or [])
        if isinstance(project, dict) and str(project.get("name") or "").strip()
    )
    comment_text = " ".join([str(item or "").strip() for item in comment_snippets if str(item or "").strip()])
    parts = [title, notes, section, workspace, project_names, comment_text]
    return " | ".join([part for part in parts if part]).strip()


def _asana_auto_assignments_with_agent(
    *,
    user,
    access_token: str,
    task_rows: list[dict[str, object]],
    resource_options: list[dict[str, str]],
    board_resource_mappings: dict[str, list[str]],
    task_resource_mappings: dict[str, list[str]],
    agenda_item_resource_mappings: dict[str, list[str]] | None = None,
) -> dict[str, list[str]]:
    if not isinstance(task_rows, list) or not task_rows:
        return {}

    normalized_resources: list[dict[str, str]] = []
    allowed_resource_set: set[str] = set()
    for option in resource_options:
        if not isinstance(option, dict):
            continue
        resource_uuid = str(option.get("resource_uuid") or "").strip().lower()
        resource_name = str(option.get("resource_name") or "").strip()
        if not resource_uuid or not resource_name or resource_uuid in allowed_resource_set:
            continue
        allowed_resource_set.add(resource_uuid)
        normalized_resources.append(
            {
                "resource_uuid": resource_uuid,
                "resource_name": resource_name,
            }
        )
    if not normalized_resources:
        return {}

    setup = get_setup_state()
    api_key = str(getattr(setup, "openai_api_key", "") or "").strip()
    if not api_key:
        return {}
    model = (
        str(getattr(settings, "ALSHIVAL_OPENAI_CHAT_MODEL", "") or "").strip()
        or str(getattr(setup, "default_model", "") or "").strip()
        or get_alshival_default_model()
    )

    agenda_mappings = agenda_item_resource_mappings if isinstance(agenda_item_resource_mappings, dict) else {}
    candidates: list[dict[str, object]] = []
    for task_row in task_rows:
        if not isinstance(task_row, dict):
            continue
        task_gid = str(task_row.get("gid") or "").strip()
        if not task_gid:
            continue
        if bool(task_row.get("completed")):
            continue
        existing = _asana_task_resource_uuids(
            task_row=task_row,
            board_resource_mappings=board_resource_mappings,
            task_resource_mappings=task_resource_mappings,
        )
        if existing:
            continue
        agenda_item_id = f"asana-agenda-{task_gid}"
        existing_agenda = [
            str(value or "").strip().lower()
            for value in (agenda_mappings.get(agenda_item_id) or [])
            if str(value or "").strip()
        ]
        if existing_agenda:
            continue
        candidates.append(task_row)

    if not candidates:
        return {}

    candidates.sort(key=_asana_task_row_sort_key)
    candidates = candidates[:_ASANA_AUTO_ASSIGN_MAX_TASKS]

    task_context_rows: list[dict[str, object]] = []
    for task_row in candidates:
        task_gid = str(task_row.get("gid") or "").strip()
        if not task_gid:
            continue
        comments = _asana_comment_snippets_for_task(
            access_token,
            task_gid,
            limit=_ASANA_AUTO_ASSIGN_COMMENT_LIMIT,
        )
        query_text = _asana_auto_assign_query_text(task_row, comments)
        kb_results_payload = _tool_search_kb_for_actor(user, {"query": query_text}) if query_text else {}
        kb_results = kb_results_payload.get("results") if isinstance(kb_results_payload, dict) else []
        kb_rows: list[dict[str, object]] = []
        if isinstance(kb_results, list):
            for kb_row in kb_results[:_ASANA_AUTO_ASSIGN_KB_RESULTS_PER_TASK]:
                if not isinstance(kb_row, dict):
                    continue
                metadata = kb_row.get("metadata") if isinstance(kb_row.get("metadata"), dict) else {}
                if not isinstance(metadata, dict):
                    metadata = {}
                kb_rows.append(
                    {
                        "source": str(metadata.get("source") or "").strip(),
                        "resource_uuid": str(metadata.get("resource_uuid") or "").strip().lower(),
                        "title": str(metadata.get("name") or metadata.get("title") or "").strip(),
                        "snippet": _truncate_kb_result_text(str(kb_row.get("document") or ""), limit=500),
                    }
                )

        project_names = [
            str(project.get("name") or "").strip()
            for project in (task_row.get("project_links") or [])
            if isinstance(project, dict) and str(project.get("name") or "").strip()
        ]
        task_context_rows.append(
            {
                "task_gid": task_gid,
                "title": str(task_row.get("name") or "").strip(),
                "notes": str(task_row.get("notes") or "").strip(),
                "due_date": str(task_row.get("due_date") or "").strip(),
                "due_time": str(task_row.get("due_time") or "").strip(),
                "workspace_name": str(task_row.get("workspace_name") or "").strip(),
                "section_name": str(task_row.get("section_name") or "").strip(),
                "project_names": project_names,
                "comments": comments,
                "kb_matches": kb_rows,
            }
        )
    if not task_context_rows:
        return {}

    messages: list[dict[str, str]] = [
        {
            "role": "system",
            "content": (
                "You map Asana tasks to monitored resources.\n"
                "Use the provided task fields (title, notes, comments, section, projects, due date) and kb_matches.\n"
                "Only assign resources when there is clear evidence. If unsure, leave the task unassigned.\n"
                "Return JSON only with shape: "
                "{\"decisions\":[{\"task_gid\":\"...\",\"resource_uuids\":[\"...\"],\"confidence\":\"high|medium|low\",\"reason\":\"...\",\"unsure\":true|false}]}\n"
                "Rules:\n"
                "- resource_uuids must come from the provided resources list.\n"
                "- Prefer zero assignments over weak guesses.\n"
                "- Keep reasons concise.\n"
                "- Do not include markdown or prose outside JSON."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "resources": normalized_resources,
                    "tasks": task_context_rows,
                },
                separators=(",", ":"),
                ensure_ascii=False,
            ),
        },
    ]

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": messages,
                "temperature": 0.1,
            },
            timeout=60,
        )
    except requests.RequestException:
        return {}
    if int(response.status_code) >= 400:
        return {}

    payload = response.json() if response.content else {}
    reply_text = _extract_chat_completion_text(payload)
    parsed = _json_object_from_text(reply_text)
    decisions = parsed.get("decisions") if isinstance(parsed, dict) else []
    if not isinstance(decisions, list):
        return {}

    candidate_gid_set = {
        str(row.get("task_gid") or "").strip()
        for row in task_context_rows
        if str(row.get("task_gid") or "").strip()
    }

    assignments: dict[str, list[str]] = {}
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        task_gid = str(decision.get("task_gid") or "").strip()
        if not task_gid or task_gid not in candidate_gid_set:
            continue
        if bool(decision.get("unsure")):
            continue
        confidence = str(decision.get("confidence") or "").strip().lower()
        if confidence in {"low", "unsure", "unknown"}:
            continue

        raw_uuids = decision.get("resource_uuids")
        if isinstance(raw_uuids, str):
            resolved_raw = [part.strip() for part in raw_uuids.split(",") if part.strip()]
        elif isinstance(raw_uuids, list):
            resolved_raw = [str(item or "").strip() for item in raw_uuids if str(item or "").strip()]
        else:
            single_uuid = str(decision.get("resource_uuid") or "").strip()
            resolved_raw = [single_uuid] if single_uuid else []

        normalized_uuids: list[str] = []
        seen_uuids: set[str] = set()
        for raw_uuid in resolved_raw:
            normalized_uuid = str(raw_uuid or "").strip().lower()
            if not normalized_uuid or normalized_uuid in seen_uuids:
                continue
            if normalized_uuid not in allowed_resource_set:
                continue
            seen_uuids.add(normalized_uuid)
            normalized_uuids.append(normalized_uuid)

        if not normalized_uuids:
            continue
        assignments[task_gid] = normalized_uuids
    return assignments


def _asana_agenda_item_payload_from_task_row(task_row: dict[str, object]) -> dict[str, object] | None:
    if not isinstance(task_row, dict):
        return None
    task_gid = str(task_row.get("gid") or "").strip()
    if not task_gid:
        return None
    due_date = str(task_row.get("due_date") or "").strip()
    due_time = str(task_row.get("due_time") or "").strip()
    due_at = ""
    if due_date and due_time:
        due_at = f"{due_date}T{due_time}:00Z"
    elif due_date:
        due_at = f"{due_date}T00:00:00Z"
    section_name = str(task_row.get("section_name") or "").strip()
    workspace_name = str(task_row.get("workspace_name") or "").strip()
    project_names = ", ".join(
        str(project.get("name") or "").strip()
        for project in (task_row.get("project_links") or [])
        if isinstance(project, dict) and str(project.get("name") or "").strip()
    )
    meta_parts = [part for part in [section_name, workspace_name, project_names] if part]
    return {
        "item_id": f"asana-agenda-{task_gid}",
        "source": "asana",
        "source_item_id": task_gid,
        "title": str(task_row.get("name") or "").strip(),
        "date": due_date,
        "time": due_time,
        "due_at": due_at,
        "url": str(task_row.get("task_url") or "").strip(),
        "meta": " | ".join(meta_parts),
        "done": bool(task_row.get("completed")),
    }


def _asana_task_resource_uuids(
    *,
    task_row: dict[str, object],
    board_resource_mappings: dict[str, list[str]],
    task_resource_mappings: dict[str, list[str]],
) -> list[str]:
    resolved_task_gid = str(task_row.get("gid") or "").strip()
    mapped: list[str] = []
    seen: set[str] = set()

    if resolved_task_gid:
        for mapped_uuid in task_resource_mappings.get(resolved_task_gid, []):
            normalized_uuid = str(mapped_uuid or "").strip().lower()
            if not normalized_uuid or normalized_uuid in seen:
                continue
            seen.add(normalized_uuid)
            mapped.append(normalized_uuid)

    project_links = task_row.get("project_links")
    if isinstance(project_links, list):
        for project in project_links:
            if not isinstance(project, dict):
                continue
            board_gid = str(project.get("gid") or "").strip()
            if not board_gid:
                continue
            for mapped_uuid in board_resource_mappings.get(board_gid, []):
                normalized_uuid = str(mapped_uuid or "").strip().lower()
                if not normalized_uuid or normalized_uuid in seen:
                    continue
                seen.add(normalized_uuid)
                mapped.append(normalized_uuid)

    return mapped


def _asana_enriched_tasks_with_resource_mappings(
    *,
    task_rows: list[dict[str, object]],
    board_resource_mappings: dict[str, list[str]],
    task_resource_mappings: dict[str, list[str]],
    resource_name_lookup: dict[str, str],
) -> list[dict[str, object]]:
    enriched_rows: list[dict[str, object]] = []
    for row in task_rows:
        if not isinstance(row, dict):
            continue
        mapped_resource_uuids = _asana_task_resource_uuids(
            task_row=row,
            board_resource_mappings=board_resource_mappings,
            task_resource_mappings=task_resource_mappings,
        )
        mapped_resource_names = [
            str(resource_name_lookup.get(resource_uuid) or "").strip()
            for resource_uuid in mapped_resource_uuids
            if str(resource_name_lookup.get(resource_uuid) or "").strip()
        ]
        next_row = dict(row)
        next_row["resource_uuids"] = mapped_resource_uuids
        next_row["resource_names"] = mapped_resource_names
        enriched_rows.append(next_row)
    return enriched_rows


def _asana_resource_options_for_user(user) -> list[dict[str, str]]:
    options = _wiki_resource_options_for_user(user)
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for option in options:
        if not isinstance(option, dict):
            continue
        resource_uuid = str(option.get("resource_uuid") or "").strip().lower()
        resource_name = str(option.get("resource_name") or "").strip()
        if not resource_uuid or not resource_name or resource_uuid in seen:
            continue
        seen.add(resource_uuid)
        normalized.append(
            {
                "resource_uuid": resource_uuid,
                "resource_name": resource_name,
            }
        )
    normalized.sort(key=lambda item: (item["resource_name"].lower(), item["resource_uuid"]))
    return normalized


def _asana_due_sort_timestamp(task: dict[str, object]) -> float:
    due_at = str(task.get("due_at") or "").strip()
    if due_at:
        parsed_due_at = _parse_runtime_timestamp(due_at)
        if parsed_due_at is not None:
            return float(parsed_due_at.timestamp())
    due_on = str(task.get("due_on") or "").strip()
    if due_on:
        try:
            parsed_due_on = datetime.strptime(due_on, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return float(parsed_due_on.timestamp())
        except Exception:
            pass
    return float("inf")


def _asana_due_display(task: dict[str, object]) -> str:
    due_at = str(task.get("due_at") or "").strip()
    if due_at:
        return _format_display_time(due_at)
    due_on = str(task.get("due_on") or "").strip()
    if due_on:
        try:
            parsed_due_on = datetime.strptime(due_on, "%Y-%m-%d")
            return parsed_due_on.strftime("%b %d, %Y")
        except Exception:
            return due_on
    return "—"


def _asana_due_components(task: dict[str, object]) -> tuple[str, str]:
    due_on = str(task.get("due_on") or "").strip()
    if due_on:
        return due_on, ""
    due_at = str(task.get("due_at") or "").strip()
    if not due_at:
        return "", ""
    parsed = _parse_runtime_timestamp(due_at)
    if parsed is None:
        return "", ""
    as_utc = parsed.astimezone(timezone.utc)
    return as_utc.strftime("%Y-%m-%d"), as_utc.strftime("%H:%M")


def _asana_task_url(task_gid: str) -> str:
    resolved_gid = str(task_gid or "").strip()
    if not resolved_gid:
        return ""
    return f"https://app.asana.com/0/0/{resolved_gid}"


def _asana_project_url(project_gid: str) -> str:
    resolved_gid = str(project_gid or "").strip()
    if not resolved_gid:
        return ""
    return f"https://app.asana.com/0/{resolved_gid}/list"


def _asana_task_row_from_api_task(
    task: dict[str, object],
    *,
    workspace_name_by_gid: dict[str, str] | None = None,
    default_workspace_name: str = "",
) -> dict[str, object] | None:
    task_gid = str(task.get("gid") or "").strip()
    if not task_gid:
        return None

    task_name = str(task.get("name") or "").strip() or f"Asana task {task_gid}"
    memberships = task.get("memberships")
    project_links: list[dict[str, str]] = []
    seen_project_gids: set[str] = set()
    section_name = ""
    if isinstance(memberships, list):
        for membership in memberships:
            if not isinstance(membership, dict):
                continue
            if not section_name:
                section_obj = membership.get("section")
                if isinstance(section_obj, dict):
                    section_name = str(section_obj.get("name") or "").strip()
            project_obj = membership.get("project")
            if not isinstance(project_obj, dict):
                continue
            project_gid = str(project_obj.get("gid") or "").strip()
            project_name = str(project_obj.get("name") or "").strip()
            if not project_gid or not project_name or project_gid in seen_project_gids:
                continue
            seen_project_gids.add(project_gid)
            project_url = str(project_obj.get("permalink_url") or "").strip() or _asana_project_url(project_gid)
            project_links.append(
                {
                    "gid": project_gid,
                    "name": project_name,
                    "url": project_url,
                }
            )

    workspace_gid = str(task.get("_workspace_gid") or "").strip()
    workspace_obj = task.get("workspace")
    if not workspace_gid and isinstance(workspace_obj, dict):
        workspace_gid = str(workspace_obj.get("gid") or "").strip()
    workspace_name = str(default_workspace_name or "").strip()
    if workspace_name_by_gid and workspace_gid:
        workspace_name = str(workspace_name_by_gid.get(workspace_gid) or "").strip() or workspace_name
    if not workspace_name and isinstance(workspace_obj, dict):
        workspace_name = str(workspace_obj.get("name") or "").strip()

    task_url = str(task.get("permalink_url") or "").strip() or _asana_task_url(task_gid)
    completed = bool(task.get("completed"))
    completed_at = str(task.get("completed_at") or "").strip()
    due_date, due_time = _asana_due_components(task)
    notes = str(task.get("notes") or "").strip()
    if len(notes) > 8000:
        notes = notes[:8000]

    assignee_obj = task.get("assignee")
    assignee_gid = ""
    assignee_name = ""
    if isinstance(assignee_obj, dict):
        assignee_gid = str(assignee_obj.get("gid") or "").strip()
        assignee_name = str(assignee_obj.get("name") or "").strip()

    subtask_count = int(task.get("num_subtasks") or 0)

    return {
        "gid": task_gid,
        "name": task_name,
        "notes": notes,
        "task_url": task_url,
        "completed": completed,
        "completed_at": completed_at,
        "status_label": "Completed" if completed else "Open",
        "status_tone": "success" if completed else "info",
        "due_display": _asana_due_display(task),
        "due_date": due_date,
        "due_time": due_time,
        "project_links": project_links,
        "section_name": section_name,
        "workspace_name": workspace_name,
        "modified_at": str(task.get("modified_at") or "").strip(),
        "assignee_gid": assignee_gid,
        "assignee_name": assignee_name,
        "subtask_count": subtask_count,
        "workspace_gid": workspace_gid,
    }


def _asana_task_row_sort_key(task_row: dict[str, object]) -> tuple[int, str, str]:
    completed_rank = 1 if bool(task_row.get("completed")) else 0
    due_date = str(task_row.get("due_date") or "").strip()
    due_time = str(task_row.get("due_time") or "").strip()
    due_key = f"{due_date} {due_time or '99:99'}" if due_date else "9999-12-31 99:99"
    return (
        completed_rank,
        due_key,
        str(task_row.get("name") or "").strip().lower(),
    )


def _asana_board_row_sort_key(board_row: dict[str, object]) -> tuple[str, str, str]:
    return (
        str(board_row.get("workspace_name") or "").strip().lower(),
        str(board_row.get("name") or "").strip().lower(),
        str(board_row.get("gid") or "").strip(),
    )


def _asana_board_row_from_project(
    project: dict[str, object],
    *,
    workspace_name_by_gid: dict[str, str] | None = None,
) -> dict[str, str] | None:
    board_gid = str(project.get("gid") or "").strip()
    board_name = str(project.get("name") or "").strip()
    if not board_gid or not board_name:
        return None
    if bool(project.get("archived")):
        return None

    workspace_obj = project.get("workspace") if isinstance(project.get("workspace"), dict) else {}
    workspace_gid = str(workspace_obj.get("gid") or "").strip()
    workspace_name = str(workspace_obj.get("name") or "").strip()
    if workspace_name_by_gid and workspace_gid:
        workspace_name = str(workspace_name_by_gid.get(workspace_gid) or "").strip() or workspace_name

    board_url = str(project.get("permalink_url") or "").strip() or _asana_project_url(board_gid)
    return {
        "gid": board_gid,
        "name": board_name,
        "url": board_url,
        "workspace_gid": workspace_gid,
        "workspace_name": workspace_name,
    }


def _asana_board_rows_from_task_row(task_row: dict[str, object]) -> list[dict[str, str]]:
    project_links = task_row.get("project_links")
    if not isinstance(project_links, list):
        return []
    workspace_name = str(task_row.get("workspace_name") or "").strip()
    board_rows: list[dict[str, str]] = []
    seen_board_gids: set[str] = set()
    for project in project_links:
        if not isinstance(project, dict):
            continue
        board_gid = str(project.get("gid") or "").strip()
        board_name = str(project.get("name") or "").strip()
        if not board_gid or not board_name or board_gid in seen_board_gids:
            continue
        seen_board_gids.add(board_gid)
        board_rows.append(
            {
                "gid": board_gid,
                "name": board_name,
                "url": str(project.get("url") or "").strip() or _asana_project_url(board_gid),
                "workspace_gid": "",
                "workspace_name": workspace_name,
            }
        )
    board_rows.sort(key=_asana_board_row_sort_key)
    return board_rows


def _asana_merge_board_rows(*board_row_lists: list[dict[str, object]]) -> list[dict[str, str]]:
    merged_by_gid: dict[str, dict[str, str]] = {}
    for board_rows in board_row_lists:
        if not isinstance(board_rows, list):
            continue
        for raw_row in board_rows:
            if not isinstance(raw_row, dict):
                continue
            board_gid = str(raw_row.get("gid") or "").strip()
            board_name = str(raw_row.get("name") or "").strip()
            if not board_gid or not board_name:
                continue
            normalized_row = {
                "gid": board_gid,
                "name": board_name,
                "url": str(raw_row.get("url") or "").strip() or _asana_project_url(board_gid),
                "workspace_gid": str(raw_row.get("workspace_gid") or "").strip(),
                "workspace_name": str(raw_row.get("workspace_name") or "").strip(),
            }
            existing = merged_by_gid.get(board_gid)
            if existing is None:
                merged_by_gid[board_gid] = normalized_row
                continue
            if not existing.get("workspace_gid") and normalized_row.get("workspace_gid"):
                existing["workspace_gid"] = normalized_row["workspace_gid"]
            if not existing.get("workspace_name") and normalized_row.get("workspace_name"):
                existing["workspace_name"] = normalized_row["workspace_name"]
            if not existing.get("url") and normalized_row.get("url"):
                existing["url"] = normalized_row["url"]
    merged_rows = list(merged_by_gid.values())
    merged_rows.sort(key=_asana_board_row_sort_key)
    return merged_rows


def _asana_overview_payload_from_api(
    access_token: str,
    *,
    task_fetch_limit: int | None = None,
) -> tuple[dict[str, object] | None, str | None]:
    me_payload, me_error = _asana_api_get_json(
        access_token=access_token,
        path="/users/me",
        params={"opt_fields": "gid,name,email"},
    )
    if me_error:
        return None, me_error
    me_data = me_payload.get("data") if isinstance(me_payload, dict) else {}
    if not isinstance(me_data, dict):
        me_data = {}

    workspaces, _workspaces_truncated, workspace_error = _asana_api_list(
        access_token=access_token,
        path="/users/me/workspaces",
        params={
            "opt_fields": "gid,name",
            "limit": _ASANA_OVERVIEW_PER_REQUEST_LIMIT,
        },
        max_items=250,
    )
    if workspace_error:
        return None, workspace_error

    workspace_name_by_gid: dict[str, str] = {}
    workspace_gids: list[str] = []
    for workspace in workspaces:
        workspace_gid = str(workspace.get("gid") or "").strip()
        if not workspace_gid or workspace_gid in workspace_name_by_gid:
            continue
        workspace_name_by_gid[workspace_gid] = str(workspace.get("name") or "").strip()
        workspace_gids.append(workspace_gid)

    # Fetch all workspace projects (not just user-member ones) so tasks assigned
    # to any team member are visible.  /projects?workspace= returns every project
    # in the workspace the token owner can access; /users/me/projects would only
    # return projects where the user is an explicit member.
    projects: list[dict[str, object]] = []
    projects_truncated = False
    for workspace_gid in workspace_gids:
        ws_projects, ws_projects_truncated, ws_project_error = _asana_api_list(
            access_token=access_token,
            path="/projects",
            params={
                "workspace": workspace_gid,
                "opt_fields": "gid,name,permalink_url,archived,workspace.gid,workspace.name",
                "limit": _ASANA_OVERVIEW_PER_REQUEST_LIMIT,
            },
            max_items=1000,
        )
        if ws_project_error:
            continue
        if ws_projects_truncated:
            projects_truncated = True
        projects.extend(ws_projects)
    project_board_rows = [
        board_row
        for board_row in (
            _asana_board_row_from_project(
                project,
                workspace_name_by_gid=workspace_name_by_gid,
            )
            for project in projects
            if isinstance(project, dict)
        )
        if board_row is not None
    ]

    all_tasks_by_gid: dict[str, dict[str, object]] = {}
    truncated = bool(projects_truncated)
    resolved_task_fetch_limit = _ASANA_OVERVIEW_TASK_FETCH_LIMIT
    try:
        if task_fetch_limit is not None:
            resolved_task_fetch_limit = max(1, int(task_fetch_limit))
    except Exception:
        resolved_task_fetch_limit = _ASANA_OVERVIEW_TASK_FETCH_LIMIT
    fetched_count = 0
    for workspace_gid in workspace_gids:
        remaining = resolved_task_fetch_limit - fetched_count
        if remaining <= 0:
            truncated = True
            break
        tasks, workspace_truncated, task_error = _asana_api_list(
            access_token=access_token,
            path="/tasks",
            params={
                "assignee": "me",
                "workspace": workspace_gid,
                "completed_since": "1970-01-01T00:00:00.000Z",
                "limit": _ASANA_OVERVIEW_PER_REQUEST_LIMIT,
                "opt_fields": _ASANA_TASK_OPT_FIELDS,
            },
            max_items=remaining,
        )
        if task_error:
            # Some workspace types (e.g. organizations) may reject this query;
            # skip and rely on the project-level task fetch below.
            continue
        if workspace_truncated:
            truncated = True
        fetched_count += len(tasks)
        for task in tasks:
            task_gid = str(task.get("gid") or "").strip()
            if not task_gid:
                continue
            normalized_task = dict(task)
            normalized_task["_workspace_gid"] = workspace_gid
            all_tasks_by_gid[task_gid] = normalized_task

    # Also fetch tasks from each project so all project tasks appear,
    # not just those assigned to the current user.
    project_completed_since = (
        datetime.now(timezone.utc) - timedelta(days=_ASANA_AGENDA_COMPLETED_WINDOW_DAYS)
    ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    for project in projects:
        if fetched_count >= resolved_task_fetch_limit:
            truncated = True
            break
        project_gid = str(project.get("gid") or "").strip()
        if not project_gid:
            continue
        project_workspace = project.get("workspace") if isinstance(project.get("workspace"), dict) else {}
        project_workspace_gid = str(project_workspace.get("gid") or "").strip()
        remaining = resolved_task_fetch_limit - fetched_count
        project_tasks, project_truncated, _project_task_error = _asana_api_list(
            access_token=access_token,
            path=f"/projects/{project_gid}/tasks",
            params={
                "completed_since": project_completed_since,
                "limit": _ASANA_OVERVIEW_PER_REQUEST_LIMIT,
                "opt_fields": _ASANA_TASK_OPT_FIELDS,
            },
            max_items=min(remaining, 500),
        )
        if _project_task_error:
            continue
        if project_truncated:
            truncated = True
        for task in project_tasks:
            task_gid = str(task.get("gid") or "").strip()
            if not task_gid or task_gid in all_tasks_by_gid:
                continue
            normalized_task = dict(task)
            normalized_task["_workspace_gid"] = project_workspace_gid
            all_tasks_by_gid[task_gid] = normalized_task
            fetched_count += 1

    task_rows: list[dict[str, object]] = []
    for task in all_tasks_by_gid.values():
        task_row = _asana_task_row_from_api_task(
            task,
            workspace_name_by_gid=workspace_name_by_gid,
        )
        if task_row is None:
            continue
        task_rows.append(task_row)

    task_rows.sort(key=_asana_task_row_sort_key)
    task_board_rows: list[dict[str, str]] = []
    for task_row in task_rows:
        task_board_rows.extend(_asana_board_rows_from_task_row(task_row))
    board_rows = _asana_merge_board_rows(project_board_rows, task_board_rows)

    fetched_at_iso = datetime.now(timezone.utc).isoformat()
    return (
        {
            "fetched_at": fetched_at_iso,
            "task_count": len(task_rows),
            "workspace_count": len(workspace_name_by_gid),
            "boards": board_rows,
            "board_count": len(board_rows),
            "user_name": str(me_data.get("name") or "").strip(),
            "tasks": task_rows,
            "truncated": bool(truncated),
            "includes_project_tasks": True,
        },
        None,
    )


def _asana_access_token_for_user(user, *, force_refresh: bool = False) -> tuple[str, str | None]:
    try:
        account = (
            SocialAccount.objects.filter(user=user, provider="asana")
            .order_by("id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        return "", None
    except Exception:
        return "", "Unable to load Asana account connection."

    if account is None:
        return "", None

    try:
        token_row = (
            SocialToken.objects.filter(account=account)
            .exclude(token__exact="")
            .order_by("-id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        token_row = None
    except Exception:
        token_row = None
    access_token = str(getattr(token_row, "token", "") or "").strip()
    if token_row is None:
        return "", "Asana is connected, but the OAuth token is missing. Reconnect Asana from Settings."

    expires_at = getattr(token_row, "expires_at", None)
    if isinstance(expires_at, datetime) and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    should_refresh = bool(force_refresh)
    if not should_refresh and not access_token:
        should_refresh = True
    if not should_refresh and isinstance(expires_at, datetime):
        should_refresh = expires_at <= (datetime.now(timezone.utc) + timedelta(seconds=60))

    if should_refresh:
        refreshed_token, refresh_error = _asana_refresh_access_token(token_row)
        if refreshed_token:
            return refreshed_token, None
        return "", refresh_error

    if access_token:
        return access_token, None
    return "", "Asana is connected, but the OAuth token is missing. Reconnect Asana from Settings."


def _asana_calendar_cache_events(task_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for row in task_rows:
        if not isinstance(row, dict):
            continue
        gid = str(row.get("gid") or "").strip()
        if not gid:
            continue
        events.append(
            {
                "event_id": gid,
                "title": str(row.get("name") or "").strip(),
                "due_date": str(row.get("due_date") or "").strip(),
                "due_time": str(row.get("due_time") or "").strip(),
                "is_completed": bool(row.get("completed")),
                "status": "completed" if bool(row.get("completed")) else "open",
                "source_url": str(row.get("task_url") or "").strip(),
                "kind": "task",
                "provider": "asana",
                "payload": row,
            }
        )
    return events


def _write_asana_calendar_cache(
    user,
    *,
    task_rows: list[dict[str, object]],
    fetched_at_epoch: int,
    status: str,
    message: str = "",
) -> None:
    try:
        replace_user_calendar_event_cache(
            user,
            provider="asana",
            events=_asana_calendar_cache_events(task_rows),
            fetched_at_epoch=fetched_at_epoch,
            status=status,
            message=message,
        )
    except Exception:
        # Calendar cache should not break the main page render path.
        return


def _asana_overview_context_for_user(
    user,
    *,
    force_refresh: bool = False,
    cache_key: str = _ASANA_OVERVIEW_CACHE_KEY,
    task_fetch_limit: int | None = None,
    run_auto_assign: bool = False,
    write_calendar_cache: bool = True,
) -> dict[str, object]:
    resolved_cache_key = str(cache_key or "").strip() or _ASANA_OVERVIEW_CACHE_KEY
    resolved_task_fetch_limit = _ASANA_OVERVIEW_TASK_FETCH_LIMIT
    try:
        if task_fetch_limit is not None:
            resolved_task_fetch_limit = max(1, int(task_fetch_limit))
    except Exception:
        resolved_task_fetch_limit = _ASANA_OVERVIEW_TASK_FETCH_LIMIT

    connect_url = f"{reverse('app_settings')}?tab=account"
    context: dict[str, object] = {
        "connected": False,
        "identity": "",
        "tasks": [],
        "task_count": 0,
        "boards": [],
        "board_count": 0,
        "workspace_count": 0,
        "synced_display": "",
        "cached": False,
        "stale": False,
        "truncated": False,
        "error": "",
        "connect_url": connect_url,
    }

    try:
        account = (
            SocialAccount.objects.filter(user=user, provider="asana")
            .order_by("id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        return context
    except Exception:
        context["error"] = "Unable to load Asana account connection."
        return context

    if account is None:
        return context

    context["connected"] = True
    context["identity"] = _asana_identity_display(account)

    access_token, token_error = _asana_access_token_for_user(user)
    if not access_token:
        if token_error:
            context["error"] = token_error
        return context

    now_epoch = int(time.time())
    cached_row = get_user_asana_task_cache(user, cache_key=resolved_cache_key) or {}
    cached_payload = cached_row.get("payload") if isinstance(cached_row, dict) else {}
    cached_payload = cached_payload if isinstance(cached_payload, dict) else {}
    cached_epoch_raw = cached_row.get("fetched_at_epoch") if isinstance(cached_row, dict) else 0
    try:
        cached_epoch = int(cached_epoch_raw or 0)
    except (TypeError, ValueError):
        cached_epoch = 0

    cached_tasks = cached_payload.get("tasks") if isinstance(cached_payload.get("tasks"), list) else []
    cache_has_due_fields = all(
        isinstance(task, dict) and ("due_date" in task) and ("due_time" in task)
        for task in cached_tasks
    ) if cached_tasks else True
    cache_has_completed_at_fields = all(
        isinstance(task, dict) and ("completed_at" in task)
        for task in cached_tasks
    ) if cached_tasks else True
    cache_has_notes_fields = all(
        isinstance(task, dict) and ("notes" in task)
        for task in cached_tasks
    ) if cached_tasks else True
    cache_has_board_fields = isinstance(cached_payload.get("boards"), list)
    cache_has_project_tasks = bool(cached_payload.get("includes_project_tasks"))
    cache_is_fresh = (
        bool(cached_payload)
        and cached_epoch > 0
        and (now_epoch - cached_epoch) <= _ASANA_OVERVIEW_CACHE_MAX_AGE_SECONDS
        and bool(cache_has_due_fields)
        and bool(cache_has_completed_at_fields)
        and bool(cache_has_notes_fields)
        and bool(cache_has_board_fields)
        and bool(cache_has_project_tasks)
    )
    if cache_is_fresh and not force_refresh:
        context["tasks"] = cached_payload.get("tasks") if isinstance(cached_payload.get("tasks"), list) else []
        context["task_count"] = int(cached_payload.get("task_count") or len(context["tasks"]))
        context["boards"] = cached_payload.get("boards") if isinstance(cached_payload.get("boards"), list) else []
        context["board_count"] = int(cached_payload.get("board_count") or len(context["boards"]))
        context["workspace_count"] = int(cached_payload.get("workspace_count") or 0)
        context["truncated"] = bool(cached_payload.get("truncated"))
        context["cached"] = True
        context["synced_display"] = _format_display_time(str(cached_payload.get("fetched_at") or ""))
        if write_calendar_cache:
            _write_asana_calendar_cache(
                user,
                task_rows=[row for row in context["tasks"] if isinstance(row, dict)],
                fetched_at_epoch=cached_epoch,
                status="cached",
            )
        return context

    fresh_payload, fresh_error = _asana_overview_payload_from_api(
        access_token,
        task_fetch_limit=resolved_task_fetch_limit,
    )
    if fresh_payload is not None:
        fresh_tasks = fresh_payload.get("tasks") if isinstance(fresh_payload.get("tasks"), list) else []
        auto_assignments: dict[str, list[str]] = {}
        if run_auto_assign and fresh_tasks:
            try:
                board_resource_mappings = list_user_asana_board_resource_mappings(user)
                task_resource_mappings = list_user_asana_task_resource_mappings(user)
                agenda_item_resource_mappings = list_user_agenda_item_resource_mappings(user)
                resource_options = _asana_resource_options_for_user(user)
                auto_assignments = _asana_auto_assignments_with_agent(
                    user=user,
                    access_token=access_token,
                    task_rows=[row for row in fresh_tasks if isinstance(row, dict)],
                    resource_options=resource_options,
                    board_resource_mappings=board_resource_mappings,
                    task_resource_mappings=task_resource_mappings,
                    agenda_item_resource_mappings=agenda_item_resource_mappings,
                )
            except Exception:
                auto_assignments = {}

        if auto_assignments:
            task_lookup = {
                str(row.get("gid") or "").strip(): row
                for row in fresh_tasks
                if isinstance(row, dict) and str(row.get("gid") or "").strip()
            }
            affected_resource_uuids: set[str] = set()
            for task_gid, resource_uuids in auto_assignments.items():
                resolved_task_gid = str(task_gid or "").strip()
                if not resolved_task_gid:
                    continue
                saved_uuids = set_user_asana_task_resource_mapping(
                    user,
                    task_gid=resolved_task_gid,
                    resource_uuids=resource_uuids,
                )
                task_row = task_lookup.get(resolved_task_gid)
                if isinstance(task_row, dict):
                    due_date = str(task_row.get("due_date") or "").strip()
                    due_time = str(task_row.get("due_time") or "").strip()
                    due_at = ""
                    if due_date and due_time:
                        due_at = f"{due_date}T{due_time}:00Z"
                    elif due_date:
                        due_at = f"{due_date}T00:00:00Z"
                    section_name = str(task_row.get("section_name") or "").strip()
                    workspace_name = str(task_row.get("workspace_name") or "").strip()
                    project_names = ", ".join(
                        str(project.get("name") or "").strip()
                        for project in (task_row.get("project_links") or [])
                        if isinstance(project, dict) and str(project.get("name") or "").strip()
                    )
                    meta_parts = [part for part in [section_name, workspace_name, project_names] if part]
                    set_user_agenda_item_resource_mapping(
                        user,
                        item={
                            "item_id": f"asana-agenda-{resolved_task_gid}",
                            "source": "asana",
                            "source_item_id": resolved_task_gid,
                            "title": str(task_row.get("name") or "").strip(),
                            "date": due_date,
                            "time": due_time,
                            "due_at": due_at,
                            "url": str(task_row.get("task_url") or "").strip(),
                            "meta": " | ".join(meta_parts),
                            "done": bool(task_row.get("completed")),
                        },
                        resource_uuids=saved_uuids,
                    )
                for resource_uuid in saved_uuids:
                    normalized_uuid = str(resource_uuid or "").strip().lower()
                    if normalized_uuid:
                        affected_resource_uuids.add(normalized_uuid)

            for resource_uuid in sorted(affected_resource_uuids):
                try:
                    _upsert_resource_kb_after_wiki_mutation(actor=user, resource_uuid=resource_uuid)
                except Exception:
                    continue

        set_user_asana_task_cache(
            user,
            cache_key=resolved_cache_key,
            payload=fresh_payload,
            fetched_at_epoch=now_epoch,
        )
        context["tasks"] = fresh_payload.get("tasks") if isinstance(fresh_payload.get("tasks"), list) else []
        context["task_count"] = int(fresh_payload.get("task_count") or len(context["tasks"]))
        context["boards"] = fresh_payload.get("boards") if isinstance(fresh_payload.get("boards"), list) else []
        context["board_count"] = int(fresh_payload.get("board_count") or len(context["boards"]))
        context["workspace_count"] = int(fresh_payload.get("workspace_count") or 0)
        context["truncated"] = bool(fresh_payload.get("truncated"))
        context["cached"] = False
        context["synced_display"] = _format_display_time(str(fresh_payload.get("fetched_at") or ""))
        if write_calendar_cache:
            _write_asana_calendar_cache(
                user,
                task_rows=[row for row in context["tasks"] if isinstance(row, dict)],
                fetched_at_epoch=now_epoch,
                status="ok",
            )
        return context

    if cached_payload:
        context["tasks"] = cached_payload.get("tasks") if isinstance(cached_payload.get("tasks"), list) else []
        context["task_count"] = int(cached_payload.get("task_count") or len(context["tasks"]))
        context["boards"] = cached_payload.get("boards") if isinstance(cached_payload.get("boards"), list) else []
        context["board_count"] = int(cached_payload.get("board_count") or len(context["boards"]))
        context["workspace_count"] = int(cached_payload.get("workspace_count") or 0)
        context["truncated"] = bool(cached_payload.get("truncated"))
        context["cached"] = True
        context["stale"] = True
        context["synced_display"] = _format_display_time(str(cached_payload.get("fetched_at") or ""))
        if fresh_error:
            context["error"] = f"{fresh_error} Showing cached Asana tasks."
        if write_calendar_cache:
            _write_asana_calendar_cache(
                user,
                task_rows=[row for row in context["tasks"] if isinstance(row, dict)],
                fetched_at_epoch=cached_epoch if cached_epoch > 0 else now_epoch,
                status="stale",
                message=str(fresh_error or "").strip(),
            )
        return context

    if fresh_error:
        context["error"] = fresh_error
    if write_calendar_cache:
        _write_asana_calendar_cache(
            user,
            task_rows=[],
            fetched_at_epoch=now_epoch,
            status="error" if fresh_error else "ok",
            message=str(fresh_error or "").strip(),
        )
    return context


def _asana_task_kind_for_planner(task_row: dict[str, object]) -> str:
    section = str(task_row.get("section_name") or "").strip().lower()
    project_names = " ".join(
        str(project.get("name") or "").strip().lower()
        for project in (task_row.get("project_links") or [])
        if isinstance(project, dict)
    )
    combined = f"{section} {project_names}".strip()
    if not combined:
        return "follow-up"
    if any(token in combined for token in ("meeting", "sync", "standup", "planning")):
        return "meeting"
    if any(token in combined for token in ("review", "retro", "qa", "audit")):
        return "review"
    if any(token in combined for token in ("release", "deploy", "delivery", "launch", "ship")):
        return "delivery"
    return "follow-up"


def _asana_planner_items_from_context(asana_overview: dict[str, object]) -> list[dict[str, object]]:
    rows = asana_overview.get("tasks") if isinstance(asana_overview, dict) else []
    if not isinstance(rows, list):
        return []
    items: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        gid = str(row.get("gid") or "").strip()
        due_date = str(row.get("due_date") or "").strip()
        if not gid or not due_date:
            continue
        items.append(
            {
                "id": f"asana-task-{gid}",
                "title": str(row.get("name") or "").strip() or f"Asana task {gid}",
                "date": due_date,
                "time": str(row.get("due_time") or "").strip(),
                "kind": _asana_task_kind_for_planner(row),
                "done": bool(row.get("completed")),
                "completed_at": str(row.get("completed_at") or "").strip(),
                "source": "asana",
                "task_gid": gid,
                "url": str(row.get("task_url") or "").strip(),
                "resource_uuids": (
                    [str(value or "").strip().lower() for value in (row.get("resource_uuids") or []) if str(value or "").strip()]
                    if isinstance(row.get("resource_uuids"), list)
                    else []
                ),
            }
        )
    return items


def _outlook_task_kind_for_planner(event_row: dict[str, object]) -> str:
    payload = event_row.get("payload") if isinstance(event_row.get("payload"), dict) else {}
    is_teams_meeting = bool(payload.get("is_teams_meeting"))
    if is_teams_meeting:
        return "meeting"

    title = str(event_row.get("title") or "").strip().lower()
    if any(token in title for token in ("meeting", "sync", "standup", "review", "retro")):
        return "meeting"
    if any(token in title for token in ("release", "deploy", "launch", "ship")):
        return "delivery"
    return "follow-up"


def _outlook_planner_items_for_user(user, *, limit: int = 800) -> list[dict[str, object]]:
    resolved_limit = max(1, min(int(limit or 800), 2000))
    rows = list_user_calendar_event_cache(
        user,
        provider="outlook",
        limit=resolved_limit,
        include_completed=True,
    )
    if not isinstance(rows, list):
        return []

    items: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        event_id = str(row.get("event_id") or "").strip()
        due_date = str(row.get("due_date") or "").strip()
        if not event_id or not due_date:
            continue

        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if not isinstance(payload, dict):
            payload = {}

        status = str(row.get("status") or "").strip().lower()
        source_url = str(row.get("source_url") or payload.get("web_link") or "").strip()
        teams_join_url = str(
            payload.get("teams_join_url")
            or payload.get("online_meeting_url")
            or ""
        ).strip()
        is_teams_meeting = bool(payload.get("is_teams_meeting"))
        if not is_teams_meeting and teams_join_url:
            is_teams_meeting = "teams.microsoft.com" in teams_join_url.lower()
        if not teams_join_url and is_teams_meeting:
            teams_join_url = source_url
        primary_url = teams_join_url or source_url

        items.append(
            {
                "id": f"outlook-event-{event_id}",
                "title": str(row.get("title") or "").strip() or f"Outlook event {event_id}",
                "date": due_date,
                "time": str(row.get("due_time") or "").strip(),
                "kind": _outlook_task_kind_for_planner(row),
                "done": status in {"cancelled", "declined"},
                "source": "outlook",
                "url": primary_url,
                "event_id": event_id,
                "status": status,
                "event_url": source_url,
                "teams_join_url": teams_join_url,
                "is_teams_meeting": is_teams_meeting,
                "can_toggle": False,
            }
        )
    return items


def _merge_planner_external_items(*item_groups: list[dict[str, object]]) -> list[dict[str, object]]:
    merged: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    for group in item_groups:
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or "").strip()
            if not item_id or item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            merged.append(item)
    merged.sort(
        key=lambda item: (
            str(item.get("date") or "9999-12-31"),
            str(item.get("time") or "99:99"),
            str(item.get("title") or "").strip().lower(),
        )
    )
    return merged


def _team_planner_external_items_by_team(
    *,
    memberships: list[dict[str, object]],
    resource_index: dict[str, dict[str, object]],
    resource_name_lookup: dict[str, str] | None = None,
) -> dict[str, list[dict[str, object]]]:
    team_ids = [
        int(item.get("id") or 0)
        for item in memberships
        if int(item.get("id") or 0) > 0
    ]
    items_by_team: dict[str, list[dict[str, object]]] = {
        str(team_id): [] for team_id in team_ids
    }
    if not team_ids or not resource_index:
        return items_by_team

    membership_id_by_name: dict[str, int] = {}
    for item in memberships:
        team_id = int(item.get("id") or 0)
        team_name = str(item.get("name") or "").strip().lower()
        if team_id <= 0 or not team_name:
            continue
        membership_id_by_name[team_name] = team_id

    team_resource_uuids: dict[int, set[str]] = {team_id: set() for team_id in team_ids}
    for resource_uuid, payload in resource_index.items():
        resolved_uuid = str(resource_uuid or "").strip().lower()
        if not resolved_uuid:
            continue
        team_names = payload.get("team_names", set())
        if not isinstance(team_names, (list, tuple, set)):
            continue
        for raw_team_name in team_names:
            team_name = str(raw_team_name or "").strip().lower()
            if not team_name:
                continue
            team_id = membership_id_by_name.get(team_name)
            if team_id is None:
                continue
            team_resource_uuids.setdefault(team_id, set()).add(resolved_uuid)

    if not any(team_resource_uuids.get(team_id) for team_id in team_ids):
        return items_by_team

    User = get_user_model()
    membership_rows = list(
        User.groups.through.objects.filter(group_id__in=team_ids).values_list("user_id", "group_id")
    )
    if not membership_rows:
        return items_by_team

    team_ids_by_user: dict[int, set[int]] = {}
    for raw_user_id, raw_team_id in membership_rows:
        user_id = int(raw_user_id or 0)
        team_id = int(raw_team_id or 0)
        if user_id <= 0 or team_id <= 0:
            continue
        team_ids_by_user.setdefault(user_id, set()).add(team_id)

    candidate_user_ids = sorted(team_ids_by_user.keys())
    if not candidate_user_ids:
        return items_by_team

    connected_asana_user_ids = {
        int(user_id)
        for user_id in (
            SocialAccount.objects.filter(
                provider="asana",
                user__is_active=True,
                user_id__in=candidate_user_ids,
            )
            .values_list("user_id", flat=True)
            .distinct()
        )
        if int(user_id or 0) > 0
    }
    # Legacy users may have cached Asana tasks/mappings without a SocialAccount row.
    # Prefer connected users for performance, but gracefully fall back to all team members.
    planner_user_ids = sorted(connected_asana_user_ids) if connected_asana_user_ids else candidate_user_ids
    member_users = (
        User.objects.filter(is_active=True, id__in=planner_user_ids)
        .only("id", "username")
        .order_by("id")
    )

    indexed_items_by_team: dict[int, dict[str, dict[str, object]]] = {
        team_id: {} for team_id in team_ids
    }
    resolved_resource_name_lookup = {
        str(resource_uuid or "").strip().lower(): str(resource_name or "").strip()
        for resource_uuid, resource_name in (resource_name_lookup or {}).items()
        if str(resource_uuid or "").strip()
    }

    for member in member_users:
        member_id = int(getattr(member, "id", 0) or 0)
        if member_id <= 0:
            continue
        member_team_ids = team_ids_by_user.get(member_id, set())
        if not member_team_ids:
            continue

        board_resource_mappings = list_user_asana_board_resource_mappings(member)
        task_resource_mappings = list_user_asana_task_resource_mappings(member)
        if not board_resource_mappings and not task_resource_mappings:
            continue
        asana_rows = list_user_calendar_event_cache(
            member,
            provider="asana",
            limit=1200,
            include_completed=True,
        )
        if not asana_rows:
            continue
        for asana_row in asana_rows:
            if not isinstance(asana_row, dict):
                continue
            task_row = asana_row.get("payload") if isinstance(asana_row.get("payload"), dict) else {}
            if not isinstance(task_row, dict):
                task_row = {}

            task_gid = str(task_row.get("gid") or asana_row.get("event_id") or "").strip()
            due_date = str(task_row.get("due_date") or asana_row.get("due_date") or "").strip()
            if not task_gid or not due_date:
                continue

            mapped_resource_uuids = _asana_task_resource_uuids(
                task_row=task_row,
                board_resource_mappings=board_resource_mappings,
                task_resource_mappings=task_resource_mappings,
            )
            mapped_resource_set = {
                str(resource_uuid or "").strip().lower()
                for resource_uuid in mapped_resource_uuids
                if str(resource_uuid or "").strip()
            }
            if not mapped_resource_set:
                continue

            due_time = str(task_row.get("due_time") or asana_row.get("due_time") or "").strip()
            item_title = str(task_row.get("name") or asana_row.get("title") or "").strip() or f"Asana task {task_gid}"
            task_url = str(task_row.get("task_url") or asana_row.get("source_url") or "").strip()
            completed = bool(task_row.get("completed") or asana_row.get("is_completed"))
            completed_at = str(task_row.get("completed_at") or "").strip()
            updated_marker = str(task_row.get("modified_at") or asana_row.get("updated_at") or "").strip()

            for team_id in member_team_ids:
                team_uuid_set = team_resource_uuids.get(team_id, set())
                if not team_uuid_set:
                    continue
                team_mapped_resource_uuids = sorted(mapped_resource_set.intersection(team_uuid_set))
                if not team_mapped_resource_uuids:
                    continue
                team_mapped_resource_names = [
                    str(resolved_resource_name_lookup.get(resource_uuid) or "").strip()
                    for resource_uuid in team_mapped_resource_uuids
                    if str(resolved_resource_name_lookup.get(resource_uuid) or "").strip()
                ]
                project_links = task_row.get("project_links")
                resolved_project_links: list[dict[str, str]] = []
                if isinstance(project_links, list):
                    for project in project_links:
                        if not isinstance(project, dict):
                            continue
                        project_gid = str(project.get("gid") or "").strip()
                        project_name = str(project.get("name") or "").strip()
                        project_url = str(project.get("url") or "").strip()
                        if not project_gid and not project_name and not project_url:
                            continue
                        resolved_project_links.append(
                            {
                                "gid": project_gid,
                                "name": project_name,
                                "url": project_url,
                            }
                        )

                item_id = f"asana-task-{task_gid}"
                team_bucket = indexed_items_by_team.setdefault(team_id, {})
                existing = team_bucket.get(item_id)
                existing_marker = str(existing.get("_sort_updated_marker") or "") if isinstance(existing, dict) else ""
                if existing is not None and existing_marker and updated_marker and existing_marker >= updated_marker:
                    continue

                team_bucket[item_id] = {
                    "id": item_id,
                    "title": item_title,
                    "date": due_date,
                    "time": due_time,
                    "kind": _asana_task_kind_for_planner(task_row),
                    "done": completed,
                    "completed_at": completed_at,
                    "source": "asana",
                    "task_gid": task_gid,
                    "url": task_url,
                    "resource_uuids": team_mapped_resource_uuids,
                    "resource_names": team_mapped_resource_names,
                    "project_links": resolved_project_links,
                    "section_name": str(task_row.get("section_name") or "").strip(),
                    "can_toggle": False,
                    "_sort_updated_marker": updated_marker,
                }

    for team_id in team_ids:
        team_bucket = indexed_items_by_team.get(team_id, {})
        if not isinstance(team_bucket, dict):
            items_by_team[str(team_id)] = []
            continue
        rows = list(team_bucket.values())
        rows.sort(
            key=lambda item: (
                str(item.get("date") or "9999-12-31"),
                str(item.get("time") or "99:99"),
                str(item.get("title") or "").strip().lower(),
            )
        )
        cleaned_rows: list[dict[str, object]] = []
        for row in rows:
            cleaned = dict(row)
            cleaned.pop("_sort_updated_marker", None)
            cleaned_rows.append(cleaned)
        items_by_team[str(team_id)] = cleaned_rows

    return items_by_team


def _update_asana_overview_cache_completion(
    user,
    *,
    task_gid: str,
    completed: bool,
    completed_at: str = "",
) -> None:
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return
    cached_row = get_user_asana_task_cache(user, cache_key=_ASANA_OVERVIEW_CACHE_KEY) or {}
    cached_payload = cached_row.get("payload") if isinstance(cached_row, dict) else {}
    if not isinstance(cached_payload, dict):
        return
    tasks = cached_payload.get("tasks")
    if not isinstance(tasks, list):
        return
    updated = False
    for task in tasks:
        if not isinstance(task, dict):
            continue
        if str(task.get("gid") or "").strip() != resolved_task_gid:
            continue
        task["completed"] = bool(completed)
        task["completed_at"] = (
            str(completed_at or "").strip()
            if completed
            else ""
        )
        task["status_label"] = "Completed" if completed else "Open"
        task["status_tone"] = "success" if completed else "info"
        task["modified_at"] = datetime.now(timezone.utc).isoformat()
        updated = True
        break
    if not updated:
        return
    now_epoch = int(time.time())
    cached_payload["fetched_at"] = datetime.now(timezone.utc).isoformat()
    cached_payload["tasks"] = tasks
    try:
        cached_payload["task_count"] = int(cached_payload.get("task_count") or len(tasks))
    except (TypeError, ValueError):
        cached_payload["task_count"] = len(tasks)
    existing_boards = cached_payload.get("boards") if isinstance(cached_payload.get("boards"), list) else []
    if existing_boards:
        next_boards = _asana_merge_board_rows(existing_boards)
    else:
        derived_board_rows: list[dict[str, str]] = []
        for task_row in tasks:
            if isinstance(task_row, dict):
                derived_board_rows.extend(_asana_board_rows_from_task_row(task_row))
        next_boards = _asana_merge_board_rows(derived_board_rows)
    cached_payload["boards"] = next_boards
    cached_payload["board_count"] = len(next_boards)
    set_user_asana_task_cache(
        user,
        cache_key=_ASANA_OVERVIEW_CACHE_KEY,
        payload=cached_payload,
        fetched_at_epoch=now_epoch,
    )
    _write_asana_calendar_cache(
        user,
        task_rows=[row for row in tasks if isinstance(row, dict)],
        fetched_at_epoch=now_epoch,
        status="ok",
    )


def _upsert_asana_overview_cache_task(user, *, task_row: dict[str, object]) -> None:
    task_gid = str(task_row.get("gid") or "").strip()
    if not task_gid:
        return
    cached_row = get_user_asana_task_cache(user, cache_key=_ASANA_OVERVIEW_CACHE_KEY) or {}
    cached_payload = cached_row.get("payload") if isinstance(cached_row, dict) else {}
    if not isinstance(cached_payload, dict):
        return
    tasks = cached_payload.get("tasks")
    if not isinstance(tasks, list):
        return

    next_tasks: list[dict[str, object]] = []
    replaced = False
    for row in tasks:
        if not isinstance(row, dict):
            continue
        if str(row.get("gid") or "").strip() == task_gid:
            next_tasks.append(dict(task_row))
            replaced = True
            continue
        next_tasks.append(row)
    if not replaced:
        next_tasks.append(dict(task_row))

    next_tasks.sort(key=_asana_task_row_sort_key)
    workspace_names = {
        str(row.get("workspace_name") or "").strip()
        for row in next_tasks
        if isinstance(row, dict) and str(row.get("workspace_name") or "").strip()
    }
    existing_boards = cached_payload.get("boards") if isinstance(cached_payload.get("boards"), list) else []
    next_boards = _asana_merge_board_rows(
        existing_boards,
        _asana_board_rows_from_task_row(task_row),
    )
    if not next_boards:
        derived_board_rows: list[dict[str, str]] = []
        for row in next_tasks:
            if isinstance(row, dict):
                derived_board_rows.extend(_asana_board_rows_from_task_row(row))
        next_boards = _asana_merge_board_rows(derived_board_rows)
    now_epoch = int(time.time())
    now_iso = datetime.now(timezone.utc).isoformat()
    cached_payload["fetched_at"] = now_iso
    cached_payload["tasks"] = next_tasks
    cached_payload["task_count"] = len(next_tasks)
    cached_payload["boards"] = next_boards
    cached_payload["board_count"] = len(next_boards)
    cached_payload["workspace_count"] = len(workspace_names)

    set_user_asana_task_cache(
        user,
        cache_key=_ASANA_OVERVIEW_CACHE_KEY,
        payload=cached_payload,
        fetched_at_epoch=now_epoch,
    )
    _write_asana_calendar_cache(
        user,
        task_rows=[row for row in next_tasks if isinstance(row, dict)],
        fetched_at_epoch=now_epoch,
        status="ok",
    )


def _remove_asana_overview_cache_task(user, *, task_gid: str) -> None:
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return
    cached_row = get_user_asana_task_cache(user, cache_key=_ASANA_OVERVIEW_CACHE_KEY) or {}
    cached_payload = cached_row.get("payload") if isinstance(cached_row, dict) else {}
    if not isinstance(cached_payload, dict):
        return
    tasks = cached_payload.get("tasks")
    if not isinstance(tasks, list):
        return

    removed = False
    next_tasks: list[dict[str, object]] = []
    for row in tasks:
        if not isinstance(row, dict):
            continue
        if str(row.get("gid") or "").strip() == resolved_task_gid:
            removed = True
            continue
        next_tasks.append(row)
    if not removed:
        return

    workspace_names = {
        str(row.get("workspace_name") or "").strip()
        for row in next_tasks
        if isinstance(row, dict) and str(row.get("workspace_name") or "").strip()
    }
    existing_boards = cached_payload.get("boards") if isinstance(cached_payload.get("boards"), list) else []
    if existing_boards:
        next_boards = _asana_merge_board_rows(existing_boards)
    else:
        derived_board_rows: list[dict[str, str]] = []
        for row in next_tasks:
            if isinstance(row, dict):
                derived_board_rows.extend(_asana_board_rows_from_task_row(row))
        next_boards = _asana_merge_board_rows(derived_board_rows)
    now_epoch = int(time.time())
    now_iso = datetime.now(timezone.utc).isoformat()
    cached_payload["fetched_at"] = now_iso
    cached_payload["tasks"] = next_tasks
    cached_payload["task_count"] = len(next_tasks)
    cached_payload["boards"] = next_boards
    cached_payload["board_count"] = len(next_boards)
    cached_payload["workspace_count"] = len(workspace_names)

    set_user_asana_task_cache(
        user,
        cache_key=_ASANA_OVERVIEW_CACHE_KEY,
        payload=cached_payload,
        fetched_at_epoch=now_epoch,
    )
    _write_asana_calendar_cache(
        user,
        task_rows=[row for row in next_tasks if isinstance(row, dict)],
        fetched_at_epoch=now_epoch,
        status="ok",
    )


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
        "wiki": "resource_wiki",
        "wiki_editor_new": "resource_wiki_editor_new",
        "wiki_editor": "resource_wiki_editor",
        "wiki_create_page": "resource_wiki_create_page",
        "wiki_update_page": "resource_wiki_update_page",
        "wiki_delete_page": "resource_wiki_delete_page",
        "check": "check_resource_health_detail",
        "ping_stream": "resource_ping_stream",
        "notes_add": "resource_note_add",
        "notes_attachment": "resource_note_attachment",
        "alerts_update": "update_resource_alert_settings",
        "api_create": "create_resource_api_key_item",
        "api_revoke": "revoke_resource_api_key_item",
        "logs_ingest": "resource_logs_ingest",
    }
    team_names = {
        "detail": "team_resource_detail",
        "wiki": "team_resource_wiki",
        "wiki_editor_new": "team_resource_wiki_editor_new",
        "wiki_editor": "team_resource_wiki_editor",
        "wiki_create_page": "team_resource_wiki_create_page",
        "wiki_update_page": "team_resource_wiki_update_page",
        "wiki_delete_page": "team_resource_wiki_delete_page",
        "check": "team_check_resource_health_detail",
        "ping_stream": "team_resource_ping_stream",
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


def _active_resource_route_values(
    *,
    current_alias: ResourceRouteAlias | None,
    route_kind: str,
    route_value: str,
) -> tuple[str, str]:
    active_route_kind = str(current_alias.route_kind if current_alias else route_kind or "user").strip().lower() or "user"
    active_route_value = str(current_alias.route_value if current_alias else route_value or "").strip()
    return active_route_kind, active_route_value


def _resource_wiki_route_url(
    *,
    route_kind: str,
    route_value: str,
    resource_uuid: str,
    endpoint_key: str,
    status: str = "",
    page_path: str = "",
    **kwargs,
) -> str:
    base_url = _resource_route_reverse(
        route_kind=route_kind,
        route_value=route_value,
        endpoint_key=endpoint_key,
        resource_uuid=resource_uuid,
        **kwargs,
    )
    query: dict[str, str] = {}
    if status:
        query["status"] = status
    if page_path:
        query["page"] = page_path
    if query:
        return f"{base_url}?{urlencode(query)}"
    return base_url


def _redirect_resource_wiki(
    *,
    route_kind: str,
    route_value: str,
    resource_uuid: str,
    status: str = "",
    page_path: str = "",
):
    return redirect(
        _resource_wiki_route_url(
            route_kind=route_kind,
            route_value=route_value,
            resource_uuid=resource_uuid,
            endpoint_key="wiki",
            status=status,
            page_path=page_path,
        )
    )


def _redirect_resource_wiki_editor_new(
    *,
    route_kind: str,
    route_value: str,
    resource_uuid: str,
    status: str = "",
):
    return redirect(
        _resource_wiki_route_url(
            route_kind=route_kind,
            route_value=route_value,
            resource_uuid=resource_uuid,
            endpoint_key="wiki_editor_new",
            status=status,
        )
    )


def _redirect_resource_wiki_editor(
    *,
    route_kind: str,
    route_value: str,
    resource_uuid: str,
    page_id: int,
    status: str = "",
):
    return redirect(
        _resource_wiki_route_url(
            route_kind=route_kind,
            route_value=route_value,
            resource_uuid=resource_uuid,
            endpoint_key="wiki_editor",
            page_id=int(page_id),
            status=status,
        )
    )


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


def _resource_wiki_url_for_uuid(*, actor, resource_uuid: str, page_path: str = "") -> str:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return ""

    current_alias = (
        ResourceRouteAlias.objects.filter(resource_uuid=resolved_uuid, is_current=True)
        .only("route_kind", "route_value")
        .first()
    )
    url = ""
    if current_alias:
        try:
            url = _resource_route_reverse(
                route_kind=current_alias.route_kind,
                route_value=current_alias.route_value,
                endpoint_key="wiki",
                resource_uuid=resolved_uuid,
            )
        except NoReverseMatch:
            url = ""
    if not url:
        try:
            url = reverse(
                "resource_wiki",
                kwargs={"username": actor.get_username(), "resource_uuid": resolved_uuid},
            )
        except NoReverseMatch:
            return ""

    cleaned_page_path = _normalize_kb_result_text(page_path)
    if cleaned_page_path:
        return f"{url}?{urlencode({'page': cleaned_page_path})}"
    return url


def _can_access_owner_resource(*, actor, owner, resource_uuid: str) -> bool:
    if not actor.is_authenticated:
        return False
    if actor.is_superuser:
        return True
    if owner is not None and actor.id == owner.id:
        return True
    return user_can_access_resource(user=actor, resource_uuid=str(resource_uuid or "").strip())


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
        "asana_redirect_uri": f"{http_base_url}/accounts/asana/login/callback/",
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
        "web_voice_token_uri": f"{http_base_url}/chat/voice-token/",
        "web_voice_log_uri": f"{http_base_url}/chat/voice-log/",
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
        "microsoft_mailbox_email": "",
        "github_client_id": "",
        "github_client_secret": "",
        "asana_client_id": "",
        "asana_client_secret": "",
        "twilio_account_sid": "",
        "twilio_auth_token": "",
        "twilio_from_number": "",
        "admin_username": "",
    }

    setup = get_setup_state()
    if setup is not None:
        initial["openai_api_key"] = str(getattr(setup, "openai_api_key", "") or "").strip()
        initial["microsoft_mailbox_email"] = str(getattr(setup, "microsoft_mailbox_email", "") or "").strip()
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

    asana_app, _site = _social_app_for_provider("asana")
    if asana_app is not None:
        initial["asana_client_id"] = str(getattr(asana_app, "client_id", "") or "").strip()
        initial["asana_client_secret"] = str(getattr(asana_app, "secret", "") or "").strip()

    return initial


def setup_welcome(request):
    if is_setup_complete():
        if request.user.is_authenticated:
            return redirect("home")
        return redirect(settings.LOGIN_URL)

    connector_runtime = _connector_runtime_context(request)
    known_connectors = {"openai", "microsoft", "github", "asana", "twilio"}
    connector_labels = {
        "openai": "OpenAI",
        "microsoft": "Microsoft",
        "github": "GitHub",
        "asana": "Asana",
        "twilio": "Twilio",
    }
    action_connector_map = {
        "save_openai": "openai",
        "save_microsoft": "microsoft",
        "save_github": "github",
        "save_asana": "asana",
        "save_twilio": "twilio",
        "test_microsoft": "microsoft",
        "test_github": "github",
        "test_asana": "asana",
    }

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
                    "microsoft_mailbox_email": "",
                    "github_client_id": "",
                    "github_client_secret": "",
                    "asana_client_id": "",
                    "asana_client_secret": "",
                    "twilio_account_sid": "",
                    "twilio_auth_token": "",
                    "twilio_from_number": "",
                    "admin_username": "",
                },
                "active_connector": "",
                "initial_step": "1",
                **connector_runtime,
            },
        )

    errors: list[str] = []
    initial_step = str(request.GET.get("step") or "").strip()
    if initial_step not in {"1", "2"}:
        initial_step = "1"
    active_connector = str(request.GET.get("connector") or "").strip().lower()
    if active_connector not in known_connectors:
        active_connector = ""
    initial = _connector_initial_values()

    if request.method == "POST":
        setup_action = (request.POST.get("setup_action") or "").strip().lower() or "complete"
        posted_step = (request.POST.get("setup_step") or "").strip()
        posted_connector = str(request.POST.get("setup_connector") or "").strip().lower()
        if posted_step in {"1", "2"}:
            initial_step = posted_step
        initial["openai_api_key"] = (request.POST.get("openai_api_key") or "").strip()
        initial["microsoft_tenant_id"] = (request.POST.get("microsoft_tenant_id") or "").strip()
        initial["microsoft_client_id"] = (request.POST.get("microsoft_client_id") or "").strip()
        initial["microsoft_client_secret"] = (request.POST.get("microsoft_client_secret") or "").strip()
        initial["microsoft_mailbox_email"] = (request.POST.get("microsoft_mailbox_email") or "").strip().lower()
        initial["github_client_id"] = (request.POST.get("github_client_id") or "").strip()
        initial["github_client_secret"] = (request.POST.get("github_client_secret") or "").strip()
        initial["asana_client_id"] = (request.POST.get("asana_client_id") or "").strip()
        initial["asana_client_secret"] = (request.POST.get("asana_client_secret") or "").strip()
        initial["twilio_account_sid"] = (request.POST.get("twilio_account_sid") or "").strip()
        initial["twilio_auth_token"] = (request.POST.get("twilio_auth_token") or "").strip()
        initial["twilio_from_number"] = (request.POST.get("twilio_from_number") or "").strip()
        initial["admin_username"] = (request.POST.get("admin_username") or "").strip()
        admin_password = (request.POST.get("admin_password") or "").strip()
        admin_password_confirm = (request.POST.get("admin_password_confirm") or "").strip()
        targeted_connector = action_connector_map.get(setup_action)
        if targeted_connector:
            active_connector = targeted_connector
            initial_step = "2"
        elif posted_connector in known_connectors:
            active_connector = posted_connector

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
        has_any_asana_values = any([initial["asana_client_id"], initial["asana_client_secret"]])
        has_full_asana_values = all([initial["asana_client_id"], initial["asana_client_secret"]])
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

        if setup_action == "complete":
            selected_connectors = set(known_connectors)
        elif targeted_connector:
            selected_connectors = {targeted_connector}
        else:
            selected_connectors = set(known_connectors)

        if setup_action == "complete" and not users_exist:
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
        if "microsoft" in selected_connectors and has_any_microsoft_values and not has_full_microsoft_values:
            errors.append(
                "To configure Microsoft Entra, provide Tenant ID, Client ID, and Client Secret Value."
            )
        if initial["microsoft_mailbox_email"] and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", initial["microsoft_mailbox_email"]):
            errors.append("Microsoft Email Agent mailbox must be a valid email address.")
        if "github" in selected_connectors and has_any_github_values and not has_full_github_values:
            errors.append("To configure GitHub OAuth, provide Client ID and Client Secret.")
        if "asana" in selected_connectors and has_any_asana_values and not has_full_asana_values:
            errors.append("To configure Asana OAuth, provide Client ID and Client Secret.")
        if "twilio" in selected_connectors and has_any_twilio_values and not has_full_twilio_values:
            errors.append("To configure Twilio alerts, provide Account SID, Auth Token, and a From number.")
        if setup_action == "test_microsoft" and not has_full_microsoft_values:
            errors.append("Provide Microsoft Entra Tenant ID, Client ID, and Client Secret before testing sign-in.")
        if setup_action == "test_github" and not has_full_github_values:
            errors.append("Provide GitHub OAuth Client ID and Client Secret before testing sign-in.")
        if setup_action == "test_asana" and not has_full_asana_values:
            errors.append("Provide Asana OAuth Client ID and Client Secret before testing sign-in.")

        if not errors:
            setup = get_or_create_setup_state()
            if setup is None:
                errors.append("Setup database is not ready yet. Run migrations and try again.")
            else:
                setup_update_fields: list[str] = []
                if "openai" in selected_connectors:
                    setup.openai_api_key = initial["openai_api_key"]
                    setup_update_fields.append("openai_api_key")
                if "microsoft" in selected_connectors:
                    setup.microsoft_mailbox_email = initial["microsoft_mailbox_email"]
                    setup_update_fields.append("microsoft_mailbox_email")
                if "twilio" in selected_connectors:
                    setup.twilio_account_sid = initial["twilio_account_sid"]
                    setup.twilio_auth_token = initial["twilio_auth_token"]
                    setup.twilio_from_number = initial["twilio_from_number"]
                    setup_update_fields.extend(
                        [
                            "twilio_account_sid",
                            "twilio_auth_token",
                            "twilio_from_number",
                        ]
                    )
                if setup_action == "complete":
                    setup.is_completed = True
                    setup_update_fields.append("is_completed")
                if setup_update_fields:
                    setup.save(update_fields=[*setup_update_fields, "updated_at"])

                if "microsoft" in selected_connectors and has_full_microsoft_values:
                    try:
                        microsoft_app, site = _social_app_for_provider("microsoft")
                        if microsoft_app is None:
                            microsoft_app = SocialApp(provider="microsoft", name="Microsoft Entra")
                        microsoft_app.client_id = initial["microsoft_client_id"]
                        microsoft_app.secret = initial["microsoft_client_secret"]
                        app_settings = dict(microsoft_app.settings or {})
                        app_settings["tenant"] = initial["microsoft_tenant_id"]
                        app_settings["scope"] = list(_MICROSOFT_CONNECTOR_SCOPES)
                        microsoft_app.settings = app_settings
                        microsoft_app.save()
                        if site is not None:
                            microsoft_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save Microsoft Entra social app settings.")
                if "github" in selected_connectors and has_full_github_values:
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
                if "asana" in selected_connectors and has_full_asana_values:
                    try:
                        asana_app, site = _social_app_for_provider("asana")
                        if asana_app is None:
                            asana_app = SocialApp(provider="asana", name="Asana OAuth")
                        asana_app.client_id = initial["asana_client_id"]
                        asana_app.secret = initial["asana_client_secret"]
                        asana_app.save()
                        if site is not None:
                            asana_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save Asana social app settings.")

            if not errors and setup_action == "complete" and not users_exist:
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
                if setup_action == "test_asana" and has_full_asana_values:
                    try:
                        asana_login_url = reverse("asana_login")
                    except NoReverseMatch:
                        asana_login_url = "/accounts/asana/login/"
                    messages.success(request, "Setup saved. Continue with Asana sign-in to test connection.")
                    return redirect(f"{asana_login_url}?process=connect")
                if targeted_connector:
                    messages.success(
                        request,
                        f"{connector_labels.get(targeted_connector, 'Connector')} settings saved.",
                    )
                    return redirect(f"{reverse('setup_welcome')}?step=2&connector={targeted_connector}")
                if setup_action == "complete":
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
            "active_connector": active_connector,
            **connector_runtime,
        },
    )


@login_required
def home(request):
    _ensure_default_sdk_workspace_wiki_page(actor=request.user)
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
    twilio_sms_available = is_twilio_configured()
    email_notifications_available = is_support_inbox_email_alerts_enabled()
    calendar_notification_settings = get_user_calendar_notification_settings(request.user)
    if not twilio_sms_available:
        calendar_notification_settings["calendar_events_sms_enabled"] = False
    if not email_notifications_available:
        calendar_notification_settings["calendar_events_email_enabled"] = False
    asana_overview = _asana_overview_context_for_user(
        request.user,
        force_refresh=False,
        cache_key=_ASANA_OVERVIEW_CACHE_KEY,
        task_fetch_limit=_ASANA_OVERVIEW_TASK_FETCH_LIMIT,
        run_auto_assign=False,
        write_calendar_cache=False,
    )
    asana_resource_options = _asana_resource_options_for_user(request.user)
    asana_resource_lookup = {
        str(item.get("resource_uuid") or "").strip().lower(): str(item.get("resource_name") or "").strip()
        for item in asana_resource_options
        if isinstance(item, dict)
    }
    agenda_item_resource_mappings = list_user_agenda_item_resource_mappings(request.user)
    asana_board_resource_mappings = list_user_asana_board_resource_mappings(request.user)
    asana_task_resource_mappings = list_user_asana_task_resource_mappings(request.user)
    asana_overview_tasks = asana_overview.get("tasks") if isinstance(asana_overview.get("tasks"), list) else []
    asana_overview["tasks"] = _asana_enriched_tasks_with_resource_mappings(
        task_rows=[row for row in asana_overview_tasks if isinstance(row, dict)],
        board_resource_mappings=asana_board_resource_mappings,
        task_resource_mappings=asana_task_resource_mappings,
        resource_name_lookup=asana_resource_lookup,
    )
    try:
        refresh_calendar_cache_for_user(
            request.user,
            provider="outlook",
            force=False,
        )
    except Exception:
        pass
    overview_calendar_external_items = _merge_planner_external_items(
        _asana_planner_items_from_context(asana_overview),
        _outlook_planner_items_for_user(request.user),
    )

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
            "twilio_sms_available": twilio_sms_available,
            "email_notifications_available": email_notifications_available,
            "calendar_notification_settings": calendar_notification_settings,
            "asana_overview": asana_overview,
            "overview_calendar_external_items": overview_calendar_external_items,
            "asana_resource_options": asana_resource_options,
            "agenda_item_resource_mappings": agenda_item_resource_mappings,
            "asana_board_resource_mappings": asana_board_resource_mappings,
            "asana_task_resource_mappings": asana_task_resource_mappings,
            "asana_completed_window_days": _ASANA_AGENDA_COMPLETED_WINDOW_DAYS,
        },
    )

@login_required
@require_POST
def refresh_calendar_cache(request):
    provider = str(request.POST.get("provider") or "").strip().lower()
    if not provider:
        try:
            payload = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            payload = {}
        if isinstance(payload, dict):
            provider = str(payload.get("provider") or "").strip().lower()
    if not provider:
        provider = "all"

    if provider not in {"all", "asana", "outlook"}:
        return JsonResponse(
            {
                "ok": False,
                "error": "unsupported_provider",
                "provider": provider,
            },
            status=400,
        )

    result = refresh_calendar_cache_for_user(
        request.user,
        provider=provider,
        force=True,
    )

    return JsonResponse(
        {
            "ok": True,
            "provider": provider,
            "result": result,
        }
    )


@login_required
@require_POST
def update_asana_task_completion(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_task_gid",
            },
            status=400,
        )

    completed_value: object | None = request.POST.get("completed")
    if completed_value is None:
        try:
            payload = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            payload = {}
        if isinstance(payload, dict):
            completed_value = payload.get("completed")
    completed_str = str(completed_value).strip().lower() if completed_value is not None else "true"
    completed = completed_str in {"1", "true", "yes", "on"}

    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse(
            {
                "ok": False,
                "error": str(token_error or "asana_not_connected"),
            },
            status=403,
        )

    _payload, update_error = _asana_api_request_json(
        method="PUT",
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}",
        body={"data": {"completed": bool(completed)}},
    )
    if _asana_error_requires_refresh(update_error):
        refreshed_token, refresh_error = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed_token:
            _payload, update_error = _asana_api_request_json(
                method="PUT",
                access_token=refreshed_token,
                path=f"/tasks/{resolved_task_gid}",
                body={"data": {"completed": bool(completed)}},
            )
        elif refresh_error:
            update_error = refresh_error
    if update_error:
        return JsonResponse(
            {
                "ok": False,
                "error": update_error,
            },
            status=502,
        )

    completed_at = datetime.now(timezone.utc).isoformat() if completed else ""
    _update_asana_overview_cache_completion(
        request.user,
        task_gid=resolved_task_gid,
        completed=completed,
        completed_at=completed_at,
    )
    update_user_calendar_event_completion(
        request.user,
        provider="asana",
        event_id=resolved_task_gid,
        is_completed=completed,
        status="completed" if completed else "open",
    )

    return JsonResponse(
        {
            "ok": True,
            "provider": "asana",
            "task_gid": resolved_task_gid,
            "completed": bool(completed),
            "completed_at": completed_at,
        }
    )


@login_required
@require_POST
def create_asana_board_task(request, board_gid: str):
    resolved_board_gid = str(board_gid or "").strip()
    if not resolved_board_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_board_gid",
            },
            status=400,
        )

    payload = _request_json_payload(request)
    task_name = str(request.POST.get("name") or payload.get("name") or "").strip()
    if not task_name:
        return JsonResponse(
            {
                "ok": False,
                "error": "task_name_required",
            },
            status=400,
        )
    if len(task_name) > 500:
        task_name = task_name[:500]

    notes = str(request.POST.get("notes") or payload.get("notes") or "").strip()
    if len(notes) > 5000:
        notes = notes[:5000]

    due_date_raw = str(
        request.POST.get("due_date")
        or payload.get("due_date")
        or request.POST.get("due_on")
        or payload.get("due_on")
        or ""
    ).strip()
    due_time_raw = str(request.POST.get("due_time") or payload.get("due_time") or "").strip()
    due_on = ""
    due_at = ""
    if due_time_raw and not due_date_raw:
        return JsonResponse(
            {
                "ok": False,
                "error": "due_date_required_for_time",
            },
            status=400,
        )
    if due_date_raw:
        try:
            due_date = datetime.strptime(due_date_raw, "%Y-%m-%d").date()
        except ValueError:
            return JsonResponse(
                {
                    "ok": False,
                    "error": "invalid_due_date",
                },
                status=400,
            )
        due_on = due_date.isoformat()
        if due_time_raw:
            due_time_value = None
            for fmt in ("%H:%M", "%H:%M:%S"):
                try:
                    due_time_value = datetime.strptime(due_time_raw, fmt).time()
                    break
                except ValueError:
                    continue
            if due_time_value is None:
                return JsonResponse(
                    {
                        "ok": False,
                        "error": "invalid_due_time",
                    },
                    status=400,
                )
            due_at = datetime.combine(due_date, due_time_value, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            due_on = ""

    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse(
            {
                "ok": False,
                "error": str(token_error or "asana_not_connected"),
            },
            status=403,
        )

    project_payload, project_error = _asana_api_get_json(
        access_token=access_token,
        path=f"/projects/{resolved_board_gid}",
        params={"opt_fields": "gid,name,permalink_url,workspace.gid,workspace.name"},
    )
    if _asana_error_requires_refresh(project_error):
        refreshed_token, refresh_error = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed_token:
            project_payload, project_error = _asana_api_get_json(
                access_token=refreshed_token,
                path=f"/projects/{resolved_board_gid}",
                params={"opt_fields": "gid,name,permalink_url,workspace.gid,workspace.name"},
            )
            access_token = refreshed_token
        elif refresh_error:
            project_error = refresh_error
    if project_error:
        return JsonResponse(
            {
                "ok": False,
                "error": str(project_error or "asana_board_lookup_failed"),
            },
            status=502,
        )

    project_data = project_payload.get("data") if isinstance(project_payload, dict) else {}
    if not isinstance(project_data, dict):
        project_data = {}
    board_name = str(project_data.get("name") or "").strip() or "Asana board"
    board_url = str(project_data.get("permalink_url") or "").strip() or _asana_project_url(resolved_board_gid)
    workspace_data = project_data.get("workspace") if isinstance(project_data.get("workspace"), dict) else {}
    workspace_gid = str(workspace_data.get("gid") or "").strip()
    workspace_name = str(workspace_data.get("name") or "").strip()
    if not workspace_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "asana_board_workspace_missing",
            },
            status=502,
        )

    create_data: dict[str, object] = {
        "name": task_name,
        "projects": [resolved_board_gid],
        "workspace": workspace_gid,
        "assignee": "me",
    }
    if notes:
        create_data["notes"] = notes
    if due_at:
        create_data["due_at"] = due_at
    elif due_on:
        create_data["due_on"] = due_on

    create_payload, create_error = _asana_api_request_json(
        method="POST",
        access_token=access_token,
        path="/tasks",
        params={"opt_fields": _ASANA_TASK_OPT_FIELDS},
        body={"data": create_data},
    )
    if _asana_error_requires_refresh(create_error):
        refreshed_token, refresh_error = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed_token:
            create_payload, create_error = _asana_api_request_json(
                method="POST",
                access_token=refreshed_token,
                path="/tasks",
                params={"opt_fields": _ASANA_TASK_OPT_FIELDS},
                body={"data": create_data},
            )
            access_token = refreshed_token
        elif refresh_error:
            create_error = refresh_error
    if create_error:
        return JsonResponse(
            {
                "ok": False,
                "error": str(create_error or "asana_task_create_failed"),
            },
            status=502,
        )

    created_task_data = create_payload.get("data") if isinstance(create_payload, dict) else {}
    if not isinstance(created_task_data, dict):
        created_task_data = {}
    created_task_gid = str(created_task_data.get("gid") or "").strip()
    if not created_task_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "asana_task_gid_missing",
            },
            status=502,
        )

    if not isinstance(created_task_data.get("memberships"), list):
        fetched_payload, fetched_error = _asana_api_get_json(
            access_token=access_token,
            path=f"/tasks/{created_task_gid}",
            params={"opt_fields": _ASANA_TASK_OPT_FIELDS},
        )
        if _asana_error_requires_refresh(fetched_error):
            refreshed_token, refresh_error = _asana_access_token_for_user(request.user, force_refresh=True)
            if refreshed_token:
                fetched_payload, fetched_error = _asana_api_get_json(
                    access_token=refreshed_token,
                    path=f"/tasks/{created_task_gid}",
                    params={"opt_fields": _ASANA_TASK_OPT_FIELDS},
                )
            elif refresh_error:
                fetched_error = refresh_error
        fetched_data = fetched_payload.get("data") if isinstance(fetched_payload, dict) else {}
        if isinstance(fetched_data, dict) and str(fetched_data.get("gid") or "").strip():
            created_task_data = fetched_data

    created_task_data["_workspace_gid"] = workspace_gid
    if not isinstance(created_task_data.get("workspace"), dict):
        created_task_data["workspace"] = {
            "gid": workspace_gid,
            "name": workspace_name,
        }
    memberships = created_task_data.get("memberships")
    if not isinstance(memberships, list) or not memberships:
        created_task_data["memberships"] = [
            {
                "project": {
                    "gid": resolved_board_gid,
                    "name": board_name,
                    "permalink_url": board_url,
                }
            }
        ]

    task_row = _asana_task_row_from_api_task(
        created_task_data,
        default_workspace_name=workspace_name,
    )
    if task_row is None:
        return JsonResponse(
            {
                "ok": False,
                "error": "asana_task_row_parse_failed",
            },
            status=502,
        )
    if not isinstance(task_row.get("project_links"), list) or not task_row.get("project_links"):
        task_row["project_links"] = [
            {
                "gid": resolved_board_gid,
                "name": board_name,
                "url": board_url,
            }
        ]

    _upsert_asana_overview_cache_task(request.user, task_row=task_row)

    return JsonResponse(
        {
            "ok": True,
            "provider": "asana",
            "board_gid": resolved_board_gid,
            "task_gid": created_task_gid,
            "task": task_row,
        }
    )


@login_required
@require_POST
def delete_asana_task(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_task_gid",
            },
            status=400,
        )

    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse(
            {
                "ok": False,
                "error": str(token_error or "asana_not_connected"),
            },
            status=403,
        )

    _payload, delete_error = _asana_api_request_json(
        method="DELETE",
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}",
    )
    if _asana_error_requires_refresh(delete_error):
        refreshed_token, refresh_error = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed_token:
            _payload, delete_error = _asana_api_request_json(
                method="DELETE",
                access_token=refreshed_token,
                path=f"/tasks/{resolved_task_gid}",
            )
        elif refresh_error:
            delete_error = refresh_error
    if delete_error:
        return JsonResponse(
            {
                "ok": False,
                "error": str(delete_error or "asana_task_delete_failed"),
            },
            status=502,
        )

    _remove_asana_overview_cache_task(request.user, task_gid=resolved_task_gid)
    set_user_asana_task_resource_mapping(request.user, task_gid=resolved_task_gid, resource_uuids=[])
    set_user_agenda_item_resource_mapping(
        request.user,
        item={
            "item_id": f"asana-agenda-{resolved_task_gid}",
            "source": "asana",
            "source_item_id": resolved_task_gid,
        },
        resource_uuids=[],
    )

    return JsonResponse(
        {
            "ok": True,
            "provider": "asana",
            "task_gid": resolved_task_gid,
            "deleted": True,
        }
    )


def _request_json_payload(request) -> dict[str, object]:
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _request_resource_uuid_values(request, *, field_name: str = "resource_uuids") -> list[str]:
    values: list[str] = []
    post_values = request.POST.getlist(field_name)
    if post_values:
        values.extend(post_values)
    payload = _request_json_payload(request)
    raw_value = payload.get(field_name)
    if isinstance(raw_value, list):
        values.extend([str(item or "") for item in raw_value])
    elif isinstance(raw_value, str):
        values.extend([part.strip() for part in raw_value.split(",") if part.strip()])

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        candidate = str(raw or "").strip().lower()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def _coerce_boolish(value, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value if value is not None else "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _request_agenda_item_payload(request) -> dict[str, object]:
    payload = _request_json_payload(request)
    nested_item = payload.get("item")
    item_data = nested_item if isinstance(nested_item, dict) else {}

    def pick(*keys: str, default: str = "") -> str:
        for key in keys:
            if key in item_data:
                candidate = str(item_data.get(key) or "").strip()
                if candidate:
                    return candidate
            if key in payload:
                candidate = str(payload.get(key) or "").strip()
                if candidate:
                    return candidate
            raw_post = str(request.POST.get(key) or "").strip()
            if raw_post:
                return raw_post
        return default

    item_id = pick("item_id", "id")
    source = pick("source")
    source_item_id = pick("source_item_id", "task_gid", "event_id")
    title = pick("title")
    due_date = pick("date", "due_date")
    due_time = pick("time", "due_time")
    due_at = pick("due_at")
    item_url = pick("url", "item_url")
    item_meta = pick("meta", "item_meta")

    done_value = None
    for key in ("done", "is_completed", "completed"):
        if key in item_data:
            done_value = item_data.get(key)
            break
        if key in payload:
            done_value = payload.get(key)
            break
        raw_post = request.POST.get(key)
        if raw_post is not None:
            done_value = raw_post
            break

    return {
        "item_id": item_id,
        "source": source,
        "source_item_id": source_item_id,
        "title": title,
        "date": due_date,
        "time": due_time,
        "due_at": due_at,
        "url": item_url,
        "meta": item_meta,
        "done": _coerce_boolish(done_value, default=False),
    }


@login_required
@require_GET
def list_asana_task_comments(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_task_gid",
            },
            status=400,
        )

    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse(
            {
                "ok": False,
                "error": str(token_error or "asana_not_connected"),
            },
            status=403,
        )

    comments, fetch_error = _asana_comment_rows_for_task(access_token, resolved_task_gid)
    if _asana_error_requires_refresh(fetch_error):
        refreshed_token, refresh_error = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed_token:
            comments, fetch_error = _asana_comment_rows_for_task(refreshed_token, resolved_task_gid)
        elif refresh_error:
            fetch_error = refresh_error

    if fetch_error:
        return JsonResponse(
            {
                "ok": False,
                "error": str(fetch_error or "asana_comments_failed"),
            },
            status=502,
        )

    return JsonResponse(
        {
            "ok": True,
            "provider": "asana",
            "task_gid": resolved_task_gid,
            "comment_count": len(comments),
            "comments": comments,
        }
    )


@login_required
@require_POST
def add_asana_task_comment(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_task_gid",
            },
            status=400,
        )

    payload = _request_json_payload(request)
    text = str(request.POST.get("text") or payload.get("text") or "").strip()
    if not text:
        return JsonResponse(
            {
                "ok": False,
                "error": "comment_text_required",
            },
            status=400,
        )
    if len(text) > 5000:
        text = text[:5000]

    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse(
            {
                "ok": False,
                "error": str(token_error or "asana_not_connected"),
            },
            status=403,
        )

    response_payload, post_error = _asana_api_request_json(
        method="POST",
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}/stories",
        body={"data": {"text": text}},
    )
    if _asana_error_requires_refresh(post_error):
        refreshed_token, refresh_error = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed_token:
            response_payload, post_error = _asana_api_request_json(
                method="POST",
                access_token=refreshed_token,
                path=f"/tasks/{resolved_task_gid}/stories",
                body={"data": {"text": text}},
            )
        elif refresh_error:
            post_error = refresh_error

    if post_error:
        return JsonResponse(
            {
                "ok": False,
                "error": str(post_error or "asana_comment_add_failed"),
            },
            status=502,
        )

    story = response_payload.get("data") if isinstance(response_payload, dict) else {}
    story = story if isinstance(story, dict) else {}
    created_at = str(story.get("created_at") or datetime.now(timezone.utc).isoformat()).strip()
    created_by = story.get("created_by") if isinstance(story.get("created_by"), dict) else {}
    return JsonResponse(
        {
            "ok": True,
            "provider": "asana",
            "task_gid": resolved_task_gid,
            "comment": {
                "gid": str(story.get("gid") or "").strip(),
                "text": str(story.get("text") or text).strip(),
                "created_at": created_at,
                "created_display": _format_display_time(created_at),
                "author_gid": str(created_by.get("gid") or "").strip(),
                "author_name": str(created_by.get("name") or "").strip() or "Asana user",
            },
        }
    )


@login_required
@require_GET
def list_asana_task_subtasks(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse({"ok": False, "error": "missing_task_gid"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    subtasks, _trunc, fetch_error = _asana_api_list(
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}/subtasks",
        params={"opt_fields": "gid,name,completed,due_on,assignee.name"},
        max_items=100,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            subtasks, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed,
                path=f"/tasks/{resolved_task_gid}/subtasks",
                params={"opt_fields": "gid,name,completed,due_on,assignee.name"},
                max_items=100,
            )
    if fetch_error:
        return JsonResponse({"ok": False, "error": fetch_error}, status=502)
    rows = [
        {
            "gid": t.get("gid"),
            "name": t.get("name"),
            "completed": t.get("completed"),
            "due_on": t.get("due_on"),
            "assignee": (t.get("assignee") or {}).get("name"),
        }
        for t in (subtasks or [])
    ]
    return JsonResponse({"ok": True, "task_gid": resolved_task_gid, "subtasks": rows})


@login_required
@require_GET
def list_asana_project_sections(request, board_gid: str):
    resolved_board_gid = str(board_gid or "").strip()
    if not resolved_board_gid:
        return JsonResponse({"ok": False, "error": "missing_board_gid"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    sections, _trunc, fetch_error = _asana_api_list(
        access_token=access_token,
        path=f"/projects/{resolved_board_gid}/sections",
        params={"opt_fields": "gid,name"},
        max_items=200,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            sections, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed,
                path=f"/projects/{resolved_board_gid}/sections",
                params={"opt_fields": "gid,name"},
                max_items=200,
            )
    if fetch_error:
        return JsonResponse({"ok": False, "error": fetch_error}, status=502)
    rows = [{"gid": s.get("gid"), "name": s.get("name")} for s in (sections or [])]
    return JsonResponse({"ok": True, "board_gid": resolved_board_gid, "sections": rows})


@login_required
@require_POST
def move_asana_task_to_section(request, section_gid: str):
    resolved_section_gid = str(section_gid or "").strip()
    if not resolved_section_gid:
        return JsonResponse({"ok": False, "error": "missing_section_gid"}, status=400)
    payload = _request_json_payload(request)
    task_gid = str(request.POST.get("task_gid") or payload.get("task_gid") or "").strip()
    if not task_gid:
        return JsonResponse({"ok": False, "error": "task_gid_required"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    _result, move_error = _asana_api_request_json(
        method="POST",
        access_token=access_token,
        path=f"/sections/{resolved_section_gid}/addTask",
        body={"data": {"task": task_gid}},
    )
    if _asana_error_requires_refresh(move_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            _result, move_error = _asana_api_request_json(
                method="POST",
                access_token=refreshed,
                path=f"/sections/{resolved_section_gid}/addTask",
                body={"data": {"task": task_gid}},
            )
    if move_error:
        return JsonResponse({"ok": False, "error": move_error}, status=502)
    return JsonResponse({"ok": True, "task_gid": task_gid, "section_gid": resolved_section_gid})


@login_required
@require_POST
def update_asana_task_assignee(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse({"ok": False, "error": "missing_task_gid"}, status=400)
    payload = _request_json_payload(request)
    assignee_gid = str(request.POST.get("assignee_gid") or payload.get("assignee_gid") or "").strip()
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    body_data = {"data": {"assignee": assignee_gid if assignee_gid else None}}
    _result, assign_error = _asana_api_request_json(
        method="PUT",
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}",
        body=body_data,
    )
    if _asana_error_requires_refresh(assign_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            _result, assign_error = _asana_api_request_json(
                method="PUT",
                access_token=refreshed,
                path=f"/tasks/{resolved_task_gid}",
                body=body_data,
            )
    if assign_error:
        return JsonResponse({"ok": False, "error": assign_error}, status=502)
    return JsonResponse({"ok": True, "task_gid": resolved_task_gid, "assignee_gid": assignee_gid})


@login_required
@require_GET
def list_asana_workspace_members(request, workspace_gid: str):
    resolved_workspace_gid = str(workspace_gid or "").strip()
    if not resolved_workspace_gid:
        return JsonResponse({"ok": False, "error": "missing_workspace_gid"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    members, _trunc, fetch_error = _asana_api_list(
        access_token=access_token,
        path=f"/workspaces/{resolved_workspace_gid}/users",
        params={"opt_fields": "gid,name,email"},
        max_items=500,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            members, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed,
                path=f"/workspaces/{resolved_workspace_gid}/users",
                params={"opt_fields": "gid,name,email"},
                max_items=500,
            )
    if fetch_error:
        return JsonResponse({"ok": False, "error": fetch_error}, status=502)
    rows = [
        {"gid": m.get("gid"), "name": m.get("name"), "email": m.get("email")}
        for m in (members or [])
    ]
    return JsonResponse({"ok": True, "workspace_gid": resolved_workspace_gid, "members": rows})


@login_required
@require_GET
def list_asana_task_dependencies(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse({"ok": False, "error": "missing_task_gid"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    deps, _trunc, fetch_error = _asana_api_list(
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}/dependencies",
        params={"opt_fields": "gid,name,completed"},
        max_items=100,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            deps, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed,
                path=f"/tasks/{resolved_task_gid}/dependencies",
                params={"opt_fields": "gid,name,completed"},
                max_items=100,
            )
    if fetch_error:
        return JsonResponse({"ok": False, "error": fetch_error}, status=502)
    rows = [{"gid": d.get("gid"), "name": d.get("name"), "completed": d.get("completed")} for d in (deps or [])]
    return JsonResponse({"ok": True, "task_gid": resolved_task_gid, "dependencies": rows})


@login_required
@require_POST
def add_asana_task_dependency(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse({"ok": False, "error": "missing_task_gid"}, status=400)
    payload = _request_json_payload(request)
    dependency_gid = str(request.POST.get("dependency_gid") or payload.get("dependency_gid") or "").strip()
    if not dependency_gid:
        return JsonResponse({"ok": False, "error": "dependency_gid_required"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    _result, add_error = _asana_api_request_json(
        method="POST",
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}/addDependencies",
        body={"data": {"dependencies": [dependency_gid]}},
    )
    if _asana_error_requires_refresh(add_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            _result, add_error = _asana_api_request_json(
                method="POST",
                access_token=refreshed,
                path=f"/tasks/{resolved_task_gid}/addDependencies",
                body={"data": {"dependencies": [dependency_gid]}},
            )
    if add_error:
        return JsonResponse({"ok": False, "error": add_error}, status=502)
    return JsonResponse({"ok": True, "task_gid": resolved_task_gid, "dependency_gid": dependency_gid})


@login_required
@require_POST
def remove_asana_task_dependency(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse({"ok": False, "error": "missing_task_gid"}, status=400)
    payload = _request_json_payload(request)
    dependency_gid = str(request.POST.get("dependency_gid") or payload.get("dependency_gid") or "").strip()
    if not dependency_gid:
        return JsonResponse({"ok": False, "error": "dependency_gid_required"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    _result, remove_error = _asana_api_request_json(
        method="POST",
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}/removeDependencies",
        body={"data": {"dependencies": [dependency_gid]}},
    )
    if _asana_error_requires_refresh(remove_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            _result, remove_error = _asana_api_request_json(
                method="POST",
                access_token=refreshed,
                path=f"/tasks/{resolved_task_gid}/removeDependencies",
                body={"data": {"dependencies": [dependency_gid]}},
            )
    if remove_error:
        return JsonResponse({"ok": False, "error": remove_error}, status=502)
    return JsonResponse({"ok": True, "task_gid": resolved_task_gid, "dependency_gid": dependency_gid})


@login_required
@require_GET
def get_asana_project_status(request, board_gid: str):
    resolved_board_gid = str(board_gid or "").strip()
    if not resolved_board_gid:
        return JsonResponse({"ok": False, "error": "missing_board_gid"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    statuses, _trunc, fetch_error = _asana_api_list(
        access_token=access_token,
        path=f"/projects/{resolved_board_gid}/project_statuses",
        params={"opt_fields": "gid,title,color,text,created_at,author.name", "limit": 5},
        max_items=5,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            statuses, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed,
                path=f"/projects/{resolved_board_gid}/project_statuses",
                params={"opt_fields": "gid,title,color,text,created_at,author.name", "limit": 5},
                max_items=5,
            )
    if fetch_error:
        return JsonResponse({"ok": False, "error": fetch_error}, status=502)
    latest = statuses[0] if statuses else None
    return JsonResponse({"ok": True, "board_gid": resolved_board_gid, "latest_status": latest})


@login_required
@require_GET
def list_asana_task_attachments(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse({"ok": False, "error": "missing_task_gid"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)
    attachments, _trunc, fetch_error = _asana_api_list(
        access_token=access_token,
        path=f"/tasks/{resolved_task_gid}/attachments",
        params={"opt_fields": "gid,name,download_url,view_url,created_at,size"},
        max_items=100,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            attachments, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed,
                path=f"/tasks/{resolved_task_gid}/attachments",
                params={"opt_fields": "gid,name,download_url,view_url,created_at,size"},
                max_items=100,
            )
    if fetch_error:
        return JsonResponse({"ok": False, "error": fetch_error}, status=502)
    rows = [
        {
            "gid": a.get("gid"),
            "name": a.get("name"),
            "download_url": a.get("download_url"),
            "view_url": a.get("view_url"),
            "created_at": a.get("created_at"),
            "size": a.get("size"),
        }
        for a in (attachments or [])
    ]
    return JsonResponse({"ok": True, "task_gid": resolved_task_gid, "attachments": rows})


@login_required
@require_POST
def register_asana_webhook(request, board_gid: str):
    resolved_board_gid = str(board_gid or "").strip()
    if not resolved_board_gid:
        return JsonResponse({"ok": False, "error": "missing_board_gid"}, status=400)
    access_token, token_error = _asana_access_token_for_user(request.user)
    if not access_token:
        return JsonResponse({"ok": False, "error": str(token_error or "asana_not_connected")}, status=403)

    app_base_url = str(getattr(settings, "APP_BASE_URL", "") or "").rstrip("/")
    if not app_base_url:
        return JsonResponse({"ok": False, "error": "APP_BASE_URL is not configured"}, status=500)
    target_url = f"{app_base_url}/calendar/asana/webhook/receive/"

    webhook_body = {
        "data": {
            "resource": resolved_board_gid,
            "target": target_url,
            "filters": [
                {"resource_type": "task", "action": "changed"},
                {"resource_type": "task", "action": "added"},
                {"resource_type": "task", "action": "removed"},
            ],
        }
    }
    result_payload, reg_error = _asana_api_request_json(
        method="POST",
        access_token=access_token,
        path="/webhooks",
        body=webhook_body,
    )
    if _asana_error_requires_refresh(reg_error):
        refreshed, _ = _asana_access_token_for_user(request.user, force_refresh=True)
        if refreshed:
            result_payload, reg_error = _asana_api_request_json(
                method="POST",
                access_token=refreshed,
                path="/webhooks",
                body=webhook_body,
            )
    if reg_error:
        return JsonResponse({"ok": False, "error": reg_error}, status=502)

    webhook_data = result_payload.get("data") if isinstance(result_payload, dict) else {}
    webhook_data = webhook_data if isinstance(webhook_data, dict) else {}
    webhook_gid = str(webhook_data.get("gid") or "").strip()
    webhook_secret = str(webhook_data.get("secret") or "").strip()

    # Persist the webhook secret for HMAC verification of incoming events
    if webhook_gid:
        webhooks_file = _user_owner_dir(request.user) / "asana_webhooks.json"
        try:
            existing: dict[str, object] = {}
            if webhooks_file.exists():
                existing = json.loads(webhooks_file.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        existing[resolved_board_gid] = {"webhook_gid": webhook_gid, "secret": webhook_secret}
        try:
            webhooks_file.write_text(json.dumps(existing), encoding="utf-8")
        except Exception:
            pass

    return JsonResponse({
        "ok": True,
        "board_gid": resolved_board_gid,
        "webhook_gid": webhook_gid,
        "target": target_url,
    })


@csrf_exempt
@require_POST
def receive_asana_webhook(request):
    # Asana handshake: echo back X-Hook-Secret on first delivery
    hook_secret_header = request.headers.get("X-Hook-Secret") or request.META.get("HTTP_X_HOOK_SECRET", "")
    if hook_secret_header:
        response = HttpResponse(status=200)
        response["X-Hook-Secret"] = hook_secret_header
        return response

    # Verify HMAC-SHA256 signature for subsequent event deliveries
    hook_signature = request.headers.get("X-Hook-Signature") or request.META.get("HTTP_X_HOOK_SIGNATURE", "")
    raw_body = request.body
    if hook_signature and raw_body:
        # Find a matching webhook secret across all users
        var_dir = Path(settings.BASE_DIR) / "var" / "user_data"
        verified = False
        if var_dir.exists():
            for webhooks_file in var_dir.glob("*/asana_webhooks.json"):
                try:
                    stored: dict[str, object] = json.loads(webhooks_file.read_text(encoding="utf-8"))
                except Exception:
                    continue
                for _board_gid, entry in stored.items():
                    if not isinstance(entry, dict):
                        continue
                    secret = str(entry.get("secret") or "").strip()
                    if not secret:
                        continue
                    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
                    if hmac.compare_digest(expected, hook_signature):
                        verified = True
                        break
                if verified:
                    break
        if not verified:
            return HttpResponse(status=403)

    # Parse and process events — invalidate the relevant user's cache
    try:
        events_payload = json.loads(raw_body)
    except Exception:
        return HttpResponse(status=400)

    events = events_payload.get("events") if isinstance(events_payload, dict) else []
    if not isinstance(events, list):
        return HttpResponse(status=200)

    # Best-effort cache invalidation: trigger a refresh for all users with Asana connected
    from .calendar_sync_service import refresh_calendar_cache_for_user
    User = get_user_model()
    processed_user_ids: set[int] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        resource_obj = event.get("resource") if isinstance(event.get("resource"), dict) else {}
        if not resource_obj:
            continue
        # Trigger cache refresh for users — run in background thread to not block webhook response
        if not processed_user_ids:
            try:
                from allauth.socialaccount.models import SocialAccount as _SA
                asana_users = list(User.objects.filter(
                    socialaccount__provider="asana"
                ).distinct()[:10])
                for u in asana_users:
                    if u.pk in processed_user_ids:
                        continue
                    processed_user_ids.add(u.pk)
                    try:
                        refresh_calendar_cache_for_user(u, provider="asana", force=True)
                    except Exception:
                        pass
            except Exception:
                pass
        break  # Only need to trigger once per webhook call

    return HttpResponse(status=200)


@login_required
@require_POST
def update_asana_board_resource_mapping(request, board_gid: str):
    resolved_board_gid = str(board_gid or "").strip()
    if not resolved_board_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_board_gid",
            },
            status=400,
        )

    requested_resource_uuids = _request_resource_uuid_values(request)
    allowed_options = _asana_resource_options_for_user(request.user)
    allowed_set = {
        str(item.get("resource_uuid") or "").strip().lower()
        for item in allowed_options
        if isinstance(item, dict)
    }
    filtered_uuids = [value for value in requested_resource_uuids if value in allowed_set]
    saved_uuids = set_user_asana_board_resource_mapping(
        request.user,
        board_gid=resolved_board_gid,
        resource_uuids=filtered_uuids,
    )
    return JsonResponse(
        {
            "ok": True,
            "provider": "asana",
            "board_gid": resolved_board_gid,
            "resource_uuids": saved_uuids,
        }
    )


@login_required
@require_POST
def update_asana_task_resource_mapping(request, task_gid: str):
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_task_gid",
            },
            status=400,
        )

    requested_resource_uuids = _request_resource_uuid_values(request)
    allowed_options = _asana_resource_options_for_user(request.user)
    allowed_set = {
        str(item.get("resource_uuid") or "").strip().lower()
        for item in allowed_options
        if isinstance(item, dict)
    }
    filtered_uuids = [value for value in requested_resource_uuids if value in allowed_set]
    saved_uuids = set_user_asana_task_resource_mapping(
        request.user,
        task_gid=resolved_task_gid,
        resource_uuids=filtered_uuids,
    )
    return JsonResponse(
        {
            "ok": True,
            "provider": "asana",
            "task_gid": resolved_task_gid,
            "resource_uuids": saved_uuids,
        }
    )


@login_required
@require_POST
def update_overview_agenda_item_resource_mapping(request):
    item_payload = _request_agenda_item_payload(request)
    resolved_item_id = str(item_payload.get("item_id") or "").strip()
    if not resolved_item_id:
        return JsonResponse(
            {
                "ok": False,
                "error": "missing_item_id",
            },
            status=400,
        )

    requested_resource_uuids = _request_resource_uuid_values(request)
    allowed_options = _asana_resource_options_for_user(request.user)
    allowed_set = {
        str(item.get("resource_uuid") or "").strip().lower()
        for item in allowed_options
        if isinstance(item, dict)
    }
    filtered_uuids = [value for value in requested_resource_uuids if value in allowed_set]

    existing_mappings = list_user_agenda_item_resource_mappings(request.user)
    previous_uuids = [
        str(value or "").strip().lower()
        for value in (existing_mappings.get(resolved_item_id) or [])
        if str(value or "").strip()
    ]
    saved_uuids = set_user_agenda_item_resource_mapping(
        request.user,
        item=item_payload,
        resource_uuids=filtered_uuids,
    )
    affected_resource_uuids = {
        str(value or "").strip().lower()
        for value in [*previous_uuids, *saved_uuids]
        if str(value or "").strip()
    }
    for resource_uuid in sorted(affected_resource_uuids):
        try:
            _upsert_resource_kb_after_wiki_mutation(actor=request.user, resource_uuid=resource_uuid)
        except Exception:
            continue
    return JsonResponse(
        {
            "ok": True,
            "item_id": resolved_item_id,
            "source": str(item_payload.get("source") or "").strip().lower(),
            "resource_uuids": saved_uuids,
        }
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
@require_POST
def update_overview_calendar_notification_settings(request):
    twilio_sms_available = is_twilio_configured()
    email_notifications_available = is_support_inbox_email_alerts_enabled()
    payload = {
        "calendar_events_app_enabled": _post_flag(request.POST, "calendar_events_app_enabled"),
        "calendar_events_sms_enabled": _post_flag(request.POST, "calendar_events_sms_enabled") if twilio_sms_available else False,
        "calendar_events_email_enabled": _post_flag(request.POST, "calendar_events_email_enabled") if email_notifications_available else False,
    }
    upsert_user_calendar_notification_settings(
        request.user,
        payload=payload,
    )
    messages.success(request, "Calendar notification settings updated.")
    return redirect(f"{reverse('home')}#calendar-alerts")


@login_required
@require_GET
def search_kb_suggestions(request):
    query = re.sub(r"\s+", " ", str(request.GET.get("q") or "")).strip()
    if len(query) < 2:
        return JsonResponse(
            {
                "ok": True,
                "query": query,
                "result_count": 0,
                "results": [],
            }
        )

    raw_limit = str(request.GET.get("limit") or "").strip()
    try:
        limit = int(raw_limit or 8)
    except (TypeError, ValueError):
        limit = 8
    resolved_limit = max(1, min(limit, 12))

    payload = _tool_search_kb_for_actor(request.user, {"query": query})
    if not bool(payload.get("ok")):
        return JsonResponse(
            {
                "ok": False,
                "query": query,
                "result_count": 0,
                "results": [],
                "error": str(payload.get("error") or "search_failed"),
            },
            status=502,
        )

    raw_results = payload.get("results") if isinstance(payload, dict) else []
    if not isinstance(raw_results, list):
        raw_results = []
    context_resource_uuid = _normalize_resource_uuid(str(request.GET.get("context_resource_uuid") or ""))
    contextual_rows: list[dict[str, object]] = []
    if context_resource_uuid:
        try:
            contextual_rows = _resource_context_kb_rows_for_actor(
                actor=request.user,
                resource_uuid=context_resource_uuid,
                query=query,
                kb_limit=4,
                wiki_limit=4,
            )
        except Exception:
            contextual_rows = []
    merged_rows = contextual_rows + raw_results
    results = _build_topbar_kb_suggestions(
        actor=request.user,
        rows=merged_rows,
        limit=resolved_limit,
    )
    return JsonResponse(
        {
            "ok": True,
            "query": query,
            "result_count": len(results),
            "results": results,
        }
    )


def _connector_settings_ui_state(
    request,
    *,
    redirect_base: str,
) -> tuple[dict[str, object], HttpResponse | None]:
    errors: list[str] = []
    initial = _connector_initial_values()
    connector_runtime = _connector_runtime_context(request)
    known_connectors = {"openai", "microsoft", "github", "asana", "twilio"}
    connector_labels = {
        "openai": "OpenAI",
        "microsoft": "Microsoft",
        "github": "GitHub",
        "asana": "Asana",
        "twilio": "Twilio",
    }
    action_connector_map = {
        "save_openai": "openai",
        "save_microsoft": "microsoft",
        "save_github": "github",
        "save_asana": "asana",
        "save_twilio": "twilio",
        "test_microsoft": "microsoft",
        "test_github": "github",
        "test_asana": "asana",
    }
    active_connector = str(request.GET.get("connector") or "").strip().lower()
    if active_connector not in known_connectors:
        active_connector = ""

    if request.method == "POST":
        setup_action = (request.POST.get("setup_action") or "").strip().lower() or "complete"
        posted_connector = str(request.POST.get("setup_connector") or "").strip().lower()
        initial["openai_api_key"] = (request.POST.get("openai_api_key") or "").strip()
        initial["microsoft_tenant_id"] = (request.POST.get("microsoft_tenant_id") or "").strip()
        initial["microsoft_client_id"] = (request.POST.get("microsoft_client_id") or "").strip()
        initial["microsoft_client_secret"] = (request.POST.get("microsoft_client_secret") or "").strip()
        initial["microsoft_mailbox_email"] = (request.POST.get("microsoft_mailbox_email") or "").strip().lower()
        initial["github_client_id"] = (request.POST.get("github_client_id") or "").strip()
        initial["github_client_secret"] = (request.POST.get("github_client_secret") or "").strip()
        initial["asana_client_id"] = (request.POST.get("asana_client_id") or "").strip()
        initial["asana_client_secret"] = (request.POST.get("asana_client_secret") or "").strip()
        initial["twilio_account_sid"] = (request.POST.get("twilio_account_sid") or "").strip()
        initial["twilio_auth_token"] = (request.POST.get("twilio_auth_token") or "").strip()
        initial["twilio_from_number"] = (request.POST.get("twilio_from_number") or "").strip()
        targeted_connector = action_connector_map.get(setup_action)
        if targeted_connector:
            active_connector = targeted_connector
        elif posted_connector in known_connectors:
            active_connector = posted_connector
        else:
            active_connector = ""

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
        has_any_asana_values = any([initial["asana_client_id"], initial["asana_client_secret"]])
        has_full_asana_values = all([initial["asana_client_id"], initial["asana_client_secret"]])
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

        if setup_action == "complete":
            selected_connectors = set(known_connectors)
        elif targeted_connector:
            selected_connectors = {targeted_connector}
        else:
            selected_connectors = set(known_connectors)

        if "microsoft" in selected_connectors and has_any_microsoft_values and not has_full_microsoft_values:
            errors.append("To configure Microsoft Entra, provide Tenant ID, Client ID, and Client Secret Value.")
        if initial["microsoft_mailbox_email"] and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", initial["microsoft_mailbox_email"]):
            errors.append("Microsoft Email Agent mailbox must be a valid email address.")
        if "github" in selected_connectors and has_any_github_values and not has_full_github_values:
            errors.append("To configure GitHub OAuth, provide Client ID and Client Secret.")
        if "asana" in selected_connectors and has_any_asana_values and not has_full_asana_values:
            errors.append("To configure Asana OAuth, provide Client ID and Client Secret.")
        if "twilio" in selected_connectors and has_any_twilio_values and not has_full_twilio_values:
            errors.append("To configure Twilio alerts, provide Account SID, Auth Token, and a From number.")
        if setup_action == "test_microsoft" and not has_full_microsoft_values:
            errors.append("Provide Microsoft Entra Tenant ID, Client ID, and Client Secret before testing sign-in.")
        if setup_action == "test_github" and not has_full_github_values:
            errors.append("Provide GitHub OAuth Client ID and Client Secret before testing sign-in.")
        if setup_action == "test_asana" and not has_full_asana_values:
            errors.append("Provide Asana OAuth Client ID and Client Secret before testing sign-in.")

        if not errors:
            setup = get_or_create_setup_state()
            if setup is None:
                errors.append("Setup database is not ready yet. Run migrations and try again.")
            else:
                setup_update_fields: list[str] = []
                if "openai" in selected_connectors:
                    setup.openai_api_key = initial["openai_api_key"]
                    setup_update_fields.append("openai_api_key")
                if "microsoft" in selected_connectors:
                    setup.microsoft_mailbox_email = initial["microsoft_mailbox_email"]
                    setup_update_fields.append("microsoft_mailbox_email")
                if "twilio" in selected_connectors:
                    setup.twilio_account_sid = initial["twilio_account_sid"]
                    setup.twilio_auth_token = initial["twilio_auth_token"]
                    setup.twilio_from_number = initial["twilio_from_number"]
                    setup_update_fields.extend(
                        [
                            "twilio_account_sid",
                            "twilio_auth_token",
                            "twilio_from_number",
                        ]
                    )
                if setup_update_fields:
                    setup.save(update_fields=[*setup_update_fields, "updated_at"])

                if "microsoft" in selected_connectors and has_full_microsoft_values:
                    try:
                        microsoft_app, site = _social_app_for_provider("microsoft")
                        if microsoft_app is None:
                            microsoft_app = SocialApp(provider="microsoft", name="Microsoft Entra")
                        microsoft_app.client_id = initial["microsoft_client_id"]
                        microsoft_app.secret = initial["microsoft_client_secret"]
                        app_settings = dict(microsoft_app.settings or {})
                        app_settings["tenant"] = initial["microsoft_tenant_id"]
                        app_settings["scope"] = list(_MICROSOFT_CONNECTOR_SCOPES)
                        microsoft_app.settings = app_settings
                        microsoft_app.save()
                        if site is not None:
                            microsoft_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save Microsoft Entra social app settings.")

                if "github" in selected_connectors and has_full_github_values:
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
                if "asana" in selected_connectors and has_full_asana_values:
                    try:
                        asana_app, site = _social_app_for_provider("asana")
                        if asana_app is None:
                            asana_app = SocialApp(provider="asana", name="Asana OAuth")
                        asana_app.client_id = initial["asana_client_id"]
                        asana_app.secret = initial["asana_client_secret"]
                        asana_app.save()
                        if site is not None:
                            asana_app.sites.add(site)
                    except Exception:
                        errors.append("Unable to save Asana social app settings.")

        if not errors:
            if setup_action == "test_microsoft" and has_full_microsoft_values:
                try:
                    microsoft_login_url = reverse("microsoft_login")
                except NoReverseMatch:
                    microsoft_login_url = "/accounts/microsoft/login/"
                messages.success(request, "Connector settings saved. Continue with Microsoft sign-in to test login.")
                return {}, redirect(f"{microsoft_login_url}?process=login")
            if setup_action == "test_github" and has_full_github_values:
                try:
                    github_login_url = reverse("github_login")
                except NoReverseMatch:
                    github_login_url = "/accounts/github/login/"
                messages.success(request, "Connector settings saved. Continue with GitHub sign-in to test login.")
                return {}, redirect(f"{github_login_url}?process=login")
            if setup_action == "test_asana" and has_full_asana_values:
                try:
                    asana_login_url = reverse("asana_login")
                except NoReverseMatch:
                    asana_login_url = "/accounts/asana/login/"
                messages.success(request, "Connector settings saved. Continue with Asana sign-in to test connection.")
                return {}, redirect(f"{asana_login_url}?process=connect")

            if targeted_connector:
                messages.success(
                    request,
                    f"{connector_labels.get(targeted_connector, 'Connector')} settings saved.",
                )
                redirect_target = (
                    f"{redirect_base}&connector={targeted_connector}"
                    if "?" in redirect_base
                    else f"{redirect_base}?connector={targeted_connector}"
                )
                return {}, redirect(redirect_target)

            messages.success(request, "Connector settings updated.")
            return {}, redirect(redirect_base)

    return {
        "errors": errors,
        "initial": initial,
        "active_connector": active_connector,
        **connector_runtime,
    }, None


@login_required
def app_settings(request):
    active_tab = (request.GET.get('tab') or 'account').strip().lower()
    if active_tab not in {'account', 'api-key', 'connectors', 'admin'}:
        active_tab = 'account'
    if active_tab in {"admin", "connectors"} and not request.user.is_superuser:
        active_tab = "account"

    notification_settings = UserNotificationSettings.objects.filter(user=request.user).first()
    account_phone_number = str(getattr(notification_settings, "phone_number", "") or "").strip()
    connector_settings_context: dict[str, object] = {}
    if request.user.is_superuser and active_tab == "connectors":
        connector_settings_context, connector_response = _connector_settings_ui_state(
            request,
            redirect_base=f"{reverse('app_settings')}?tab=connectors",
        )
        if connector_response is not None:
            return connector_response

    def _social_account_email(account: SocialAccount | None) -> str:
        if account is None:
            return ""
        extra_data = dict(getattr(account, "extra_data", {}) or {})
        candidates = [
            extra_data.get("mail"),
            extra_data.get("email"),
            extra_data.get("userPrincipalName"),
            extra_data.get("preferred_username"),
            getattr(account, "uid", ""),
        ]
        for raw_value in candidates:
            value = str(raw_value or "").strip()
            if value and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value):
                return value.lower()
        return ""

    if request.method == "POST" and active_tab == "account":
        phone_number = (request.POST.get("phone_number") or "").strip()
        raw_display_name = request.POST.get("display_name")
        display_name = None
        if raw_display_name is not None:
            display_name = " ".join(str(raw_display_name or "").split())
        posted_email = str(request.POST.get("email") or "").strip().lower()
        microsoft_account: SocialAccount | None = None
        try:
            microsoft_account = (
                SocialAccount.objects.filter(user=request.user, provider="microsoft")
                .order_by("id")
                .first()
            )
        except (OperationalError, ProgrammingError):
            microsoft_account = None
        except Exception:
            microsoft_account = None
        email_locked_to_microsoft = microsoft_account is not None
        microsoft_email = _social_account_email(microsoft_account)
        if email_locked_to_microsoft:
            email_value = microsoft_email or str(getattr(request.user, "email", "") or "").strip()
        else:
            email_value = posted_email
        if len(phone_number) > 32:
            messages.warning(request, "Phone number is too long.")
            return redirect(f"{reverse('app_settings')}?tab=account")
        if phone_number and not re.match(r"^[0-9+()\\-\\s]{6,32}$", phone_number):
            messages.warning(request, "Use a valid phone number format.")
            return redirect(f"{reverse('app_settings')}?tab=account")
        if display_name is not None and len(display_name) > 150:
            messages.warning(request, "Display name must be 150 characters or fewer.")
            return redirect(f"{reverse('app_settings')}?tab=account")
        if not email_locked_to_microsoft:
            if len(email_value) > 254:
                messages.warning(request, "Email must be 254 characters or fewer.")
                return redirect(f"{reverse('app_settings')}?tab=account")
            if email_value and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email_value):
                messages.warning(request, "Use a valid email address.")
                return redirect(f"{reverse('app_settings')}?tab=account")

        account_updated = False
        user_update_fields: list[str] = []
        if display_name is not None:
            current_display_name = " ".join(str(request.user.get_full_name() or "").split())
            if display_name != current_display_name:
                request.user.first_name = display_name
                request.user.last_name = ""
                user_update_fields.extend(["first_name", "last_name"])

        existing_email = str(getattr(request.user, "email", "") or "").strip()
        if email_value != existing_email:
            request.user.email = email_value
            user_update_fields.append("email")

        if user_update_fields:
            request.user.save(update_fields=list(dict.fromkeys(user_update_fields)))
            account_updated = True

        settings_row, _created = UserNotificationSettings.objects.get_or_create(user=request.user)
        existing_phone_number = str(getattr(settings_row, "phone_number", "") or "").strip()
        if existing_phone_number != phone_number:
            settings_row.phone_number = phone_number
            settings_row.save(update_fields=["phone_number", "updated_at"])
            account_updated = True

        if account_updated:
            messages.success(request, "Account settings updated.")
        else:
            messages.info(request, "No account changes to save.")
        return redirect(f"{reverse('app_settings')}?tab=account")

    settings_account_url = f"{reverse('app_settings')}?tab=account"
    microsoft_connector_configured = is_microsoft_connector_configured()
    github_connector_configured = is_github_connector_configured()
    try:
        asana_connector_configured = (
            SocialApp.objects.filter(provider="asana")
            .exclude(client_id__exact="")
            .exclude(secret__exact="")
            .order_by("id")
            .exists()
        )
    except (OperationalError, ProgrammingError):
        asana_connector_configured = False
    except Exception:
        asana_connector_configured = False

    social_accounts_by_provider: dict[str, SocialAccount | None] = {
        "microsoft": None,
        "github": None,
        "asana": None,
    }
    try:
        linked_social_accounts = list(
            SocialAccount.objects.filter(
                user=request.user,
                provider__in=("microsoft", "github", "asana"),
            ).order_by("id")
        )
    except (OperationalError, ProgrammingError):
        linked_social_accounts = []
    except Exception:
        linked_social_accounts = []
    for social_account in linked_social_accounts:
        provider_key = str(getattr(social_account, "provider", "") or "").strip().lower()
        if provider_key in social_accounts_by_provider and social_accounts_by_provider[provider_key] is None:
            social_accounts_by_provider[provider_key] = social_account

    def _connected_identity_display(account: SocialAccount | None) -> str:
        if account is None:
            return ""
        provider_key = str(getattr(account, "provider", "") or "").strip().lower()
        extra_data = dict(getattr(account, "extra_data", {}) or {})
        if provider_key == "github":
            candidates = [
                extra_data.get("login"),
                extra_data.get("name"),
                getattr(account, "uid", ""),
            ]
        elif provider_key == "asana":
            candidates = [
                extra_data.get("name"),
                extra_data.get("email"),
                getattr(account, "uid", ""),
            ]
        else:
            candidates = [
                extra_data.get("preferred_username"),
                extra_data.get("mail"),
                extra_data.get("userPrincipalName"),
                extra_data.get("name"),
                getattr(account, "uid", ""),
            ]
        for raw_value in candidates:
            value = str(raw_value or "").strip()
            if value:
                return value
        return ""

    account_email_locked_to_microsoft = social_accounts_by_provider["microsoft"] is not None
    account_email = str(getattr(request.user, "email", "") or "").strip()
    if account_email_locked_to_microsoft:
        microsoft_connected_email = _social_account_email(social_accounts_by_provider["microsoft"])
        if microsoft_connected_email:
            account_email = microsoft_connected_email

    def _provider_connect_url(provider_key: str) -> str:
        provider = str(provider_key or "").strip().lower()
        if provider not in {"github", "microsoft", "asana"}:
            return settings_account_url
        try:
            base_path = reverse(f"{provider}_login")
        except NoReverseMatch:
            base_path = f"/accounts/{provider}/login/"
        query = urlencode({"process": "connect", "next": settings_account_url})
        return f"{base_path}?{query}"

    account_connector_cards = [
        {
            "provider": "microsoft",
            "title": "Microsoft",
            "description": "Link your Microsoft identity for third-party login and account federation.",
            "configured": microsoft_connector_configured,
            "connected": social_accounts_by_provider["microsoft"] is not None,
            "identity": _connected_identity_display(social_accounts_by_provider["microsoft"]),
            "connect_url": _provider_connect_url("microsoft"),
            "disconnect_url": reverse("disconnect_social_connector", kwargs={"provider": "microsoft"}),
        },
        {
            "provider": "github",
            "title": "GitHub",
            "description": "Link your GitHub identity for third-party login and repository-linked workflows.",
            "configured": github_connector_configured,
            "connected": social_accounts_by_provider["github"] is not None,
            "identity": _connected_identity_display(social_accounts_by_provider["github"]),
            "connect_url": _provider_connect_url("github"),
            "disconnect_url": reverse("disconnect_social_connector", kwargs={"provider": "github"}),
        },
        {
            "provider": "asana",
            "title": "Asana",
            "description": "Link your Asana identity for third-party login and project/workspace sync workflows.",
            "configured": asana_connector_configured,
            "connected": social_accounts_by_provider["asana"] is not None,
            "identity": _connected_identity_display(social_accounts_by_provider["asana"]),
            "connect_url": _provider_connect_url("asana"),
            "disconnect_url": reverse("disconnect_social_connector", kwargs={"provider": "asana"}),
        },
    ]

    admin_context = {
        "admin_setup_ready": False,
        "monitoring_enabled": True,
        "maintenance_mode": False,
        "maintenance_message": "",
        "default_model": get_alshival_default_model(),
        "support_inbox_monitoring_enabled": False,
        "microsoft_connector_configured": False,
        "microsoft_login_enabled": False,
        "github_connector_configured": False,
        "github_login_enabled": False,
        "ask_github_mcp_enabled": False,
    }
    if request.user.is_superuser:
        setup = get_or_create_setup_state()
        if setup is not None:
            admin_context.update(
                {
                    "admin_setup_ready": True,
                    "monitoring_enabled": bool(getattr(setup, "monitoring_enabled", True)),
                    "maintenance_mode": bool(getattr(setup, "maintenance_mode", False)),
                    "maintenance_message": str(getattr(setup, "maintenance_message", "") or "").strip(),
                    "default_model": str(getattr(setup, "default_model", "") or "").strip() or get_alshival_default_model(),
                    "support_inbox_monitoring_enabled": bool(getattr(setup, "support_inbox_monitoring_enabled", False)),
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
        admin_action = str(request.POST.get("admin_action") or "save").strip().lower()
        if admin_action == "restart_docker":
            restarted, detail = _trigger_docker_app_restart()
            if restarted:
                messages.success(
                    request,
                    "Docker app restart initiated. It may take a few minutes for the application to come back online.",
                )
            else:
                messages.warning(request, f"Docker restart could not be started: {detail}")
            return redirect(f"{reverse('app_settings')}?tab=admin")

        setup = get_or_create_setup_state()
        if setup is None:
            messages.warning(request, "Setup database is not ready yet. Run migrations first.")
            return redirect(f"{reverse('app_settings')}?tab=admin")

        monitoring_enabled = _post_flag(request.POST, "monitoring_enabled")
        maintenance_mode = _post_flag(request.POST, "maintenance_mode")
        support_inbox_monitoring_enabled = _post_flag(request.POST, "support_inbox_monitoring_enabled")
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
        setup.support_inbox_monitoring_enabled = support_inbox_monitoring_enabled
        setup.maintenance_message = maintenance_message
        setup.default_model = default_model
        setup.microsoft_login_enabled = microsoft_login_enabled
        setup.github_login_enabled = github_login_enabled
        setup.ask_github_mcp_enabled = ask_github_mcp_enabled
        setup.save(
            update_fields=[
                "monitoring_enabled",
                "maintenance_mode",
                "support_inbox_monitoring_enabled",
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
            "account_email": account_email,
            "account_email_locked_to_microsoft": account_email_locked_to_microsoft,
            "account_connector_cards": account_connector_cards,
            "connector_errors": connector_settings_context.get("errors", []),
            "connector_initial": connector_settings_context.get("initial", {}),
            "connector_active_connector": connector_settings_context.get("active_connector", ""),
            "microsoft_redirect_uri": str(connector_settings_context.get("microsoft_redirect_uri") or ""),
            "github_redirect_uri": str(connector_settings_context.get("github_redirect_uri") or ""),
            "asana_redirect_uri": str(connector_settings_context.get("asana_redirect_uri") or ""),
            "twilio_sms_webhook_uri": str(connector_settings_context.get("twilio_sms_webhook_uri") or ""),
            "twilio_sms_group_webhook_uri": str(connector_settings_context.get("twilio_sms_group_webhook_uri") or ""),
            "twilio_voice_webhook_uri": str(connector_settings_context.get("twilio_voice_webhook_uri") or ""),
            "twilio_voice_stream_public_uri": str(connector_settings_context.get("twilio_voice_stream_public_uri") or ""),
            "twilio_voice_stream_internal_uri": str(connector_settings_context.get("twilio_voice_stream_internal_uri") or ""),
            "web_voice_token_uri": str(connector_settings_context.get("web_voice_token_uri") or ""),
            "web_voice_log_uri": str(connector_settings_context.get("web_voice_log_uri") or ""),
            **admin_context,
        },
    )


@login_required
@require_POST
def disconnect_social_connector(request, provider: str):
    provider_key = str(provider or "").strip().lower()
    settings_account_url = f"{reverse('app_settings')}?tab=account"
    labels = {
        "microsoft": "Microsoft",
        "github": "GitHub",
        "asana": "Asana",
    }
    provider_label = labels.get(provider_key)
    if not provider_label:
        messages.warning(request, "Unknown connector provider.")
        return redirect(settings_account_url)

    try:
        provider_links = list(
            SocialAccount.objects.filter(user=request.user, provider=provider_key).order_by("id")
        )
        total_links = int(
            SocialAccount.objects.filter(user=request.user).count()
        )
    except (OperationalError, ProgrammingError):
        messages.warning(request, "Connector records are unavailable. Run migrations first.")
        return redirect(settings_account_url)
    except Exception:
        messages.warning(request, "Connector update failed. Please try again.")
        return redirect(settings_account_url)

    if not provider_links:
        messages.info(request, f"{provider_label} is not connected to your account.")
        return redirect(settings_account_url)

    if (not request.user.has_usable_password()) and total_links <= len(provider_links):
        messages.warning(
            request,
            "Set a password before disconnecting your last sign-in connector.",
        )
        return redirect(settings_account_url)

    try:
        SocialAccount.objects.filter(
            user=request.user,
            id__in=[int(account.id) for account in provider_links],
        ).delete()
    except Exception:
        messages.warning(request, "Unable to disconnect connector right now.")
        return redirect(settings_account_url)

    messages.success(request, f"{provider_label} disconnected from your account.")
    return redirect(settings_account_url)


@superuser_required
def connector_settings(request):
    known_connectors = {"openai", "microsoft", "github", "asana", "twilio"}
    connector = str(request.GET.get("connector") or "").strip().lower()
    redirect_target = f"{reverse('app_settings')}?tab=connectors"
    if connector in known_connectors:
        redirect_target = f"{redirect_target}&connector={connector}"
    return redirect(redirect_target)


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
        'available_invite_methods': _visible_invite_signup_methods(),
        'invite_default_method_keys': list(_invite_enabled_signup_methods()),
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
def team_directory_invite_preview(request):
    username = (request.POST.get('username') or '').strip()
    email = (request.POST.get('email') or '').strip().lower()
    phone_number = _normalize_phone(request.POST.get("phone_number") or "")
    invite_channel = (request.POST.get("invite_channel") or UserInvite.CHANNEL_EMAIL).strip().lower()
    invite_note = str(request.POST.get("invite_note") or "").strip()
    if len(invite_note) > 2000:
        invite_note = invite_note[:2000]
    team_names = _normalize_team_names(request.POST.getlist("team_names"))
    feature_keys = _normalize_feature_keys(request.POST.getlist("feature_keys"))
    signup_methods = _normalize_invite_signup_methods(
        request.POST.getlist("signup_methods"),
        fallback_to_local=False,
    )
    if invite_channel not in {UserInvite.CHANNEL_EMAIL, UserInvite.CHANNEL_SMS}:
        invite_channel = UserInvite.CHANNEL_EMAIL
    if not signup_methods:
        signup_methods = _normalize_invite_signup_methods(["local"], fallback_to_local=True)

    preview_url = request.build_absolute_uri(reverse("accept_user_invite", kwargs={"token": "generated-on-send"}))
    allowed_labels = ", ".join(_invite_method_label(item) for item in signup_methods) or "Local account"
    expiry_text = "Set when invite is sent."
    generated_message = _generate_invite_delivery_message_with_agent(
        actor=request.user,
        invite_channel=invite_channel,
        invite_url=preview_url,
        allowed_labels=allowed_labels,
        expiry_text=expiry_text,
        invite_note=invite_note,
        invited_username=username,
        invited_email=email,
        invited_phone=phone_number,
        team_names=team_names,
        feature_keys=feature_keys,
        signup_methods=signup_methods,
    )
    preview_message = str(generated_message or "").strip() or _default_invite_delivery_message(
        invite_channel=invite_channel,
        invite_url=preview_url,
        allowed_labels=allowed_labels,
        expiry_text=expiry_text,
        invite_note=invite_note,
    )
    preview_subject = ""
    preview_text = str(preview_message or "").strip()
    preview_html = ""
    if invite_channel == UserInvite.CHANNEL_EMAIL:
        preview_subject = "You are invited to Alshival"
        preview_text, preview_html = _decorate_invite_email_message(
            message=preview_message,
            invite_url=preview_url,
            invite_token="generated-on-send",
            signup_methods=signup_methods,
            invited_email=email,
        )
        if str(preview_html or "").strip():
            preview_subject, preview_text, preview_html = build_alshival_branded_email_from_html(
                preview_subject,
                preview_text,
                preview_html,
            )
        else:
            preview_subject, preview_text, preview_html = build_alshival_branded_email(
                preview_subject,
                preview_text,
            )
        preview_message = preview_html or preview_text
    else:
        preview_message = preview_text
    return JsonResponse(
        {
            "ok": True,
            "channel": invite_channel,
            "subject": preview_subject,
            "message": preview_message,
            "message_text": preview_text,
            "message_html": preview_html,
            "message_is_html": bool(invite_channel == UserInvite.CHANNEL_EMAIL and bool(str(preview_html or "").strip())),
            "used_ai": bool(str(generated_message or "").strip()),
        }
    )


@require_POST
@superuser_required
def team_directory_invite_user(request):
    username = (request.POST.get('username') or '').strip()
    email = (request.POST.get('email') or '').strip().lower()
    phone_number = _normalize_phone(request.POST.get("phone_number") or "")
    invite_channel = (request.POST.get("invite_channel") or UserInvite.CHANNEL_EMAIL).strip().lower()
    invite_note = str(request.POST.get("invite_note") or "").strip()
    if len(invite_note) > 2000:
        invite_note = invite_note[:2000]
    make_staff = _post_flag(request, 'is_staff')
    make_superuser = _post_flag(request, 'is_superuser')
    is_active = _post_flag(request, 'is_active')
    team_names = _normalize_team_names(request.POST.getlist("team_names"))
    feature_keys = _normalize_feature_keys(request.POST.getlist("feature_keys"))
    signup_methods = _normalize_invite_signup_methods(
        request.POST.getlist("signup_methods"),
        fallback_to_local=False,
    )

    if not email and not phone_number:
        return _redirect_team_directory(tab='users', status='invite_required_fields')
    if not signup_methods:
        return _redirect_team_directory(tab='users', status='invite_method_required')
    if invite_channel not in {UserInvite.CHANNEL_EMAIL, UserInvite.CHANNEL_SMS}:
        return _redirect_team_directory(tab='users', status='invite_channel_required')
    if invite_channel == UserInvite.CHANNEL_EMAIL and not email:
        return _redirect_team_directory(tab='users', status='invite_email_required')
    if invite_channel == UserInvite.CHANNEL_SMS and not phone_number:
        return _redirect_team_directory(tab='users', status='invite_phone_required')
    if invite_channel == UserInvite.CHANNEL_SMS and not is_twilio_configured():
        return _redirect_team_directory(tab='users', status='invite_sms_not_configured')

    invite = UserInvite.objects.create(
        token=_invite_token(),
        invited_username=username,
        invited_email=email,
        invited_phone=phone_number,
        delivery_channel=invite_channel,
        sent_to=email if invite_channel == UserInvite.CHANNEL_EMAIL else phone_number,
        allowed_signup_methods=signup_methods,
        team_names=team_names,
        feature_keys=feature_keys,
        is_active=is_active,
        is_staff=make_staff,
        is_superuser=make_superuser,
        created_by=request.user,
        expires_at=_invite_expiry_datetime(),
    )

    invite_url = _invite_absolute_url(request, invite.token)
    allowed_labels = ", ".join(_invite_method_label(item) for item in signup_methods)
    expiry_text = invite.expires_at.astimezone().strftime("%b %d, %Y %I:%M %p %Z")
    generated_message = _generate_invite_delivery_message_with_agent(
        actor=request.user,
        invite_channel=invite_channel,
        invite_url=invite_url,
        allowed_labels=allowed_labels,
        expiry_text=expiry_text,
        invite_note=invite_note,
        invited_username=username,
        invited_email=email,
        invited_phone=phone_number,
        team_names=team_names,
        feature_keys=feature_keys,
        signup_methods=signup_methods,
    )
    if invite_channel == UserInvite.CHANNEL_EMAIL:
        subject = "You are invited to Alshival"
        base_email_content = str(generated_message or "").strip() or _default_invite_delivery_message(
            invite_channel=invite_channel,
            invite_url=invite_url,
            allowed_labels=allowed_labels,
            expiry_text=expiry_text,
            invite_note=invite_note,
        )
        message, generated_email_html = _decorate_invite_email_message(
            message=base_email_content,
            invite_url=invite_url,
            invite_token=invite.token,
            signup_methods=signup_methods,
            invited_email=email,
        )
        if not str(message or "").strip():
            message = _default_invite_delivery_message(
                invite_channel=invite_channel,
                invite_url=invite_url,
                allowed_labels=allowed_labels,
                expiry_text=expiry_text,
                invite_note=invite_note,
            )
        sent_ok, _send_error = _send_invite_email(
            recipient_email=email,
            subject=subject,
            message=message,
            message_html=generated_email_html,
        )
    else:
        message = str(generated_message or "").strip() or _default_invite_delivery_message(
            invite_channel=invite_channel,
            invite_url=invite_url,
            allowed_labels=allowed_labels,
            expiry_text=expiry_text,
            invite_note=invite_note,
        )
        sent_ok, _send_error = _send_invite_sms(
            to_number=phone_number,
            message=message,
        )

    if not sent_ok:
        send_error = str(_send_error or "").strip()
        if send_error and bool(getattr(request.user, "is_superuser", False)):
            messages.warning(request, f"Invite delivery detail: {send_error}")
        return _redirect_team_directory(tab='users', status='invite_send_failed')

    return _redirect_team_directory(tab='users', status='invite_sent')


def _social_login_path(provider: str) -> str:
    resolved_provider = str(provider or "").strip().lower()
    if not resolved_provider:
        return "/accounts/login/"
    try:
        return reverse(f"{resolved_provider}_login")
    except NoReverseMatch:
        return f"/accounts/{resolved_provider}/login/"


def _lookup_user_invite(token: str) -> UserInvite | None:
    resolved_token = str(token or "").strip()
    if not resolved_token:
        return None
    try:
        return (
            UserInvite.objects.select_related("created_by", "accepted_by")
            .filter(token=resolved_token)
            .order_by("-id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        return None
    except Exception:
        return None


def _invite_expired(invite: UserInvite) -> bool:
    expires_at = getattr(invite, "expires_at", None)
    if not isinstance(expires_at, datetime):
        return True
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at <= datetime.now(timezone.utc)


def _invite_allowed_methods(invite: UserInvite) -> list[str]:
    raw_methods = invite.allowed_signup_methods if isinstance(invite.allowed_signup_methods, list) else []
    selected = _normalize_invite_signup_methods(
        [str(item or "") for item in raw_methods],
        fallback_to_local=False,
    )
    if not selected:
        selected = _normalize_invite_signup_methods(["local"], fallback_to_local=True)
    return selected


def _masked_target(invite: UserInvite) -> str:
    email = str(invite.invited_email or "").strip().lower()
    if email and "@" in email:
        user_part, domain_part = email.split("@", 1)
        if len(user_part) <= 2:
            masked_user = f"{user_part[:1]}*"
        else:
            masked_user = f"{user_part[:2]}***"
        return f"{masked_user}@{domain_part}"
    phone = str(invite.invited_phone or "").strip()
    if phone and len(phone) > 4:
        return f"***{phone[-4:]}"
    return email or phone or str(invite.invited_username or "").strip() or "this invite"


def _invite_template_context(*, invite: UserInvite | None, state: str) -> dict[str, object]:
    context: dict[str, object] = {
        "invite": invite,
        "invite_state": state,
        "invite_target": _masked_target(invite) if invite is not None else "",
        "invite_method_options": [],
        "invite_local_allowed": False,
        "local_login_url": "",
        "local_signup_url": "",
    }
    if invite is None:
        return context
    if state not in {"active", "method_not_allowed"}:
        return context
    methods = _invite_allowed_methods(invite)
    local_allowed = "local" in methods
    context["invite_local_allowed"] = local_allowed
    complete_path = reverse("complete_user_invite", kwargs={"token": invite.token})
    if local_allowed:
        local_login_qs = urlencode({"next": complete_path})
        context["local_login_url"] = f"{reverse('account_login')}?{local_login_qs}"
        signup_params = {"next": complete_path}
        if str(invite.invited_email or "").strip():
            signup_params["email"] = str(invite.invited_email or "").strip().lower()
        context["local_signup_url"] = f"{reverse('account_signup')}?{urlencode(signup_params)}"

    method_options: list[dict[str, str]] = []
    for method in methods:
        if method == "local":
            continue
        login_base = _social_login_path(method)
        login_url = f"{login_base}?{urlencode({'process': 'login', 'next': complete_path})}"
        method_options.append(
            {
                "key": method,
                "label": _invite_method_label(method),
                "url": login_url,
            }
        )
    context["invite_method_options"] = method_options
    return context


def accept_user_invite(request, token: str):
    invite = _lookup_user_invite(token)
    if invite is None:
        return render(
            request,
            "pages/invite_accept.html",
            _invite_template_context(invite=None, state="invalid"),
            status=404,
        )
    if invite.accepted_at is not None:
        return render(
            request,
            "pages/invite_accept.html",
            _invite_template_context(invite=invite, state="claimed"),
            status=410,
        )
    if _invite_expired(invite):
        return render(
            request,
            "pages/invite_accept.html",
            _invite_template_context(invite=invite, state="expired"),
            status=410,
        )
    if request.user.is_authenticated:
        return redirect("complete_user_invite", token=invite.token)

    return render(
        request,
        "pages/invite_accept.html",
        _invite_template_context(invite=invite, state="active"),
    )


@login_required
def complete_user_invite(request, token: str):
    invite = _lookup_user_invite(token)
    if invite is None or invite.accepted_at is not None or _invite_expired(invite):
        messages.warning(request, _TEAM_DIRECTORY_STATUS["invite_invalid_or_expired"][0])
        return redirect("home")

    allowed_methods = _invite_allowed_methods(invite)
    matched_methods = _invite_user_matched_methods(request.user, allowed_methods=allowed_methods)
    if not matched_methods:
        return render(
            request,
            "pages/invite_accept.html",
            _invite_template_context(invite=invite, state="method_not_allowed"),
            status=403,
        )

    expected_email = str(invite.invited_email or "").strip().lower()
    expected_phone = _normalize_phone(str(invite.invited_phone or ""))
    user_email = str(getattr(request.user, "email", "") or "").strip().lower()
    user_phone = _invite_phone_for_user(request.user)
    adopt_invite_phone = False

    identity_match = False
    if expected_email and user_email == expected_email:
        identity_match = True
    if expected_phone and user_phone == expected_phone:
        identity_match = True
    if (
        not identity_match
        and expected_phone
        and not expected_email
        and str(invite.delivery_channel or "").strip().lower() == UserInvite.CHANNEL_SMS
        and not user_phone
    ):
        identity_match = True
        adopt_invite_phone = True
    if not expected_email and not expected_phone:
        identity_match = True

    if not identity_match:
        return render(
            request,
            "pages/invite_accept.html",
            _invite_template_context(invite=invite, state="mismatch"),
            status=403,
        )

    with transaction.atomic():
        actor = invite.created_by if invite.created_by_id else request.user
        feature_keys = _normalize_feature_keys(
            [str(item or "").strip().lower() for item in (invite.feature_keys if isinstance(invite.feature_keys, list) else [])]
        )
        team_names = _normalize_team_names(
            [str(item or "").strip() for item in (invite.team_names if isinstance(invite.team_names, list) else [])]
        )

        update_fields: list[str] = []
        resolved_is_staff = True if bool(invite.is_superuser) else bool(invite.is_staff)
        if bool(request.user.is_staff) != resolved_is_staff:
            request.user.is_staff = resolved_is_staff
            update_fields.append("is_staff")
        if bool(request.user.is_superuser) != bool(invite.is_superuser):
            request.user.is_superuser = bool(invite.is_superuser)
            update_fields.append("is_superuser")
        if bool(request.user.is_active) != bool(invite.is_active):
            request.user.is_active = bool(invite.is_active)
            update_fields.append("is_active")
        if update_fields:
            request.user.save(update_fields=update_fields)

        if adopt_invite_phone and expected_phone:
            UserNotificationSettings.objects.update_or_create(
                user=request.user,
                defaults={"phone_number": expected_phone},
            )

        groups = list(Group.objects.filter(name__in=team_names))
        request.user.groups.set(groups)
        _sync_user_feature_access(user=request.user, feature_keys=feature_keys, actor=actor)

        invite.accepted_by = request.user
        invite.accepted_at = datetime.now(timezone.utc)
        invite.save(update_fields=["accepted_by", "accepted_at", "updated_at"])

    messages.success(request, _TEAM_DIRECTORY_STATUS["invite_applied"][0])
    return redirect("home")


def _build_wiki_page_listing_context(
    *,
    actor,
    wiki_scope: str,
    wiki_resource_uuid: str,
    wiki_team_id: str = "",
    requested_page_raw: str,
) -> dict[str, object]:
    requested_path = _normalize_wiki_path(requested_page_raw, "")
    member_teams = _ssh_team_choices_for_user(actor)
    pages = list(
        _wiki_accessible_queryset(
            actor,
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        ).order_by("path", "title")
    )

    wiki_pages: list[dict[str, object]] = []
    for item in pages:
        item_scope = str(item.scope or _WIKI_SCOPE_WORKSPACE).strip().lower()
        item_scope_key = str(item.resource_uuid or "").strip()
        item_resource_uuid = _normalize_resource_uuid(item_scope_key) if item_scope == _WIKI_SCOPE_RESOURCE else ""
        item_team_id = _normalize_team_id(item_scope_key) if item_scope == _WIKI_SCOPE_TEAM else ""
        item_scope_name = str(item.resource_name or "").strip()
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
                "can_edit": _can_edit_wiki_page(actor=actor, page=item),
                "scope": item_scope,
                "resource_uuid": item_resource_uuid,
                "resource_name": item_scope_name if item_scope == _WIKI_SCOPE_RESOURCE else "",
                "team_id": item_team_id,
                "team_name": item_scope_name if item_scope == _WIKI_SCOPE_TEAM else "",
                "updated_display": _format_display_time(item.updated_at.isoformat() if getattr(item, "updated_at", None) else ""),
            }
        )

    selected_page = None
    if requested_path:
        selected_page = next((item for item in pages if item.path == requested_path), None)
    if selected_page is None and pages:
        selected_page = pages[0]

    missing_status_code = ""
    if requested_page_raw and selected_page is None:
        normalized_requested = _normalize_wiki_path(requested_page_raw, "")
        scope_key = wiki_resource_uuid if wiki_scope == _WIKI_SCOPE_RESOURCE else (wiki_team_id if wiki_scope == _WIKI_SCOPE_TEAM else "")
        if normalized_requested and WikiPage.objects.filter(
            path=normalized_requested,
            scope=wiki_scope,
            resource_uuid=scope_key,
        ).exists():
            missing_status_code = "wiki_no_access"
        else:
            missing_status_code = "wiki_page_not_found"

    selected_page_payload: dict[str, object] = {}
    selected_page_html_fallback = ""
    if selected_page is not None:
        selected_page_scope = str(selected_page.scope or _WIKI_SCOPE_WORKSPACE).strip().lower()
        selected_scope_key = str(selected_page.resource_uuid or "").strip()
        selected_resource_uuid = _normalize_resource_uuid(selected_scope_key) if selected_page_scope == _WIKI_SCOPE_RESOURCE else ""
        selected_team_id = _normalize_team_id(selected_scope_key) if selected_page_scope == _WIKI_SCOPE_TEAM else ""
        selected_scope_name = str(selected_page.resource_name or "").strip()
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
            "scope": selected_page_scope,
            "resource_uuid": selected_resource_uuid,
            "resource_name": selected_scope_name if selected_page_scope == _WIKI_SCOPE_RESOURCE else "",
            "team_id": selected_team_id,
            "team_name": selected_scope_name if selected_page_scope == _WIKI_SCOPE_TEAM else "",
            "team_names": selected_team_names,
            "updated_display": _format_display_time(selected_page.updated_at.isoformat() if getattr(selected_page, "updated_at", None) else ""),
            "created_display": _format_display_time(selected_page.created_at.isoformat() if getattr(selected_page, "created_at", None) else ""),
            "can_edit": _can_edit_wiki_page(actor=actor, page=selected_page),
        }
        selected_page_html_fallback = render_markdown_fallback(selected_page.body_markdown)

    return {
        "wiki_pages": wiki_pages,
        "member_teams": member_teams,
        "selected_page": selected_page,
        "selected_page_payload": selected_page_payload,
        "selected_page_html_fallback": selected_page_html_fallback,
        "missing_status_code": missing_status_code,
    }


def _apply_wiki_action_urls(
    *,
    listing_context: dict[str, object],
    editor_url_builder,
    delete_url_builder,
) -> None:
    wiki_pages = list(listing_context.get("wiki_pages") or [])
    for item in wiki_pages:
        page_id = int(item.get("id") or 0)
        if page_id <= 0:
            continue
        item["edit_url"] = editor_url_builder(page_id)
        item["delete_url"] = delete_url_builder(page_id)

    selected_page_payload = listing_context.get("selected_page_payload")
    if isinstance(selected_page_payload, dict):
        selected_page_id = int(selected_page_payload.get("id") or 0)
        if selected_page_id > 0:
            selected_page_payload["edit_url"] = editor_url_builder(selected_page_id)
            selected_page_payload["delete_url"] = delete_url_builder(selected_page_id)


@login_required
def wiki(request):
    _ensure_default_sdk_workspace_wiki_page(actor=request.user)
    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)

    scope_context = _resolve_wiki_scope_context(
        actor=request.user,
        raw_scope=request.GET.get("scope") or "",
        raw_resource_uuid=request.GET.get("resource_uuid") or "",
        raw_team_id=request.GET.get("team_id") or "",
    )
    wiki_scope = str(scope_context["scope"])
    wiki_resource_uuid = str(scope_context["resource_uuid"])
    wiki_resource_name = str(scope_context["resource_name"])
    wiki_resource_options = list(scope_context["resource_options"])
    wiki_team_id = str(scope_context["team_id"])
    wiki_team_name = str(scope_context["team_name"])
    wiki_team_options = list(scope_context["team_options"])
    scope_status_code = str(scope_context["status_code"] or "")
    if not status_message and scope_status_code:
        status_message, status_tone = _wiki_status_context(scope_status_code)

    requested_page_raw = (request.GET.get("page") or "").strip()
    listing_context = _build_wiki_page_listing_context(
        actor=request.user,
        wiki_scope=wiki_scope,
        wiki_resource_uuid=wiki_resource_uuid,
        wiki_team_id=wiki_team_id,
        requested_page_raw=requested_page_raw,
    )
    missing_status_code = str(listing_context["missing_status_code"] or "")
    if not status_message and missing_status_code:
        status_message, status_tone = _wiki_status_context(missing_status_code)

    wiki_context_query = urlencode(
        _wiki_query_params(
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    )
    wiki_context_with_page_prefix = f"{wiki_context_query}&" if wiki_context_query else ""
    wiki_editor_new_url = reverse("wiki_editor_new")
    if wiki_context_query:
        wiki_editor_new_url = f"{wiki_editor_new_url}?{wiki_context_query}"

    def _editor_url_builder(page_id: int) -> str:
        base = reverse("wiki_editor", kwargs={"page_id": int(page_id)})
        if wiki_context_query:
            return f"{base}?{wiki_context_query}"
        return base

    def _delete_url_builder(page_id: int) -> str:
        return reverse("wiki_delete_page", kwargs={"page_id": int(page_id)})

    _apply_wiki_action_urls(
        listing_context=listing_context,
        editor_url_builder=_editor_url_builder,
        delete_url_builder=_delete_url_builder,
    )

    return render(
        request,
        "pages/wiki.html",
        {
            **listing_context,
            "wiki_scope": wiki_scope,
            "wiki_scope_label": (
                "Resource Wiki"
                if wiki_scope == _WIKI_SCOPE_RESOURCE
                else ("Team Wiki" if wiki_scope == _WIKI_SCOPE_TEAM else "Workspace Wiki")
            ),
            "wiki_is_resource_scope": wiki_scope == _WIKI_SCOPE_RESOURCE,
            "wiki_is_team_scope": wiki_scope == _WIKI_SCOPE_TEAM,
            "wiki_resource_uuid": wiki_resource_uuid,
            "wiki_resource_name": wiki_resource_name,
            "wiki_resource_options": wiki_resource_options,
            "wiki_team_id": wiki_team_id,
            "wiki_team_name": wiki_team_name,
            "wiki_team_options": wiki_team_options,
            "wiki_context_query": wiki_context_query,
            "wiki_context_with_page_prefix": wiki_context_with_page_prefix,
            "wiki_page_base_url": reverse("wiki"),
            "wiki_editor_new_url": wiki_editor_new_url,
            "wiki_resource_shell_url": "",
            "wiki_resource_shell_label": "",
            "wiki_scope_locked": False,
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
def wiki_editor_new(request):
    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)

    scope_context = _resolve_wiki_scope_context(
        actor=request.user,
        raw_scope=request.GET.get("scope") or "",
        raw_resource_uuid=request.GET.get("resource_uuid") or "",
        raw_team_id=request.GET.get("team_id") or "",
    )
    wiki_scope = str(scope_context["scope"])
    wiki_resource_uuid = str(scope_context["resource_uuid"])
    wiki_resource_name = str(scope_context["resource_name"])
    wiki_resource_options = list(scope_context["resource_options"])
    wiki_team_id = str(scope_context["team_id"])
    wiki_team_name = str(scope_context["team_name"])
    wiki_team_options = list(scope_context["team_options"])
    scope_status_code = str(scope_context["status_code"] or "")
    if not status_message and scope_status_code:
        status_message, status_tone = _wiki_status_context(scope_status_code)

    editor_context_query = urlencode(
        _wiki_query_params(
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    )
    wiki_back_url = reverse("wiki")
    if editor_context_query:
        wiki_back_url = f"{wiki_back_url}?{editor_context_query}"
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
                "scope": wiki_scope,
                "resource_uuid": wiki_resource_uuid,
                "resource_name": wiki_resource_name,
                "team_id": wiki_team_id,
                "team_name": wiki_team_name,
            },
            "member_teams": member_teams,
            "wiki_scope": wiki_scope,
            "wiki_is_resource_scope": wiki_scope == _WIKI_SCOPE_RESOURCE,
            "wiki_is_team_scope": wiki_scope == _WIKI_SCOPE_TEAM,
            "wiki_resource_uuid": wiki_resource_uuid,
            "wiki_resource_name": wiki_resource_name,
            "wiki_resource_options": wiki_resource_options,
            "wiki_team_id": wiki_team_id,
            "wiki_team_name": wiki_team_name,
            "wiki_team_options": wiki_team_options,
            "editor_context_query": editor_context_query,
            "wiki_scope_locked": False,
            "wiki_back_url": wiki_back_url,
            "wiki_create_page_url": reverse("wiki_create_page"),
            "wiki_update_page_url": "",
            "wiki_delete_page_url": "",
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
def wiki_editor(request, page_id: int):
    page = get_object_or_404(WikiPage.objects.prefetch_related("team_access"), id=page_id)
    if not _can_edit_wiki_page(actor=request.user, page=page):
        return _redirect_wiki(
            status="wiki_no_access",
            page_path=page.path,
            scope=page.scope,
            resource_uuid=page.resource_uuid,
        )

    page_scope = _normalize_wiki_scope(page.scope)
    page_scope_key = str(page.resource_uuid or "").strip()
    page_resource_uuid = _normalize_resource_uuid(page_scope_key) if page_scope == _WIKI_SCOPE_RESOURCE else ""
    page_team_id = _normalize_team_id(page_scope_key) if page_scope == _WIKI_SCOPE_TEAM else ""
    if page_scope == _WIKI_SCOPE_RESOURCE and page_resource_uuid:
        if not user_can_access_resource(user=request.user, resource_uuid=page_resource_uuid):
            return _redirect_wiki(
                status="wiki_resource_no_access",
                scope=page_scope,
                resource_uuid=page_resource_uuid,
            )
    if page_scope == _WIKI_SCOPE_TEAM and page_team_id:
        if not _user_can_access_team(actor=request.user, team_id=page_team_id):
            return _redirect_wiki(
                status="wiki_team_no_access",
                scope=page_scope,
                team_id=page_team_id,
            )

    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)
    wiki_resource_options = _wiki_resource_options_for_user(request.user)
    wiki_resource_lookup = {item["resource_uuid"]: item["resource_name"] for item in wiki_resource_options}
    wiki_team_options = _wiki_team_options_for_user(request.user)
    wiki_team_lookup = {item["team_id"]: item["team_name"] for item in wiki_team_options}
    page_resource_name = str(page.resource_name or "").strip()
    page_team_name = str(page.resource_name or "").strip()
    if page_scope == _WIKI_SCOPE_RESOURCE and page_resource_uuid:
        if not page_resource_name:
            page_resource_name = _wiki_resource_name_for_user(
                actor=request.user,
                resource_uuid=page_resource_uuid,
                options_lookup=wiki_resource_lookup,
            )
        if page_resource_uuid not in wiki_resource_lookup:
            wiki_resource_options.append(
                {
                    "resource_uuid": page_resource_uuid,
                    "resource_name": page_resource_name or page_resource_uuid,
                }
            )
            wiki_resource_options.sort(key=lambda item: (item["resource_name"].lower(), item["resource_uuid"]))
    if page_scope == _WIKI_SCOPE_TEAM and page_team_id:
        if not page_team_name:
            page_team_name = _wiki_team_name_for_user(
                actor=request.user,
                team_id=page_team_id,
                options_lookup=wiki_team_lookup,
            )
        if page_team_id not in wiki_team_lookup:
            wiki_team_options.append(
                {
                    "team_id": page_team_id,
                    "team_name": page_team_name or page_team_id,
                }
            )
            wiki_team_options.sort(key=lambda item: (item["team_name"].lower(), item["team_id"]))

    editor_context_query = urlencode(
        _wiki_query_params(
            scope=page_scope,
            resource_uuid=page_resource_uuid,
            team_id=page_team_id,
        )
    )
    back_query = urlencode(
        _wiki_query_params(
            scope=page_scope,
            resource_uuid=page_resource_uuid,
            team_id=page_team_id,
            page_path=str(page.path or ""),
        )
    )
    wiki_back_url = reverse("wiki")
    if back_query:
        wiki_back_url = f"{wiki_back_url}?{back_query}"
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
                "scope": page_scope,
                "resource_uuid": page_resource_uuid,
                "resource_name": page_resource_name,
                "team_id": page_team_id,
                "team_name": page_team_name,
                "updated_display": _format_display_time(page.updated_at.isoformat() if getattr(page, "updated_at", None) else ""),
                "created_display": _format_display_time(page.created_at.isoformat() if getattr(page, "created_at", None) else ""),
            },
            "member_teams": member_teams,
            "wiki_scope": page_scope,
            "wiki_is_resource_scope": page_scope == _WIKI_SCOPE_RESOURCE,
            "wiki_is_team_scope": page_scope == _WIKI_SCOPE_TEAM,
            "wiki_resource_uuid": page_resource_uuid,
            "wiki_resource_name": page_resource_name,
            "wiki_resource_options": wiki_resource_options,
            "wiki_team_id": page_team_id,
            "wiki_team_name": page_team_name,
            "wiki_team_options": wiki_team_options,
            "editor_context_query": editor_context_query,
            "wiki_scope_locked": False,
            "wiki_back_url": wiki_back_url,
            "wiki_create_page_url": reverse("wiki_create_page"),
            "wiki_update_page_url": reverse("wiki_update_page", kwargs={"page_id": int(page.id)}),
            "wiki_delete_page_url": reverse("wiki_delete_page", kwargs={"page_id": int(page.id)}),
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
@require_POST
def wiki_create_page(request):
    wiki_scope, wiki_resource_uuid, wiki_team_id = _normalize_wiki_scope_target(
        raw_scope=request.POST.get("wiki_scope") or "",
        raw_resource_uuid=request.POST.get("resource_uuid") or "",
        raw_team_id=request.POST.get("team_id") or "",
    )
    wiki_scope_key = ""
    wiki_scope_name = ""
    if wiki_scope == _WIKI_SCOPE_RESOURCE:
        if not wiki_resource_uuid:
            return _redirect_wiki_editor_new(
                status="wiki_resource_required",
                scope=wiki_scope,
                resource_uuid=wiki_resource_uuid,
                team_id=wiki_team_id,
            )
        if not user_can_access_resource(user=request.user, resource_uuid=wiki_resource_uuid):
            return _redirect_wiki_editor_new(
                status="wiki_resource_no_access",
                scope=wiki_scope,
                resource_uuid=wiki_resource_uuid,
                team_id=wiki_team_id,
            )
        wiki_scope_key = wiki_resource_uuid
        wiki_scope_name = _wiki_resource_name_for_user(
            actor=request.user,
            resource_uuid=wiki_resource_uuid,
            options_lookup={},
        )
    elif wiki_scope == _WIKI_SCOPE_TEAM:
        if not wiki_team_id:
            return _redirect_wiki_editor_new(
                status="wiki_team_required",
                scope=wiki_scope,
                team_id=wiki_team_id,
            )
        if not _user_can_access_team(actor=request.user, team_id=wiki_team_id):
            return _redirect_wiki_editor_new(
                status="wiki_team_no_access",
                scope=wiki_scope,
                team_id=wiki_team_id,
            )
        wiki_scope_key = wiki_team_id
        wiki_scope_name = _wiki_team_name_for_user(
            actor=request.user,
            team_id=wiki_team_id,
            options_lookup={},
        )

    body_markdown = str(request.POST.get("body_markdown") or "").replace("\r\n", "\n").replace("\r", "\n")
    title = _extract_wiki_title_from_markdown(body_markdown)
    path = _normalize_wiki_path(request.POST.get("path") or "", title)
    team_names = _normalize_wiki_team_names(request.user, request.POST.getlist("team_names"))
    if wiki_scope == _WIKI_SCOPE_TEAM:
        team_names = [wiki_scope_name] if wiki_scope_name else []
    save_intent = (request.POST.get("save_intent") or "publish").strip().lower()
    is_draft = save_intent != "publish"

    if not title:
        return _redirect_wiki_editor_new(
            status="wiki_title_required",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    if not path:
        return _redirect_wiki_editor_new(
            status="wiki_path_required",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    if len(path) > 220:
        return _redirect_wiki_editor_new(
            status="wiki_path_invalid",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    if WikiPage.objects.filter(
        scope=wiki_scope,
        resource_uuid=wiki_scope_key,
        path__iexact=path,
    ).exists():
        return _redirect_wiki_editor_new(
            status="wiki_path_exists",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )

    with transaction.atomic():
        page = WikiPage.objects.create(
            scope=wiki_scope,
            resource_uuid=wiki_scope_key,
            resource_name=wiki_scope_name,
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

    _sync_global_workspace_wiki_kb_page(page=page)
    if wiki_scope == _WIKI_SCOPE_RESOURCE and wiki_resource_uuid:
        _upsert_resource_kb_after_wiki_mutation(
            actor=request.user,
            resource_uuid=wiki_resource_uuid,
        )

    if is_draft:
        return _redirect_wiki_editor(
            page_id=page.id,
            status="wiki_draft_saved",
            scope=page.scope,
            resource_uuid=page.resource_uuid if page.scope == _WIKI_SCOPE_RESOURCE else "",
            team_id=page.resource_uuid if page.scope == _WIKI_SCOPE_TEAM else "",
        )
    return _redirect_wiki(
        status="wiki_page_created",
        page_path=page.path,
        scope=page.scope,
        resource_uuid=page.resource_uuid if page.scope == _WIKI_SCOPE_RESOURCE else "",
        team_id=page.resource_uuid if page.scope == _WIKI_SCOPE_TEAM else "",
    )


@login_required
@require_POST
def wiki_update_page(request, page_id: int):
    page = get_object_or_404(WikiPage.objects.prefetch_related("team_access"), id=page_id)
    if not _can_edit_wiki_page(actor=request.user, page=page):
        return _redirect_wiki(
            status="wiki_no_access",
            page_path=page.path,
            scope=page.scope,
            resource_uuid=page.resource_uuid,
        )

    wiki_scope, wiki_resource_uuid, wiki_team_id = _normalize_wiki_scope_target(
        raw_scope=request.POST.get("wiki_scope") or page.scope,
        raw_resource_uuid=request.POST.get("resource_uuid") or page.resource_uuid,
        raw_team_id=request.POST.get("team_id") or page.resource_uuid,
    )
    wiki_scope_key = ""
    wiki_scope_name = ""
    if wiki_scope == _WIKI_SCOPE_RESOURCE:
        if not wiki_resource_uuid:
            return _redirect_wiki_editor(
                page_id=page.id,
                status="wiki_resource_required",
                scope=wiki_scope,
                resource_uuid=wiki_resource_uuid,
                team_id=wiki_team_id,
            )
        if not user_can_access_resource(user=request.user, resource_uuid=wiki_resource_uuid):
            return _redirect_wiki_editor(
                page_id=page.id,
                status="wiki_resource_no_access",
                scope=wiki_scope,
                resource_uuid=wiki_resource_uuid,
                team_id=wiki_team_id,
            )
        wiki_scope_key = wiki_resource_uuid
        wiki_scope_name = _wiki_resource_name_for_user(
            actor=request.user,
            resource_uuid=wiki_resource_uuid,
            options_lookup={},
        )
    elif wiki_scope == _WIKI_SCOPE_TEAM:
        if not wiki_team_id:
            return _redirect_wiki_editor(
                page_id=page.id,
                status="wiki_team_required",
                scope=wiki_scope,
                team_id=wiki_team_id,
            )
        if not _user_can_access_team(actor=request.user, team_id=wiki_team_id):
            return _redirect_wiki_editor(
                page_id=page.id,
                status="wiki_team_no_access",
                scope=wiki_scope,
                team_id=wiki_team_id,
            )
        wiki_scope_key = wiki_team_id
        wiki_scope_name = _wiki_team_name_for_user(
            actor=request.user,
            team_id=wiki_team_id,
            options_lookup={},
        )

    body_markdown = str(request.POST.get("body_markdown") or "").replace("\r\n", "\n").replace("\r", "\n")
    title = _extract_wiki_title_from_markdown(body_markdown)
    path = _normalize_wiki_path(request.POST.get("path") or "", title)
    team_names = _normalize_wiki_team_names(request.user, request.POST.getlist("team_names"))
    if wiki_scope == _WIKI_SCOPE_TEAM:
        team_names = [wiki_scope_name] if wiki_scope_name else []
    save_intent = (request.POST.get("save_intent") or "publish").strip().lower()
    is_draft = save_intent != "publish"

    if not title:
        return _redirect_wiki_editor(
            page_id=page.id,
            status="wiki_title_required",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    if not path:
        return _redirect_wiki_editor(
            page_id=page.id,
            status="wiki_path_required",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    if len(path) > 220:
        return _redirect_wiki_editor(
            page_id=page.id,
            status="wiki_path_invalid",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )
    if WikiPage.objects.exclude(id=page.id).filter(
        scope=wiki_scope,
        resource_uuid=wiki_scope_key,
        path__iexact=path,
    ).exists():
        return _redirect_wiki_editor(
            page_id=page.id,
            status="wiki_path_exists",
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            team_id=wiki_team_id,
        )

    previous_scope = _normalize_wiki_scope(page.scope)
    previous_scope_key = str(page.resource_uuid or "").strip()
    previous_resource_uuid = _normalize_resource_uuid(previous_scope_key) if previous_scope == _WIKI_SCOPE_RESOURCE else ""
    was_draft = bool(page.is_draft)
    with transaction.atomic():
        page.scope = wiki_scope
        page.resource_uuid = wiki_scope_key
        page.resource_name = wiki_scope_name
        page.path = path
        page.title = title
        page.is_draft = is_draft
        page.body_markdown = body_markdown
        page.body_html_fallback = render_markdown_fallback(body_markdown)
        page.updated_by = request.user
        page.save(
            update_fields=[
                "scope",
                "resource_uuid",
                "resource_name",
                "path",
                "title",
                "is_draft",
                "body_markdown",
                "body_html_fallback",
                "updated_by",
                "updated_at",
            ]
        )

        teams = list(Group.objects.filter(name__in=team_names))
        page.team_access.set(teams)

    normalized_wiki_resource_uuid = wiki_scope_key if wiki_scope == _WIKI_SCOPE_RESOURCE else ""
    if (
        previous_scope == _WIKI_SCOPE_RESOURCE
        and previous_resource_uuid
        and (wiki_scope != _WIKI_SCOPE_RESOURCE or previous_resource_uuid != normalized_wiki_resource_uuid)
    ):
        _upsert_resource_kb_after_wiki_mutation(
            actor=request.user,
            resource_uuid=previous_resource_uuid,
        )
    if wiki_scope == _WIKI_SCOPE_RESOURCE and normalized_wiki_resource_uuid:
        _upsert_resource_kb_after_wiki_mutation(
            actor=request.user,
            resource_uuid=normalized_wiki_resource_uuid,
        )
    _sync_global_workspace_wiki_kb_page(page=page)

    if is_draft:
        return _redirect_wiki_editor(
            page_id=page.id,
            status="wiki_draft_saved",
            scope=page.scope,
            resource_uuid=page.resource_uuid if page.scope == _WIKI_SCOPE_RESOURCE else "",
            team_id=page.resource_uuid if page.scope == _WIKI_SCOPE_TEAM else "",
        )
    if was_draft:
        return _redirect_wiki(
            status="wiki_page_published",
            page_path=page.path,
            scope=page.scope,
            resource_uuid=page.resource_uuid if page.scope == _WIKI_SCOPE_RESOURCE else "",
            team_id=page.resource_uuid if page.scope == _WIKI_SCOPE_TEAM else "",
        )
    return _redirect_wiki(
        status="wiki_page_updated",
        page_path=page.path,
        scope=page.scope,
        resource_uuid=page.resource_uuid if page.scope == _WIKI_SCOPE_RESOURCE else "",
        team_id=page.resource_uuid if page.scope == _WIKI_SCOPE_TEAM else "",
    )


@require_POST
@superuser_required
def wiki_delete_page(request, page_id: int):
    page = get_object_or_404(WikiPage, id=page_id)
    page_scope = _normalize_wiki_scope(page.scope)
    page_scope_key = str(page.resource_uuid or "").strip()
    page_resource_uuid = _normalize_resource_uuid(page_scope_key) if page_scope == _WIKI_SCOPE_RESOURCE else ""
    page_team_id = _normalize_team_id(page_scope_key) if page_scope == _WIKI_SCOPE_TEAM else ""
    if page_scope == _WIKI_SCOPE_WORKSPACE:
        _sync_global_workspace_wiki_kb_page(page=page, force_delete=True)
    page.delete()
    if page_scope == _WIKI_SCOPE_RESOURCE and page_resource_uuid:
        _upsert_resource_kb_after_wiki_mutation(
            actor=request.user,
            resource_uuid=page_resource_uuid,
        )
    return _redirect_wiki(
        status="wiki_page_deleted",
        scope=page_scope,
        resource_uuid=page_resource_uuid,
        team_id=page_team_id,
    )


@login_required
def team_page(request):
    _ensure_default_sdk_workspace_wiki_page(actor=request.user)
    memberships = [
        {"id": int(team.id), "name": str(team.name or "").strip()}
        for team in request.user.groups.order_by("name")
    ]
    member_team_ids = [item["id"] for item in memberships]

    resource_index: dict[str, dict[str, object]] = {}

    def _ensure_resource_entry(resource_uuid: str) -> tuple[str, dict[str, object] | None]:
        resolved_uuid = str(resource_uuid or "").strip().lower()
        if not resolved_uuid:
            return "", None
        entry = resource_index.setdefault(
            resolved_uuid,
            {
                "team_names": set(),
                "source_tags": set(),
                "fallback_name": "",
                "candidate_owners": {},
            },
        )
        return resolved_uuid, entry

    if member_team_ids:
        team_owner_rows = (
            ResourcePackageOwner.objects.select_related("owner_team", "created_by", "updated_by")
            .filter(
                owner_scope=ResourcePackageOwner.OWNER_SCOPE_TEAM,
                owner_team_id__in=member_team_ids,
            )
            .order_by("-updated_at")
        )
        for row in team_owner_rows:
            resource_uuid, entry = _ensure_resource_entry(str(getattr(row, "resource_uuid", "") or ""))
            if entry is None:
                continue
            team_name = str(getattr(getattr(row, "owner_team", None), "name", "") or "").strip()
            if team_name:
                entry["team_names"].add(team_name)
            entry["source_tags"].add("team-owned")
            owner_candidates = entry.get("candidate_owners")
            if isinstance(owner_candidates, dict):
                for candidate in [getattr(row, "updated_by", None), getattr(row, "created_by", None)]:
                    candidate_id = int(getattr(candidate, "id", 0) or 0)
                    if candidate_id <= 0 or not bool(getattr(candidate, "is_active", False)):
                        continue
                    owner_candidates[candidate_id] = candidate

        team_share_rows = (
            ResourceTeamShare.objects.select_related("team", "owner")
            .filter(team_id__in=member_team_ids)
            .order_by("-updated_at", "-created_at")
        )
        for row in team_share_rows:
            resource_uuid, entry = _ensure_resource_entry(str(getattr(row, "resource_uuid", "") or ""))
            if entry is None:
                continue
            team_name = str(getattr(getattr(row, "team", None), "name", "") or "").strip()
            if team_name:
                entry["team_names"].add(team_name)
            fallback_name = str(getattr(row, "resource_name", "") or "").strip()
            if fallback_name and not entry["fallback_name"]:
                entry["fallback_name"] = fallback_name
            entry["source_tags"].add("shared")
            owner = getattr(row, "owner", None)
            owner_id = int(getattr(owner, "id", 0) or 0)
            owner_candidates = entry.get("candidate_owners")
            if (
                isinstance(owner_candidates, dict)
                and owner_id > 0
                and owner is not None
                and bool(getattr(owner, "is_active", False))
            ):
                owner_candidates[owner_id] = owner

    alias_by_resource_uuid: dict[str, ResourceRouteAlias] = {}
    if resource_index:
        alias_rows = (
            ResourceRouteAlias.objects.select_related("owner_user")
            .filter(resource_uuid__in=list(resource_index.keys()), is_current=True)
            .only(
                "resource_uuid",
                "route_kind",
                "route_value",
                "owner_user__id",
                "owner_user__is_active",
            )
        )
        for alias in alias_rows:
            resource_uuid = str(getattr(alias, "resource_uuid", "") or "").strip().lower()
            if not resource_uuid:
                continue
            alias_by_resource_uuid[resource_uuid] = alias
            entry = resource_index.get(resource_uuid)
            if entry is None:
                continue
            owner = getattr(alias, "owner_user", None)
            owner_id = int(getattr(alias, "owner_user_id", 0) or 0)
            owner_candidates = entry.get("candidate_owners")
            if (
                isinstance(owner_candidates, dict)
                and owner is not None
                and owner_id > 0
                and bool(getattr(owner, "is_active", False))
            ):
                owner_candidates[owner_id] = owner

    team_resources: list[dict[str, object]] = []
    resource_lookup_cache: dict[tuple[int, str], object | None] = {}
    for resource_uuid, payload in resource_index.items():
        resource = None
        owner_candidates = payload.get("candidate_owners")
        if isinstance(owner_candidates, dict):
            for owner in owner_candidates.values():
                owner_id = int(getattr(owner, "id", 0) or 0)
                if owner_id <= 0 or not bool(getattr(owner, "is_active", False)):
                    continue
                cache_key = (owner_id, resource_uuid)
                if cache_key not in resource_lookup_cache:
                    resource_lookup_cache[cache_key] = get_resource_by_uuid(owner, resource_uuid)
                resolved = resource_lookup_cache.get(cache_key)
                if resolved is not None:
                    resource = resolved
                    break
        if resource is None:
            actor_id = int(getattr(request.user, "id", 0) or 0)
            if actor_id > 0:
                actor_cache_key = (actor_id, resource_uuid)
                if actor_cache_key not in resource_lookup_cache:
                    resource_lookup_cache[actor_cache_key] = get_resource_by_uuid(request.user, resource_uuid)
                resource = resource_lookup_cache.get(actor_cache_key)

        resource_name = str(payload.get("fallback_name") or "").strip() or resource_uuid
        resource_type = "resource"
        resource_target = "—"
        resource_status = "unknown"
        resource_checked = ""
        if resource is not None:
            resource_name = str(getattr(resource, "name", "") or "").strip() or resource_name
            resource_type = str(getattr(resource, "resource_type", "") or "").strip() or resource_type
            resource_target = str(getattr(resource, "target", "") or "").strip() or resource_target
            resource_status = _normalize_health_status(str(getattr(resource, "last_status", "") or ""))
            resource_checked = _format_display_time(str(getattr(resource, "last_checked_at", "") or ""))

        source_tags = sorted({str(item or "").strip() for item in payload.get("source_tags", set()) if str(item or "").strip()})
        source_label = ", ".join(source_tags) if source_tags else "team"
        detail_url = reverse(
            "resource_detail",
            kwargs={"username": request.user.get_username(), "resource_uuid": resource_uuid},
        )
        current_alias = alias_by_resource_uuid.get(resource_uuid)
        if current_alias is not None:
            try:
                detail_url = _resource_route_reverse(
                    route_kind=current_alias.route_kind,
                    route_value=current_alias.route_value,
                    endpoint_key="detail",
                    resource_uuid=resource_uuid,
                )
            except NoReverseMatch:
                pass
        team_resources.append(
            {
                "resource_uuid": resource_uuid,
                "name": resource_name,
                "resource_type": resource_type,
                "target": resource_target,
                "status": resource_status,
                "status_label": resource_status.title(),
                "checked_display": resource_checked or "—",
                "team_names": sorted(payload.get("team_names", set()), key=lambda value: str(value).lower()),
                "source_label": source_label,
                "detail_url": detail_url,
            }
        )
    team_resources.sort(key=lambda item: (str(item["name"]).lower(), str(item["resource_uuid"])))
    team_resource_name_lookup = {
        str(item.get("resource_uuid") or "").strip().lower(): str(item.get("name") or "").strip()
        for item in team_resources
        if str(item.get("resource_uuid") or "").strip()
    }
    team_planner_external_items_by_team = _team_planner_external_items_by_team(
        memberships=memberships,
        resource_index=resource_index,
        resource_name_lookup=team_resource_name_lookup,
    )

    twilio_sms_available = is_twilio_configured()
    email_notifications_available = is_support_inbox_email_alerts_enabled()
    team_chat_notification_settings_by_team: dict[int, dict[str, bool]] = {}
    for item in memberships:
        team_id = int(item.get("id") or 0)
        team_name = str(item.get("name") or "").strip()
        if team_id <= 0:
            continue
        team_obj = Group(id=team_id, name=team_name)
        team_chat_notification_settings_by_team[team_id] = get_team_chat_notification_settings(
            team_obj,
            user_id=int(request.user.id or 0),
        )

    return render(
        request,
        "pages/team.html",
        {
            "team_memberships": memberships,
            "team_resources": team_resources,
            "team_membership_count": len(memberships),
            "team_resource_count": len(team_resources),
            "team_planner_external_items_by_team": team_planner_external_items_by_team,
            "twilio_sms_available": twilio_sms_available,
            "email_notifications_available": email_notifications_available,
            "team_chat_notification_settings_by_team": team_chat_notification_settings_by_team,
        },
    )


@login_required
def resources(request):
    resources = list_resources(request.user)
    resource_alerts = _resource_alerts(resources)
    resource_insights = _resources_overview_metrics(user=request.user, resources=resources)
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
        'resource_insights': resource_insights,
        'member_teams': member_teams,
        'account_ssh_keys': account_ssh_keys,
        'team_ssh_keys': team_ssh_keys,
        'ssh_credential_choices': ssh_credentials,
        'global_ssh_keys': global_ssh_credentials,
    }
    return render(request, 'pages/resources.html', context)


def _resolve_resource_wiki_route_context(*, actor, route_kind: str, route_value: str, resource_uuid: str):
    owner, resource, current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=route_value,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return None, None, None, "", ""
    if not _can_access_owner_resource(actor=actor, owner=owner, resource_uuid=resource.resource_uuid):
        raise PermissionDenied("You do not have access to this resource.")
    active_route_kind, active_route_value = _active_resource_route_values(
        current_alias=current_alias,
        route_kind=route_kind,
        route_value=route_value,
    )
    return owner, resource, current_alias, active_route_kind, active_route_value


@login_required
def resource_wiki(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, current_alias, active_route_kind, active_route_value = _resolve_resource_wiki_route_context(
        actor=request.user,
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if current_alias and not _resource_route_matches(current_alias=current_alias, route_kind=route_kind, route_value=username):
        redirect_url = _resource_route_redirect_url(
            current_alias=current_alias,
            endpoint_key="wiki",
            resource_uuid=resource.resource_uuid,
        )
        if redirect_url:
            return redirect(redirect_url)
    resource_detail_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="detail",
        resource_uuid=resource.resource_uuid,
    )
    wiki_page_base_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki",
        resource_uuid=resource.resource_uuid,
    )

    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)

    scope_context = _resolve_wiki_scope_context(
        actor=request.user,
        raw_scope=_WIKI_SCOPE_RESOURCE,
        raw_resource_uuid=resource.resource_uuid,
    )
    wiki_scope = str(scope_context["scope"])
    wiki_resource_uuid = str(scope_context["resource_uuid"])
    wiki_resource_name = str(scope_context["resource_name"])
    wiki_resource_options = list(scope_context["resource_options"])
    scope_status_code = str(scope_context["status_code"] or "")
    if not status_message and scope_status_code:
        status_message, status_tone = _wiki_status_context(scope_status_code)

    requested_page_raw = (request.GET.get("page") or "").strip()
    listing_context = _build_wiki_page_listing_context(
        actor=request.user,
        wiki_scope=wiki_scope,
        wiki_resource_uuid=wiki_resource_uuid,
        requested_page_raw=requested_page_raw,
    )
    missing_status_code = str(listing_context["missing_status_code"] or "")
    if not status_message and missing_status_code:
        status_message, status_tone = _wiki_status_context(missing_status_code)

    wiki_context_query = urlencode(
        _wiki_query_params(
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
        )
    )
    wiki_context_with_page_prefix = f"{wiki_context_query}&" if wiki_context_query else ""
    wiki_editor_new_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki_editor_new",
        resource_uuid=resource.resource_uuid,
    )

    def _editor_url_builder(page_id: int) -> str:
        return _resource_route_reverse(
            route_kind=active_route_kind,
            route_value=active_route_value,
            endpoint_key="wiki_editor",
            resource_uuid=resource.resource_uuid,
            page_id=int(page_id),
        )

    def _delete_url_builder(page_id: int) -> str:
        return _resource_route_reverse(
            route_kind=active_route_kind,
            route_value=active_route_value,
            endpoint_key="wiki_delete_page",
            resource_uuid=resource.resource_uuid,
            page_id=int(page_id),
        )

    _apply_wiki_action_urls(
        listing_context=listing_context,
        editor_url_builder=_editor_url_builder,
        delete_url_builder=_delete_url_builder,
    )

    return render(
        request,
        "pages/wiki.html",
        {
            **listing_context,
            "wiki_scope": wiki_scope,
            "wiki_scope_label": "Resource Wiki",
            "wiki_is_resource_scope": wiki_scope == _WIKI_SCOPE_RESOURCE,
            "wiki_resource_uuid": wiki_resource_uuid,
            "wiki_resource_name": wiki_resource_name or str(resource.name or ""),
            "wiki_resource_options": wiki_resource_options,
            "wiki_context_query": wiki_context_query,
            "wiki_context_with_page_prefix": wiki_context_with_page_prefix,
            "wiki_page_base_url": wiki_page_base_url,
            "wiki_editor_new_url": wiki_editor_new_url,
            "wiki_resource_shell_url": resource_detail_url,
            "wiki_resource_shell_label": str(resource.name or "").strip() or "Resource",
            "wiki_scope_locked": True,
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
def resource_wiki_editor_new(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, current_alias, active_route_kind, active_route_value = _resolve_resource_wiki_route_context(
        actor=request.user,
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if current_alias and not _resource_route_matches(current_alias=current_alias, route_kind=route_kind, route_value=username):
        redirect_url = _resource_route_redirect_url(
            current_alias=current_alias,
            endpoint_key="wiki_editor_new",
            resource_uuid=resource.resource_uuid,
        )
        if redirect_url:
            return redirect(redirect_url)

    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)

    wiki_resource_uuid = _normalize_resource_uuid(resource.resource_uuid)
    wiki_resource_name = str(resource.name or "").strip() or wiki_resource_uuid
    wiki_resource_options = [{"resource_uuid": wiki_resource_uuid, "resource_name": wiki_resource_name}]
    editor_context_query = urlencode(
        _wiki_query_params(
            scope=_WIKI_SCOPE_RESOURCE,
            resource_uuid=wiki_resource_uuid,
        )
    )

    wiki_back_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki",
        resource_uuid=wiki_resource_uuid,
    )
    wiki_create_page_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki_create_page",
        resource_uuid=wiki_resource_uuid,
    )
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
                "scope": _WIKI_SCOPE_RESOURCE,
                "resource_uuid": wiki_resource_uuid,
                "resource_name": wiki_resource_name,
            },
            "member_teams": member_teams,
            "wiki_scope": _WIKI_SCOPE_RESOURCE,
            "wiki_is_resource_scope": True,
            "wiki_resource_uuid": wiki_resource_uuid,
            "wiki_resource_name": wiki_resource_name,
            "wiki_resource_options": wiki_resource_options,
            "editor_context_query": editor_context_query,
            "wiki_scope_locked": True,
            "wiki_back_url": wiki_back_url,
            "wiki_create_page_url": wiki_create_page_url,
            "wiki_update_page_url": "",
            "wiki_delete_page_url": "",
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
def resource_wiki_editor(request, username: str, resource_uuid, page_id: int, route_kind: str = "user"):
    owner, resource, current_alias, active_route_kind, active_route_value = _resolve_resource_wiki_route_context(
        actor=request.user,
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return redirect("resources")
    if current_alias and not _resource_route_matches(current_alias=current_alias, route_kind=route_kind, route_value=username):
        redirect_url = _resource_route_redirect_url(
            current_alias=current_alias,
            endpoint_key="wiki_editor",
            resource_uuid=resource.resource_uuid,
            page_id=int(page_id),
        )
        if redirect_url:
            return redirect(redirect_url)

    page = get_object_or_404(WikiPage.objects.prefetch_related("team_access"), id=page_id)
    page_scope = _normalize_wiki_scope(page.scope)
    page_resource_uuid = _normalize_resource_uuid(page.resource_uuid or "")
    resource_uuid_normalized = _normalize_resource_uuid(resource.resource_uuid)
    if page_scope != _WIKI_SCOPE_RESOURCE or page_resource_uuid != resource_uuid_normalized:
        return _redirect_resource_wiki(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=resource_uuid_normalized,
            status="wiki_page_not_found",
        )
    if not _can_edit_wiki_page(actor=request.user, page=page):
        return _redirect_resource_wiki(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=resource_uuid_normalized,
            status="wiki_no_access",
            page_path=str(page.path or ""),
        )

    status_code = (request.GET.get("status") or "").strip()
    status_message, status_tone = _wiki_status_context(status_code)
    page_resource_name = str(page.resource_name or "").strip() or str(resource.name or "").strip() or resource_uuid_normalized
    wiki_resource_options = [{"resource_uuid": resource_uuid_normalized, "resource_name": page_resource_name}]
    editor_context_query = urlencode(
        _wiki_query_params(
            scope=_WIKI_SCOPE_RESOURCE,
            resource_uuid=resource_uuid_normalized,
        )
    )
    wiki_back_url = _resource_wiki_route_url(
        route_kind=active_route_kind,
        route_value=active_route_value,
        resource_uuid=resource_uuid_normalized,
        endpoint_key="wiki",
        page_path=str(page.path or ""),
    )
    wiki_create_page_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki_create_page",
        resource_uuid=resource_uuid_normalized,
    )
    wiki_update_page_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki_update_page",
        resource_uuid=resource_uuid_normalized,
        page_id=int(page.id),
    )
    wiki_delete_page_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki_delete_page",
        resource_uuid=resource_uuid_normalized,
        page_id=int(page.id),
    )
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
                "scope": _WIKI_SCOPE_RESOURCE,
                "resource_uuid": resource_uuid_normalized,
                "resource_name": page_resource_name,
                "updated_display": _format_display_time(page.updated_at.isoformat() if getattr(page, "updated_at", None) else ""),
                "created_display": _format_display_time(page.created_at.isoformat() if getattr(page, "created_at", None) else ""),
            },
            "member_teams": member_teams,
            "wiki_scope": _WIKI_SCOPE_RESOURCE,
            "wiki_is_resource_scope": True,
            "wiki_resource_uuid": resource_uuid_normalized,
            "wiki_resource_name": page_resource_name,
            "wiki_resource_options": wiki_resource_options,
            "editor_context_query": editor_context_query,
            "wiki_scope_locked": True,
            "wiki_back_url": wiki_back_url,
            "wiki_create_page_url": wiki_create_page_url,
            "wiki_update_page_url": wiki_update_page_url,
            "wiki_delete_page_url": wiki_delete_page_url,
            "status_message": status_message,
            "status_tone": status_tone,
        },
    )


@login_required
@require_POST
def resource_wiki_create_page(request, username: str, resource_uuid, route_kind: str = "user"):
    _owner, resource, _current_alias, active_route_kind, active_route_value = _resolve_resource_wiki_route_context(
        actor=request.user,
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if _owner is None or resource is None:
        return redirect("resources")

    wiki_scope = _WIKI_SCOPE_RESOURCE
    wiki_resource_uuid = _normalize_resource_uuid(resource.resource_uuid)
    wiki_resource_name = str(resource.name or "").strip() or wiki_resource_uuid
    body_markdown = str(request.POST.get("body_markdown") or "").replace("\r\n", "\n").replace("\r", "\n")
    title = _extract_wiki_title_from_markdown(body_markdown)
    path = _normalize_wiki_path(request.POST.get("path") or "", title)
    team_names = _normalize_wiki_team_names(request.user, request.POST.getlist("team_names"))
    save_intent = (request.POST.get("save_intent") or "publish").strip().lower()
    is_draft = save_intent != "publish"

    if not title:
        return _redirect_resource_wiki_editor_new(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            status="wiki_title_required",
        )
    if not path:
        return _redirect_resource_wiki_editor_new(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            status="wiki_path_required",
        )
    if len(path) > 220:
        return _redirect_resource_wiki_editor_new(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            status="wiki_path_invalid",
        )
    if WikiPage.objects.filter(
        scope=wiki_scope,
        resource_uuid=wiki_resource_uuid,
        path__iexact=path,
    ).exists():
        return _redirect_resource_wiki_editor_new(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            status="wiki_path_exists",
        )

    with transaction.atomic():
        page = WikiPage.objects.create(
            scope=wiki_scope,
            resource_uuid=wiki_resource_uuid,
            resource_name=wiki_resource_name,
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

    _upsert_resource_kb_after_wiki_mutation(
        actor=request.user,
        resource_uuid=wiki_resource_uuid,
    )

    if is_draft:
        return _redirect_resource_wiki_editor(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            page_id=int(page.id),
            status="wiki_draft_saved",
        )
    return _redirect_resource_wiki(
        route_kind=active_route_kind,
        route_value=active_route_value,
        resource_uuid=wiki_resource_uuid,
        status="wiki_page_created",
        page_path=str(page.path or ""),
    )


@login_required
@require_POST
def resource_wiki_update_page(request, username: str, resource_uuid, page_id: int, route_kind: str = "user"):
    _owner, resource, _current_alias, active_route_kind, active_route_value = _resolve_resource_wiki_route_context(
        actor=request.user,
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if _owner is None or resource is None:
        return redirect("resources")

    page = get_object_or_404(WikiPage.objects.prefetch_related("team_access"), id=page_id)
    wiki_scope = _WIKI_SCOPE_RESOURCE
    wiki_resource_uuid = _normalize_resource_uuid(resource.resource_uuid)
    page_scope = _normalize_wiki_scope(page.scope)
    page_resource_uuid = _normalize_resource_uuid(page.resource_uuid or "")
    if page_scope != _WIKI_SCOPE_RESOURCE or page_resource_uuid != wiki_resource_uuid:
        return _redirect_resource_wiki(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            status="wiki_page_not_found",
        )
    if not _can_edit_wiki_page(actor=request.user, page=page):
        return _redirect_resource_wiki(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            status="wiki_no_access",
            page_path=str(page.path or ""),
        )

    wiki_resource_name = str(resource.name or "").strip() or wiki_resource_uuid
    body_markdown = str(request.POST.get("body_markdown") or "").replace("\r\n", "\n").replace("\r", "\n")
    title = _extract_wiki_title_from_markdown(body_markdown)
    path = _normalize_wiki_path(request.POST.get("path") or "", title)
    team_names = _normalize_wiki_team_names(request.user, request.POST.getlist("team_names"))
    save_intent = (request.POST.get("save_intent") or "publish").strip().lower()
    is_draft = save_intent != "publish"

    if not title:
        return _redirect_resource_wiki_editor(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            page_id=int(page.id),
            status="wiki_title_required",
        )
    if not path:
        return _redirect_resource_wiki_editor(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            page_id=int(page.id),
            status="wiki_path_required",
        )
    if len(path) > 220:
        return _redirect_resource_wiki_editor(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            page_id=int(page.id),
            status="wiki_path_invalid",
        )
    if WikiPage.objects.exclude(id=page.id).filter(
        scope=wiki_scope,
        resource_uuid=wiki_resource_uuid,
        path__iexact=path,
    ).exists():
        return _redirect_resource_wiki_editor(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            page_id=int(page.id),
            status="wiki_path_exists",
        )

    was_draft = bool(page.is_draft)
    with transaction.atomic():
        page.scope = wiki_scope
        page.resource_uuid = wiki_resource_uuid
        page.resource_name = wiki_resource_name
        page.path = path
        page.title = title
        page.is_draft = is_draft
        page.body_markdown = body_markdown
        page.body_html_fallback = render_markdown_fallback(body_markdown)
        page.updated_by = request.user
        page.save(
            update_fields=[
                "scope",
                "resource_uuid",
                "resource_name",
                "path",
                "title",
                "is_draft",
                "body_markdown",
                "body_html_fallback",
                "updated_by",
                "updated_at",
            ]
        )

        teams = list(Group.objects.filter(name__in=team_names))
        page.team_access.set(teams)

    _upsert_resource_kb_after_wiki_mutation(
        actor=request.user,
        resource_uuid=wiki_resource_uuid,
    )

    if is_draft:
        return _redirect_resource_wiki_editor(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            page_id=int(page.id),
            status="wiki_draft_saved",
        )
    if was_draft:
        return _redirect_resource_wiki(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=wiki_resource_uuid,
            status="wiki_page_published",
            page_path=str(page.path or ""),
        )
    return _redirect_resource_wiki(
        route_kind=active_route_kind,
        route_value=active_route_value,
        resource_uuid=wiki_resource_uuid,
        status="wiki_page_updated",
        page_path=str(page.path or ""),
    )


@require_POST
@superuser_required
def resource_wiki_delete_page(request, username: str, resource_uuid, page_id: int, route_kind: str = "user"):
    _owner, resource, _current_alias, active_route_kind, active_route_value = _resolve_resource_wiki_route_context(
        actor=request.user,
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if _owner is None or resource is None:
        return redirect("resources")

    page = get_object_or_404(WikiPage, id=page_id)
    page_scope = _normalize_wiki_scope(page.scope)
    page_resource_uuid = _normalize_resource_uuid(page.resource_uuid or "")
    resource_uuid_normalized = _normalize_resource_uuid(resource.resource_uuid)
    if page_scope != _WIKI_SCOPE_RESOURCE or page_resource_uuid != resource_uuid_normalized:
        return _redirect_resource_wiki(
            route_kind=active_route_kind,
            route_value=active_route_value,
            resource_uuid=resource_uuid_normalized,
            status="wiki_page_not_found",
        )
    page.delete()
    _upsert_resource_kb_after_wiki_mutation(
        actor=request.user,
        resource_uuid=resource_uuid_normalized,
    )
    return _redirect_resource_wiki(
        route_kind=active_route_kind,
        route_value=active_route_value,
        resource_uuid=resource_uuid_normalized,
        status="wiki_page_deleted",
    )


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
                "packet_loss_pct": item.packet_loss_pct,
            }
        )
    resource_api_keys = list_resource_api_keys(owner, resource.resource_uuid)
    resource_detail_url_path = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="detail",
        resource_uuid=resource.resource_uuid,
    )
    resource_wiki_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="wiki",
        resource_uuid=resource.resource_uuid,
    )
    health_check_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="check",
        resource_uuid=resource.resource_uuid,
    )
    ping_stream_url = _resource_route_reverse(
        route_kind=active_route_kind,
        route_value=active_route_value,
        endpoint_key="ping_stream",
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
    email_notifications_available = is_support_inbox_email_alerts_enabled()
    if not twilio_sms_available:
        alert_settings["health_alerts_sms_enabled"] = False
        alert_settings["cloud_log_errors_sms_enabled"] = False
    if not email_notifications_available:
        alert_settings["health_alerts_email_enabled"] = False
        alert_settings["cloud_log_errors_email_enabled"] = False
    can_manage_resource = _can_manage_owner_resource(actor=request.user, owner=owner)
    member_teams: list[str] = _ssh_team_choices_for_user(request.user) if can_manage_resource else []
    ssh_credentials: list[dict[str, object]] = []
    if can_manage_resource:
        local_ssh_credentials = list_ssh_credentials(request.user)
        global_ssh_credentials = list_global_ssh_credentials()
        for item in local_ssh_credentials:
            ssh_credentials.append(
                {
                    "id": item.id,
                    "id_value": str(item.id),
                    "name": item.name,
                    "scope": item.scope,
                    "scope_level": item.scope if item.scope in {"account", "team"} else "account",
                    "team_names": item.team_names,
                    "created_at": item.created_at,
                    "is_global": False,
                }
            )
        for item in global_ssh_credentials:
            ssh_credentials.append(
                {
                    "id": item.id,
                    "id_value": f"global:{item.id}",
                    "name": item.name,
                    "scope": "global_team",
                    "scope_level": "global",
                    "team_names": [item.team_name] if item.team_name else [],
                    "created_at": item.created_at,
                    "is_global": True,
                }
            )
    latest_resource_api_key_value = str(
        request.session.pop(f"latest_created_resource_api_key:{resource.resource_uuid}", "") or ""
    ).strip()
    raw_notes = list_resource_notes(owner, resource.resource_uuid, limit=300)
    note_author_ids = sorted({int(item.author_user_id) for item in raw_notes if int(item.author_user_id or 0) > 0})
    author_avatar_urls = resolve_user_avatar_urls(note_author_ids)

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
    asana_overview = _asana_overview_context_for_user(
        request.user,
        force_refresh=False,
        cache_key=_ASANA_OVERVIEW_CACHE_KEY,
        task_fetch_limit=_ASANA_OVERVIEW_TASK_FETCH_LIMIT,
        run_auto_assign=False,
        write_calendar_cache=False,
    )
    asana_resource_options = _asana_resource_options_for_user(request.user)
    asana_resource_lookup = {
        str(item.get("resource_uuid") or "").strip().lower(): str(item.get("resource_name") or "").strip()
        for item in asana_resource_options
        if isinstance(item, dict)
    }
    asana_board_resource_mappings = list_user_asana_board_resource_mappings(request.user)
    asana_task_resource_mappings = list_user_asana_task_resource_mappings(request.user)
    asana_overview_rows = asana_overview.get("tasks") if isinstance(asana_overview.get("tasks"), list) else []
    enriched_asana_rows = _asana_enriched_tasks_with_resource_mappings(
        task_rows=[row for row in asana_overview_rows if isinstance(row, dict)],
        board_resource_mappings=asana_board_resource_mappings,
        task_resource_mappings=asana_task_resource_mappings,
        resource_name_lookup=asana_resource_lookup,
    )
    resource_uuid_key = str(getattr(resource, "resource_uuid", "") or "").strip().lower()
    resource_asana_rows = [
        row
        for row in enriched_asana_rows
        if resource_uuid_key
        and resource_uuid_key in [
            str(value or "").strip().lower()
            for value in (row.get("resource_uuids") or [])
            if str(value or "").strip()
        ]
    ]
    resource_asana_overview = dict(asana_overview)
    resource_asana_overview["tasks"] = resource_asana_rows
    resource_asana_overview["task_count"] = len(resource_asana_rows)
    resource_calendar_external_items = _asana_planner_items_from_context(resource_asana_overview)
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
            "resource_wiki_url": resource_wiki_url,
            "can_manage_resource": can_manage_resource,
            "latest_resource_api_key_value": latest_resource_api_key_value,
            "resource_check_url": health_check_url,
            "resource_ping_stream_url": ping_stream_url,
            "resource_note_add_url": notes_add_url,
            "resource_alert_settings_update_url": alert_settings_update_url,
            "resource_api_key_create_url": api_key_create_url,
            "twilio_sms_available": twilio_sms_available,
            "email_notifications_available": email_notifications_available,
            "member_teams": member_teams,
            "ssh_credential_choices": ssh_credentials,
            "resource_asana_overview": resource_asana_overview,
            "resource_asana_all_tasks": enriched_asana_rows,
            "resource_calendar_external_items": resource_calendar_external_items,
            "asana_resource_options": asana_resource_options,
            "asana_board_resource_mappings": asana_board_resource_mappings,
            "asana_task_resource_mappings": asana_task_resource_mappings,
            "asana_completed_window_days": _ASANA_AGENDA_COMPLETED_WINDOW_DAYS,
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
    email_notifications_available = is_support_inbox_email_alerts_enabled()
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
@require_GET
def resource_ping_stream(request, username: str, resource_uuid, route_kind: str = "user"):
    owner, resource, _current_alias = _resolve_resource_route_context(
        route_kind=route_kind,
        route_value=username,
        resource_uuid=str(resource_uuid),
    )
    if owner is None or resource is None:
        return HttpResponse(status=404)
    if not _can_access_owner_resource(actor=request.user, owner=owner, resource_uuid=resource.resource_uuid):
        raise PermissionDenied("You do not have access to this resource.")

    interval_seconds = 5.0
    raw_interval = str(request.GET.get("interval") or "").strip()
    if raw_interval:
        try:
            interval_seconds = float(raw_interval)
        except Exception:
            interval_seconds = 5.0
    interval_seconds = max(1.0, min(30.0, interval_seconds))

    sample_limit = 0
    raw_limit = str(request.GET.get("max_samples") or "").strip()
    if raw_limit:
        try:
            sample_limit = int(raw_limit)
        except Exception:
            sample_limit = 0
    sample_limit = max(0, min(600, sample_limit))

    def _event_stream():
        sent = 0
        yield "retry: 2000\n\n"
        while True:
            checked_at = datetime.now(timezone.utc).isoformat()
            status, error, target, latency_ms, packet_loss_pct = probe_resource_ping(resource)
            payload = {
                "status": str(status or "unknown").strip().lower() or "unknown",
                "checked_at": checked_at,
                "target": str(target or ""),
                "error": str(error or ""),
                "check_method": "ping",
                "latency_ms": latency_ms,
                "packet_loss_pct": packet_loss_pct,
            }
            yield f"event: ping\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"
            sent += 1
            if sample_limit and sent >= sample_limit:
                break
            time.sleep(interval_seconds)

    response = StreamingHttpResponse(_event_stream(), content_type="text/event-stream")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


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
def team_resource_wiki(request, team_name: str, resource_uuid):
    return resource_wiki(request, team_name, resource_uuid, route_kind="team")


@login_required
def team_resource_wiki_editor_new(request, team_name: str, resource_uuid):
    return resource_wiki_editor_new(request, team_name, resource_uuid, route_kind="team")


@login_required
def team_resource_wiki_editor(request, team_name: str, resource_uuid, page_id: int):
    return resource_wiki_editor(request, team_name, resource_uuid, page_id, route_kind="team")


@login_required
@require_POST
def team_resource_wiki_create_page(request, team_name: str, resource_uuid):
    return resource_wiki_create_page(request, team_name, resource_uuid, route_kind="team")


@login_required
@require_POST
def team_resource_wiki_update_page(request, team_name: str, resource_uuid, page_id: int):
    return resource_wiki_update_page(request, team_name, resource_uuid, page_id, route_kind="team")


@require_POST
@superuser_required
def team_resource_wiki_delete_page(request, team_name: str, resource_uuid, page_id: int):
    return resource_wiki_delete_page(request, team_name, resource_uuid, page_id, route_kind="team")


@login_required
@require_POST
def team_update_resource_alert_settings(request, team_name: str, resource_uuid):
    return update_resource_alert_settings(request, team_name, resource_uuid, route_kind="team")


@login_required
@require_POST
def team_check_resource_health_detail(request, team_name: str, resource_uuid):
    return check_resource_health_detail(request, team_name, resource_uuid, route_kind="team")


@login_required
@require_GET
def team_resource_ping_stream(request, team_name: str, resource_uuid):
    return resource_ping_stream(request, team_name, resource_uuid, route_kind="team")


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


def _normalize_kb_result_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _truncate_kb_result_text(value: str, limit: int = 200) -> str:
    text = _normalize_kb_result_text(value)
    if len(text) <= limit:
        return text
    return f"{text[: max(1, limit - 1)].rstrip()}…"


def _kb_markdown_snippet(markdown: str, query: str, *, prefix: int = 120, suffix: int = 240, fallback: int = 240) -> str:
    body = str(markdown or "")
    search = _normalize_kb_result_text(query)
    if search:
        lowered = body.lower()
        idx = lowered.find(search.lower())
        if idx >= 0:
            start = max(0, idx - max(0, int(prefix)))
            end = min(len(body), idx + max(1, int(suffix)))
            return body[start:end].strip()
    return body[: max(1, int(fallback))].strip()


def _kb_result_kind_and_url(*, actor, row: dict[str, object], metadata: dict[str, object]) -> tuple[str, str]:
    source = _normalize_kb_result_text(metadata.get("source") or "").lower()
    row_id = _normalize_kb_result_text(row.get("id") or "").lower()
    resource_uuid = _normalize_resource_uuid(str(metadata.get("resource_uuid") or ""))
    wiki_path = _normalize_kb_result_text(metadata.get("path") or metadata.get("wiki_path") or "")
    wiki_scope = _normalize_kb_result_text(metadata.get("scope") or metadata.get("wiki_scope") or "").lower()

    is_workspace_wiki = source == "workspace_wiki" or (wiki_scope == "workspace" and not resource_uuid)
    is_wiki_result = (
        is_workspace_wiki
        or source == "resource_wiki"
        or source.endswith("_wiki")
        or row_id.startswith("wiki:")
        or wiki_scope == "resource"
    )

    if is_workspace_wiki:
        wiki_url = reverse("wiki")
        if wiki_path:
            wiki_url = f"{wiki_url}?{urlencode({'page': wiki_path})}"
        return "wiki", wiki_url

    if is_wiki_result and resource_uuid:
        resource_wiki_url = _resource_wiki_url_for_uuid(
            actor=actor,
            resource_uuid=resource_uuid,
            page_path=wiki_path,
        )
        if resource_wiki_url:
            return "wiki", resource_wiki_url

    if is_wiki_result:
        wiki_url = reverse("wiki")
        if wiki_path:
            wiki_url = f"{wiki_url}?{urlencode({'page': wiki_path})}"
        return "wiki", wiki_url

    if resource_uuid:
        detail_url = _resource_detail_url_for_uuid(actor=actor, resource_uuid=resource_uuid)
        if detail_url:
            return "resource", detail_url
    return "kb", ""


def _kb_result_title(*, row: dict[str, object], metadata: dict[str, object], kind: str) -> str:
    if kind == "wiki":
        title = _normalize_kb_result_text(metadata.get("title") or "")
        if title:
            return title
        path = _normalize_kb_result_text(metadata.get("path") or "")
        if path:
            return path
        resource_uuid = _normalize_resource_uuid(str(metadata.get("resource_uuid") or ""))
        if resource_uuid:
            return "Resource Wiki Page"
        return "Workspace Wiki Page"

    if kind == "resource":
        resource_name = _normalize_kb_result_text(metadata.get("name") or "")
        if resource_name:
            return resource_name
        resource_uuid = _normalize_resource_uuid(str(metadata.get("resource_uuid") or ""))
        if resource_uuid:
            return resource_uuid
        return "Resource"

    generic_title = _normalize_kb_result_text(metadata.get("title") or "")
    if generic_title:
        return generic_title
    row_id = _normalize_kb_result_text(row.get("id") or "")
    if row_id:
        return row_id
    return "Knowledge Result"


def _kb_result_subtitle(*, metadata: dict[str, object], kind: str) -> str:
    if kind == "wiki":
        path = _normalize_kb_result_text(metadata.get("path") or "")
        is_draft = bool(metadata.get("is_draft", False))
        source = _normalize_kb_result_text(metadata.get("source") or "").lower()
        resource_uuid = _normalize_resource_uuid(str(metadata.get("resource_uuid") or ""))
        wiki_scope = _normalize_kb_result_text(metadata.get("scope") or metadata.get("wiki_scope") or "").lower()
        resource_name = _normalize_kb_result_text(metadata.get("resource_name") or metadata.get("name") or "")
        is_resource_wiki = bool(resource_uuid) or source == "resource_wiki" or wiki_scope == "resource"
        parts = ["Resource Wiki"] if is_resource_wiki else ["Workspace Wiki"]
        if is_resource_wiki and resource_name:
            parts.append(resource_name)
        if path:
            parts.append(path)
        if is_draft:
            parts.append("Draft")
        return " · ".join(parts)

    if kind == "resource":
        resource_type = _normalize_kb_result_text(metadata.get("resource_type") or "")
        status = _normalize_kb_result_text(metadata.get("status") or "")
        parts: list[str] = []
        if resource_type:
            parts.append(resource_type.title())
        parts.append("Resource")
        if status:
            parts.append(status.upper())
        return " · ".join(parts)

    source = _normalize_kb_result_text(metadata.get("source") or "knowledge")
    return source.replace("_", " ").title()


def _build_topbar_kb_suggestions(*, actor, rows: list[dict], limit: int = 8) -> list[dict[str, str]]:
    resolved_limit = max(1, min(int(limit or 8), 20))
    suggestions: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for row in rows:
        if not isinstance(row, dict):
            continue
        metadata = row.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        kind, url = _kb_result_kind_and_url(actor=actor, row=row, metadata=metadata)
        if not url:
            continue
        if url in seen_urls:
            continue

        title = _kb_result_title(row=row, metadata=metadata, kind=kind)
        if not title:
            continue

        suggestions.append(
            {
                "kind": kind,
                "title": title,
                "subtitle": _kb_result_subtitle(metadata=metadata, kind=kind),
                "snippet": _truncate_kb_result_text(str(row.get("document") or ""), limit=220),
                "url": url,
            }
        )
        seen_urls.add(url)
        if len(suggestions) >= resolved_limit:
            break
    return suggestions


def _tool_search_kb_for_actor(actor, args: dict) -> dict:
    query = str(args.get("query") or "").strip()
    user_path = _user_knowledge_db_path(actor)
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

    wiki_limit = 4
    wiki_qs = _wiki_accessible_queryset(
        actor,
        scope=_WIKI_SCOPE_WORKSPACE,
        resource_uuid="",
    )
    wiki_qs = wiki_qs.exclude(is_draft=False, team_access__isnull=True).distinct()
    if query:
        wiki_qs = wiki_qs.filter(
            Q(title__icontains=query)
            | Q(path__icontains=query)
            | Q(body_markdown__icontains=query)
        )
    wiki_pages = list(wiki_qs.order_by("-updated_at")[:wiki_limit])
    wiki_results: list[dict[str, object]] = []
    for page in wiki_pages:
        snippet = _kb_markdown_snippet(
            str(getattr(page, "body_markdown", "") or ""),
            query,
        )
        wiki_results.append(
            {
                "id": f"wiki:{int(getattr(page, 'id', 0) or 0)}",
                "document": snippet,
                "metadata": {
                    "source": "workspace_wiki",
                    "wiki_page_id": int(getattr(page, "id", 0) or 0),
                    "title": str(getattr(page, "title", "") or "").strip(),
                    "path": str(getattr(page, "path", "") or "").strip(),
                    "is_draft": bool(getattr(page, "is_draft", False)),
                    "updated_at": str(getattr(page, "updated_at", "") or ""),
                },
                "distance": None,
            }
        )

    merged = list(user_results) + list(global_results) + list(wiki_results)
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
        "wiki_limit": wiki_limit,
        "user_result_count": len(user_results),
        "global_result_count": len(global_results),
        "wiki_result_count": len(wiki_results),
        "result_count": len(merged),
        "user_results": user_results,
        "global_results": global_results,
        "wiki_results": wiki_results,
        "results": merged,
    }


def _resource_context_kb_rows_for_actor(
    *,
    actor,
    resource_uuid: str,
    query: str,
    kb_limit: int = 4,
    wiki_limit: int = 4,
) -> list[dict[str, object]]:
    resolved_uuid = _normalize_resource_uuid(resource_uuid)
    if not resolved_uuid:
        return []

    resource_payload = _tool_resource_kb_for_actor(
        actor,
        {
            "resource_uuid": resolved_uuid,
            "query": query,
            "limit": max(1, min(int(kb_limit or 4), 20)),
        },
    )
    if not bool(resource_payload.get("ok")):
        return []

    resource_name = _normalize_kb_result_text(resource_payload.get("resource_name") or "")
    kb_rows: list[dict[str, object]] = []
    for row in resource_payload.get("results") or []:
        if not isinstance(row, dict):
            continue
        metadata = row.get("metadata")
        if isinstance(metadata, dict):
            normalized_metadata = dict(metadata)
        else:
            normalized_metadata = {}
        normalized_metadata["source"] = _normalize_kb_result_text(normalized_metadata.get("source") or "resource_kb").lower()
        normalized_metadata["resource_uuid"] = _normalize_resource_uuid(
            str(normalized_metadata.get("resource_uuid") or resolved_uuid)
        )
        if resource_name and not _normalize_kb_result_text(normalized_metadata.get("name") or ""):
            normalized_metadata["name"] = resource_name
        normalized_row = dict(row)
        normalized_row["metadata"] = normalized_metadata
        kb_rows.append(normalized_row)

    wiki_rows: list[dict[str, object]] = []
    wiki_qs = _wiki_accessible_queryset(
        actor,
        scope=_WIKI_SCOPE_RESOURCE,
        resource_uuid=resolved_uuid,
    )
    if query:
        wiki_qs = wiki_qs.filter(
            Q(title__icontains=query)
            | Q(path__icontains=query)
            | Q(body_markdown__icontains=query)
        )
    wiki_pages = list(
        wiki_qs.order_by("-updated_at")[
            : max(1, min(int(wiki_limit or 4), 10))
        ]
    )
    for page in wiki_pages:
        page_resource_name = _normalize_kb_result_text(getattr(page, "resource_name", "") or "") or resource_name
        wiki_rows.append(
            {
                "id": f"resource_wiki:{int(getattr(page, 'id', 0) or 0)}",
                "document": _kb_markdown_snippet(
                    str(getattr(page, "body_markdown", "") or ""),
                    query,
                ),
                "metadata": {
                    "source": "resource_wiki",
                    "scope": _WIKI_SCOPE_RESOURCE,
                    "wiki_page_id": int(getattr(page, "id", 0) or 0),
                    "title": _normalize_kb_result_text(getattr(page, "title", "") or ""),
                    "path": _normalize_kb_result_text(getattr(page, "path", "") or ""),
                    "is_draft": bool(getattr(page, "is_draft", False)),
                    "updated_at": str(getattr(page, "updated_at", "") or ""),
                    "resource_uuid": resolved_uuid,
                    "resource_name": page_resource_name,
                    "name": page_resource_name,
                },
                "distance": None,
            }
        )

    return wiki_rows + kb_rows


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


def _tool_resource_kb_for_actor(actor, args: dict) -> dict:
    resource_uuid = str(args.get("resource_uuid") or "").strip()
    if not resource_uuid:
        return {"ok": False, "error": "resource_uuid is required", "results": []}

    owner_row = ResourcePackageOwner.objects.filter(resource_uuid=resource_uuid).first()
    is_global = bool(owner_row and owner_row.owner_scope == ResourcePackageOwner.OWNER_SCOPE_GLOBAL)
    if not is_global and not user_can_access_resource(user=actor, resource_uuid=resource_uuid):
        return {"ok": False, "error": f"user cannot access resource: {resource_uuid}", "results": []}

    owner_user, resource = _resolve_resource_owner_and_item(resource_uuid, actor)
    if owner_user is None or resource is None:
        return {"ok": False, "error": f"resource not found: {resource_uuid}", "results": []}

    query = str(args.get("query") or "").strip()
    try:
        limit = int(args.get("limit", 8) or 8)
    except Exception:
        limit = 8
    resolved_limit = max(1, min(limit, 50))

    owner_context = get_resource_owner_context(owner_user, resource_uuid)
    resource_dir = Path(owner_context.get("resource_dir") or "")
    resource_kb_path = resource_dir / "knowledge.db" if resource_dir else Path("")
    rows, query_error = _query_kb_resources(
        knowledge_path=resource_kb_path,
        query=query,
        limit=resolved_limit,
    )
    if query_error:
        return {"ok": False, "error": query_error, "results": []}

    return {
        "ok": True,
        "resource_uuid": resource_uuid,
        "resource_name": str(getattr(resource, "name", "") or ""),
        "owner_username": str(getattr(owner_user, "username", "") or ""),
        "query": query,
        "limit": resolved_limit,
        "knowledge_path": str(resource_kb_path),
        "result_count": len(rows),
        "results": rows,
    }


def _tool_resource_ssh_exec_for_actor(actor, args: dict) -> dict:
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

    command = str(args.get("command") or "").strip()
    try:
        timeout_seconds = int(args.get("timeout_seconds", 30) or 30)
    except Exception:
        timeout_seconds = 30
    try:
        max_output_chars = int(args.get("max_output_chars", 12000) or 12000)
    except Exception:
        max_output_chars = 12000

    result = execute_resource_ssh_command(
        owner_user=owner_user,
        resource=resource,
        command=command,
        timeout_seconds=timeout_seconds,
        max_output_chars=max_output_chars,
    )
    result["resource_uuid"] = resource_uuid
    result["resource_name"] = str(getattr(resource, "name", "") or "")
    result["owner_username"] = str(getattr(owner_user, "username", "") or "")
    return result


def _tool_search_users_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}
    if not bool(getattr(actor, "is_superuser", False)):
        return {"ok": False, "error": "superuser access required", "results": []}

    query = str(args.get("query") or "").strip()
    phone = str(args.get("phone") or "").strip()
    try:
        limit = int(args.get("limit", 10) or 10)
    except Exception:
        limit = 10
    resolved_limit = max(1, min(limit, 100))
    rows, query_error = query_user_records(
        query=query,
        phone=phone,
        limit=resolved_limit,
    )
    if query_error:
        return {"ok": False, "error": query_error, "results": []}

    return {
        "ok": True,
        "collection": "user_records",
        "query": query,
        "phone": phone,
        "limit": resolved_limit,
        "result_count": len(rows),
        "results": rows,
    }


def _actor_can_contact_user(*, actor, target_user) -> bool:
    if actor is None or target_user is None:
        return False
    if bool(getattr(actor, "is_superuser", False)):
        return True
    actor_id = int(getattr(actor, "id", 0) or 0)
    target_id = int(getattr(target_user, "id", 0) or 0)
    if actor_id > 0 and actor_id == target_id:
        return True
    actor_team_ids = set(actor.groups.values_list("id", flat=True))
    if not actor_team_ids:
        return False
    target_team_ids = set(target_user.groups.values_list("id", flat=True))
    return bool(actor_team_ids & target_team_ids)


def _directory_candidates_for_actor(
    actor,
    *,
    query: str = "",
    username: str = "",
    email: str = "",
    phone: str = "",
    limit: int = 4,
) -> list[dict]:
    if actor is None:
        return []
    User = get_user_model()
    qs = User.objects.filter(is_active=True).prefetch_related("groups")
    if not bool(getattr(actor, "is_superuser", False)):
        actor_team_ids = list(actor.groups.values_list("id", flat=True))
        if actor_team_ids:
            qs = qs.filter(Q(id=int(getattr(actor, "id", 0) or 0)) | Q(groups__id__in=actor_team_ids)).distinct()
        else:
            qs = qs.filter(id=int(getattr(actor, "id", 0) or 0))

    resolved_query = str(query or "").strip()
    resolved_username = str(username or "").strip()
    resolved_email = str(email or "").strip().lower()
    resolved_phone = _normalize_phone(str(phone or "").strip())
    resolved_phone_digits = resolved_phone.lstrip("+") if resolved_phone.startswith("+") else resolved_phone

    if resolved_username:
        qs = qs.filter(username__icontains=resolved_username)
    if resolved_email:
        qs = qs.filter(email__icontains=resolved_email)
    if resolved_query:
        qs = qs.filter(
            Q(username__icontains=resolved_query)
            | Q(email__icontains=resolved_query)
            | Q(first_name__icontains=resolved_query)
            | Q(last_name__icontains=resolved_query)
            | Q(groups__name__icontains=resolved_query)
        ).distinct()

    fetch_limit = max(20, min(int(limit or 4) * 5, 300))
    users = list(qs.order_by("username")[:fetch_limit])
    if not users:
        return []
    user_ids = [int(getattr(item, "id", 0) or 0) for item in users if int(getattr(item, "id", 0) or 0) > 0]
    phone_rows = UserNotificationSettings.objects.filter(user_id__in=user_ids).values("user_id", "phone_number")
    phone_map: dict[int, str] = {
        int(row.get("user_id") or 0): _normalize_phone(str(row.get("phone_number") or ""))
        for row in phone_rows
        if int(row.get("user_id") or 0) > 0
    }

    rows: list[dict] = []
    for user in users:
        user_id = int(getattr(user, "id", 0) or 0)
        if user_id <= 0:
            continue
        row_phone = str(phone_map.get(user_id, "") or "")
        row_phone_digits = row_phone.lstrip("+") if row_phone.startswith("+") else row_phone
        if resolved_phone:
            if resolved_phone not in {row_phone, row_phone_digits} and (
                resolved_phone_digits not in {row_phone, row_phone_digits}
            ):
                continue
        team_names = sorted([str(group.name or "").strip() for group in user.groups.all() if str(group.name or "").strip()], key=lambda value: value.lower())
        rows.append(
            {
                "user_id": user_id,
                "username": str(getattr(user, "username", "") or "").strip(),
                "email": str(getattr(user, "email", "") or "").strip().lower(),
                "phone_number": row_phone,
                "full_name": " ".join(
                    [
                        str(getattr(user, "first_name", "") or "").strip(),
                        str(getattr(user, "last_name", "") or "").strip(),
                    ]
                ).strip(),
                "team_names": team_names,
                "is_superuser": bool(getattr(user, "is_superuser", False)),
                "is_staff": bool(getattr(user, "is_staff", False)),
            }
        )

    resolved_limit = max(1, min(int(limit or 4), 40))
    return rows[:resolved_limit]


def _tool_directory_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}
    query = str(args.get("query") or "").strip()
    username = str(args.get("username") or "").strip()
    email = str(args.get("email") or "").strip()
    phone = str(args.get("phone") or args.get("phone_number") or "").strip()
    try:
        limit = int(args.get("limit", 4) or 4)
    except Exception:
        limit = 4
    resolved_limit = max(1, min(limit, 40))

    rows = _directory_candidates_for_actor(
        actor,
        query=query,
        username=username,
        email=email,
        phone=phone,
        limit=resolved_limit,
    )
    return {
        "ok": True,
        "collection": "directory",
        "query": query,
        "username": username,
        "email": email,
        "phone": phone,
        "limit": resolved_limit,
        "result_count": len(rows),
        "results": rows,
    }


def _parse_reminder_recipients_arg(args: dict) -> tuple[list[str], bool]:
    recipients_provided = False
    raw_recipients = args.get("recipients")
    recipients: list[str] = []
    if isinstance(raw_recipients, list):
        recipients_provided = True
        recipients = [str(item or "").strip() for item in raw_recipients]
    elif isinstance(raw_recipients, str):
        recipients_provided = True
        recipients = [piece.strip() for piece in re.split(r"[,\n;]", raw_recipients) if piece.strip()]

    legacy_username = str(args.get("username") or "").strip()
    if legacy_username:
        recipients_provided = True
        recipients.append(legacy_username)
    legacy_recipient = str(args.get("recipient") or "").strip()
    if legacy_recipient:
        recipients_provided = True
        recipients.append(legacy_recipient)
    return recipients, recipients_provided


def _resolve_reminder_recipient_usernames_for_actor(
    actor,
    recipients: list[str] | tuple[str, ...] | None,
) -> tuple[list[str], list[dict[str, str]]]:
    if actor is None:
        return [], [{"username": "", "reason": "authenticated user required"}]

    actor_username = str(getattr(actor, "username", "") or "").strip().lower()
    requested: list[str] = []
    seen: set[str] = set()
    for raw in recipients or []:
        username = str(raw or "").strip().lstrip("@").lower()
        if not username or username in seen:
            continue
        requested.append(username)
        seen.add(username)
    if not requested and actor_username:
        requested = [actor_username]

    User = get_user_model()
    lookup_usernames = [item for item in requested if item != actor_username]
    matches: list[object] = []
    if lookup_usernames:
        query = Q()
        for username in lookup_usernames:
            query |= Q(username__iexact=username)
        matches = list(User.objects.filter(query, is_active=True).order_by("id"))
    by_username = {
        str(getattr(item, "username", "") or "").strip().lower(): item
        for item in matches
        if str(getattr(item, "username", "") or "").strip()
    }

    valid: list[str] = []
    invalid: list[dict[str, str]] = []
    for username in requested:
        if username == actor_username:
            valid.append(username)
            continue
        target_user = by_username.get(username)
        if target_user is None:
            invalid.append({"username": username, "reason": "not_found"})
            continue
        if not _actor_can_contact_user(actor=actor, target_user=target_user):
            invalid.append({"username": username, "reason": "outside_team_scope"})
            continue
        resolved_username = str(getattr(target_user, "username", "") or "").strip().lower()
        if resolved_username:
            valid.append(resolved_username)
    deduped_valid: list[str] = []
    seen_valid: set[str] = set()
    for username in valid:
        if not username or username in seen_valid:
            continue
        seen_valid.add(username)
        deduped_valid.append(username)
    return deduped_valid, invalid


def _tool_set_reminder_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    title = str(args.get("title") or "").strip()
    if not title:
        return {"ok": False, "error": "title is required"}
    remind_at = str(args.get("remind_at") or args.get("when") or "").strip()
    if not remind_at:
        return {"ok": False, "error": "remind_at is required (ISO datetime)"}

    recipients, _recipients_provided = _parse_reminder_recipients_arg(args)
    valid_recipients, invalid_recipients = _resolve_reminder_recipient_usernames_for_actor(actor, recipients)
    if invalid_recipients:
        invalid_text = ", ".join(
            f"@{item.get('username') or '?'}({item.get('reason') or 'invalid'})"
            for item in invalid_recipients
        )
        return {
            "ok": False,
            "error": f"invalid recipients: {invalid_text}",
            "invalid_recipients": invalid_recipients,
        }

    channels = args.get("channels") if isinstance(args.get("channels"), dict) else None
    metadata = args.get("metadata") if isinstance(args.get("metadata"), dict) else None
    message = str(args.get("message") or "").strip()
    action = str(args.get("action") or "notify_user").strip()

    try:
        reminder = create_reminder(
            actor,
            title=title,
            remind_at=remind_at,
            message=message,
            recipients=valid_recipients,
            action=action,
            channels=channels,
            metadata=metadata,
            created_by_user_id=int(getattr(actor, "id", 0) or 0),
            created_by_username=str(getattr(actor, "username", "") or "").strip(),
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "error": f"unable to create reminder: {exc}"}

    try:
        add_ask_chat_context_event(
            actor,
            event_type="reminder_created",
            summary=f"Reminder '{title}' scheduled for {str(reminder.get('remind_at') or '').strip()}.",
            payload={
                "reminder_id": int(reminder.get("id") or 0),
                "title": str(reminder.get("title") or "").strip(),
                "remind_at": str(reminder.get("remind_at") or "").strip(),
                "recipients": reminder.get("recipients") if isinstance(reminder.get("recipients"), list) else [],
                "channels": reminder.get("channels") if isinstance(reminder.get("channels"), dict) else {},
            },
            conversation_id="default",
        )
    except Exception:
        pass

    return {
        "ok": True,
        "tool": "set_reminder",
        "reminder": reminder,
    }


def _tool_edit_reminder_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}
    try:
        reminder_id = int(args.get("reminder_id") or args.get("id") or 0)
    except Exception:
        reminder_id = 0
    if reminder_id <= 0:
        return {"ok": False, "error": "reminder_id must be positive"}

    existing = get_reminder(actor, reminder_id)
    if existing is None:
        return {"ok": False, "error": f"Reminder {reminder_id} not found"}

    update_payload: dict[str, Any] = {}
    if "title" in args:
        update_payload["title"] = str(args.get("title") or "").strip()
    if "remind_at" in args or "when" in args:
        update_payload["remind_at"] = str(args.get("remind_at") or args.get("when") or "").strip()
    if "message" in args:
        update_payload["message"] = str(args.get("message") or "").strip()
    if "action" in args:
        update_payload["action"] = str(args.get("action") or "").strip()
    if "channels" in args:
        channels = args.get("channels")
        update_payload["channels"] = channels if isinstance(channels, dict) else {}
    if "status" in args:
        update_payload["status"] = str(args.get("status") or "").strip().lower()
    if "metadata" in args:
        metadata = args.get("metadata")
        update_payload["metadata"] = metadata if isinstance(metadata, dict) else {}
    if "last_error" in args:
        update_payload["last_error"] = str(args.get("last_error") or "").strip()

    recipients, recipients_provided = _parse_reminder_recipients_arg(args)
    if recipients_provided:
        valid_recipients, invalid_recipients = _resolve_reminder_recipient_usernames_for_actor(actor, recipients)
        if invalid_recipients:
            invalid_text = ", ".join(
                f"@{item.get('username') or '?'}({item.get('reason') or 'invalid'})"
                for item in invalid_recipients
            )
            return {
                "ok": False,
                "error": f"invalid recipients: {invalid_text}",
                "invalid_recipients": invalid_recipients,
            }
        update_payload["recipients"] = valid_recipients

    if not update_payload:
        return {"ok": False, "error": "No updates provided"}

    try:
        reminder = update_reminder(actor, reminder_id, **update_payload)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "error": f"unable to update reminder: {exc}"}

    try:
        add_ask_chat_context_event(
            actor,
            event_type="reminder_updated",
            summary=f"Reminder '{str(reminder.get('title') or reminder_id)}' updated.",
            payload={
                "reminder_id": int(reminder.get("id") or 0),
                "updated_fields": sorted(list(update_payload.keys())),
                "status": str(reminder.get("status") or "").strip(),
                "remind_at": str(reminder.get("remind_at") or "").strip(),
            },
            conversation_id="default",
        )
    except Exception:
        pass

    return {
        "ok": True,
        "tool": "edit_reminder",
        "reminder": reminder,
    }


def _tool_delete_reminder_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}
    try:
        reminder_id = int(args.get("reminder_id") or args.get("id") or 0)
    except Exception:
        reminder_id = 0
    if reminder_id <= 0:
        return {"ok": False, "error": "reminder_id must be positive"}
    hard_delete = bool(args.get("hard_delete", False))

    try:
        reminder = delete_reminder(actor, reminder_id, hard_delete=hard_delete)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "error": f"unable to delete reminder: {exc}"}

    try:
        add_ask_chat_context_event(
            actor,
            event_type="reminder_deleted",
            summary=f"Reminder '{str(reminder.get('title') or reminder_id)}' deleted.",
            payload={
                "reminder_id": int(reminder.get("id") or 0),
                "hard_delete": hard_delete,
                "status": str(reminder.get("status") or "").strip(),
            },
            conversation_id="default",
        )
    except Exception:
        pass

    return {
        "ok": True,
        "tool": "delete_reminder",
        "reminder": reminder,
    }


def _tool_list_reminders_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}

    statuses_raw = args.get("statuses")
    status_raw = str(args.get("status") or "").strip().lower()
    statuses: list[str] = []
    if isinstance(statuses_raw, list):
        statuses = [str(item or "").strip().lower() for item in statuses_raw if str(item or "").strip()]
    elif isinstance(statuses_raw, str):
        statuses = [piece.strip().lower() for piece in re.split(r"[,\n;]", statuses_raw) if piece.strip()]
    if status_raw:
        statuses.append(status_raw)

    cleaned_statuses: list[str] = []
    invalid_statuses: list[str] = []
    seen_statuses: set[str] = set()
    for item in statuses:
        if item in seen_statuses:
            continue
        seen_statuses.add(item)
        if item not in REMINDER_VALID_STATUSES:
            invalid_statuses.append(item)
            continue
        cleaned_statuses.append(item)

    try:
        limit = int(args.get("limit", 100) or 100)
    except Exception:
        limit = 100
    resolved_limit = max(1, min(limit, 500))

    rows = list_reminders(
        actor,
        statuses=cleaned_statuses or None,
        limit=resolved_limit,
    )
    return {
        "ok": True,
        "tool": "list_reminders",
        "statuses": cleaned_statuses,
        "invalid_statuses": invalid_statuses,
        "limit": resolved_limit,
        "result_count": len(rows),
        "results": rows,
    }


def _tool_sms_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    body = str(args.get("message") or "").strip()
    if not body:
        return {"ok": False, "error": "message is required"}
    if len(body) > 320:
        return {"ok": False, "error": "sms_message_too_long_use_email"}
    body = body[:1200]

    target_username = str(args.get("username") or "").strip()
    target_phone_input = str(args.get("phone_number") or "").strip()
    if not target_username and not target_phone_input:
        return {"ok": False, "error": "either username or phone_number is required"}

    User = get_user_model()
    target_user = None
    target_phone = ""

    if target_username:
        target_user = User.objects.filter(username__iexact=target_username, is_active=True).first()
        if target_user is None:
            return {"ok": False, "error": f"user not found: {target_username}"}
        if not _actor_can_contact_user(actor=actor, target_user=target_user):
            return {"ok": False, "error": "contact not allowed: target user is outside your team scope"}
        target_phone_raw = (
            UserNotificationSettings.objects.filter(user=target_user)
            .values_list("phone_number", flat=True)
            .first()
            or ""
        )
        target_phone = _normalize_phone(str(target_phone_raw or ""))
        if not target_phone:
            return {"ok": False, "error": f"user has no phone number: {target_username}"}
    else:
        target_phone = _normalize_phone(target_phone_input)
        if not target_phone:
            return {"ok": False, "error": "invalid phone_number"}
        if not bool(getattr(actor, "is_superuser", False)):
            candidate_settings = UserNotificationSettings.objects.select_related("user").filter(user__is_active=True)
            matched_user = None
            for row in candidate_settings:
                row_phone = _normalize_phone(str(getattr(row, "phone_number", "") or ""))
                if row_phone and row_phone == target_phone:
                    matched_user = getattr(row, "user", None)
                    break
            if matched_user is None or not _actor_can_contact_user(actor=actor, target_user=matched_user):
                return {"ok": False, "error": "direct phone SMS is only allowed for users in your team scope"}
            target_user = matched_user

    sent, send_error = _send_invite_sms(
        to_number=target_phone,
        message=body,
    )
    if not sent:
        return {"ok": False, "error": send_error or "sms_send_failed"}
    return {
        "ok": True,
        "to": target_phone,
        "username": str(getattr(target_user, "username", "") or target_username).strip(),
        "user_id": int(getattr(target_user, "id", 0) or 0),
        "message_sent": True,
    }


def _parse_log_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tool_resource_logs_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}
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

    try:
        limit = int(args.get("limit", 200) or 200)
    except Exception:
        limit = 200
    resolved_limit = max(1, min(limit, 1000))
    level_filter = str(args.get("level") or "").strip().lower()
    contains_filter = str(args.get("contains") or "").strip().lower()
    try:
        since_minutes = int(args.get("since_minutes", 0) or 0)
    except Exception:
        since_minutes = 0
    resolved_since_minutes = max(0, min(since_minutes, 7 * 24 * 60))
    cutoff_dt = (
        datetime.now(timezone.utc) - timedelta(minutes=resolved_since_minutes)
        if resolved_since_minutes > 0
        else None
    )

    rows = list_resource_logs(owner_user, resource_uuid, limit=resolved_limit)
    filtered: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_level = str(row.get("level") or "").strip().lower()
        row_message = str(row.get("message") or "").strip()
        row_logger = str(row.get("logger") or "").strip()
        row_ts = str(row.get("timestamp") or "").strip()
        if level_filter and row_level != level_filter:
            continue
        if contains_filter:
            haystack = f"{row_message} {row_logger}".lower()
            if contains_filter not in haystack:
                continue
        if cutoff_dt is not None:
            parsed_ts = _parse_log_timestamp(row_ts)
            if parsed_ts is None or parsed_ts < cutoff_dt:
                continue
        filtered.append(row)

    return {
        "ok": True,
        "resource_uuid": resource_uuid,
        "resource_name": str(getattr(resource, "name", "") or ""),
        "owner_username": str(getattr(owner_user, "username", "") or ""),
        "limit": resolved_limit,
        "level": level_filter,
        "contains": contains_filter,
        "since_minutes": resolved_since_minutes,
        "result_count": len(filtered),
        "results": filtered,
    }


def _tool_alert_filter_prompt_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    action = str(args.get("action") or "get").strip().lower() or "get"
    if action in {"read"}:
        action = "get"
    if action in {"set"}:
        action = "replace"

    if action == "get":
        payload = get_user_alert_filter_prompt(actor)
        return {
            "ok": True,
            "action": "get",
            "prompt": str(payload.get("prompt") or ""),
            "updated_at": str(payload.get("updated_at") or ""),
        }
    if action not in {"replace", "append", "clear"}:
        return {"ok": False, "error": "action must be one of: get, replace, append, clear"}

    prompt = str(args.get("prompt") or "")
    try:
        payload = update_user_alert_filter_prompt(
            actor,
            prompt=prompt,
            mode=action,
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "action": action,
        "prompt": str(payload.get("prompt") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
    }


def _microsoft_delegated_access_token_for_user(user) -> tuple[str, str | None]:
    try:
        account = (
            SocialAccount.objects.filter(user=user, provider="microsoft")
            .order_by("id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        return "", None
    except Exception:
        return "", "Unable to load Microsoft account connection."

    if account is None:
        return "", "Microsoft is not connected for this user."

    try:
        token_row = (
            SocialToken.objects.filter(account=account)
            .exclude(token__exact="")
            .order_by("-id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        token_row = None
    except Exception:
        token_row = None

    access_token = str(getattr(token_row, "token", "") or "").strip()
    if access_token:
        return access_token, None
    return "", "Microsoft is connected, but the OAuth token is missing. Reconnect Microsoft from Settings."


def _microsoft_graph_send_mail_with_delegated_token(
    *,
    access_token: str,
    subject: str,
    body_text: str,
    to_addresses: list[str],
    cc_addresses: list[str] | None = None,
) -> tuple[bool, str]:
    token = str(access_token or "").strip()
    if not token:
        return False, "Microsoft token is not available."

    to_recipients = [
        {"emailAddress": {"address": str(address or "").strip()}}
        for address in to_addresses
        if str(address or "").strip()
    ]
    cc_recipients = [
        {"emailAddress": {"address": str(address or "").strip()}}
        for address in (cc_addresses or [])
        if str(address or "").strip()
    ]
    if not to_recipients:
        return False, "At least one recipient is required."

    payload: dict[str, object] = {
        "message": {
            "subject": str(subject or "").strip()[:255] or "(no subject)",
            "body": {
                "contentType": "Text",
                "content": str(body_text or ""),
            },
            "toRecipients": to_recipients,
        },
        "saveToSentItems": True,
    }
    if cc_recipients:
        message_obj = payload.get("message")
        if isinstance(message_obj, dict):
            message_obj["ccRecipients"] = cc_recipients

    try:
        response = requests.post(
            "https://graph.microsoft.com/v1.0/me/sendMail",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=payload,
            timeout=20,
        )
    except requests.RequestException:
        return False, "Unable to reach Microsoft Graph right now."

    if int(response.status_code) < 400:
        return True, ""

    try:
        parsed = response.json() if response.content else {}
    except ValueError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    error_obj = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
    error_message = str(error_obj.get("message") or "").strip()
    if int(response.status_code) in {401, 403}:
        return False, "Microsoft authorization expired. Reconnect Microsoft from Settings."
    if error_message:
        return False, f"Microsoft Graph API error: {error_message}"
    return False, f"Microsoft Graph API error (HTTP {int(response.status_code)})."


_OUTLOOK_MAIL_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _outlook_mail_parse_addresses(raw_value: object) -> list[str]:
    if isinstance(raw_value, list):
        items = [str(item or "").strip().lower() for item in raw_value]
    else:
        text = str(raw_value or "").strip()
        items = [piece.strip().lower() for piece in re.split(r"[;,]", text)] if text else []
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _outlook_mail_strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _outlook_mail_recipients(payload: object) -> list[str]:
    if not isinstance(payload, list):
        return []
    recipients: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        address_obj = item.get("emailAddress") if isinstance(item.get("emailAddress"), dict) else {}
        address = str(address_obj.get("address") or "").strip().lower()
        if address:
            recipients.append(address)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in recipients:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _outlook_mail_normalize_graph_message(item: dict, *, default_folder: str = "inbox") -> dict:
    body_obj = item.get("body") if isinstance(item.get("body"), dict) else {}
    body_type = str(body_obj.get("contentType") or "").strip().lower()
    body_content = str(body_obj.get("content") or "").strip()
    body_text = _outlook_mail_strip_html(body_content) if body_type == "html" else re.sub(r"\s+", " ", body_content).strip()
    body_preview = re.sub(r"\s+", " ", str(item.get("bodyPreview") or "")).strip()
    if not body_preview and body_text:
        body_preview = body_text[:320].strip()
    sender_obj = item.get("from") if isinstance(item.get("from"), dict) else {}
    sender_email_obj = sender_obj.get("emailAddress") if isinstance(sender_obj.get("emailAddress"), dict) else {}

    return {
        "message_id": str(item.get("id") or "").strip(),
        "folder": str(default_folder or "inbox").strip().lower() or "inbox",
        "internet_message_id": str(item.get("internetMessageId") or "").strip(),
        "conversation_id": str(item.get("conversationId") or "").strip(),
        "subject": str(item.get("subject") or "").strip(),
        "sender_email": str(sender_email_obj.get("address") or "").strip().lower(),
        "sender_name": str(sender_email_obj.get("name") or "").strip(),
        "to_recipients": _outlook_mail_recipients(item.get("toRecipients")),
        "cc_recipients": _outlook_mail_recipients(item.get("ccRecipients")),
        "received_at": str(item.get("receivedDateTime") or "").strip(),
        "sent_at": str(item.get("sentDateTime") or "").strip(),
        "body_preview": body_preview,
        "body_text": body_text,
        "web_link": str(item.get("webLink") or "").strip(),
        "is_read": bool(item.get("isRead")),
        "has_attachments": bool(item.get("hasAttachments")),
        "raw_payload": item,
    }


def _outlook_mail_document(row: dict) -> str:
    return "\n".join(
        [
            f"From: {str(row.get('sender_email') or '').strip() or 'unknown'}",
            f"Subject: {str(row.get('subject') or '').strip() or '(no subject)'}",
            f"Received: {str(row.get('received_at') or '').strip() or '(unknown)'}",
            "",
            str(row.get("body_text") or row.get("body_preview") or "").strip(),
        ]
    ).strip()


def _outlook_mail_context_hash(row: dict) -> str:
    to_recipients = row.get("to_recipients") if isinstance(row.get("to_recipients"), list) else []
    cc_recipients = row.get("cc_recipients") if isinstance(row.get("cc_recipients"), list) else []
    normalized_to = sorted({str(item or "").strip().lower() for item in to_recipients if str(item or "").strip()})
    normalized_cc = sorted({str(item or "").strip().lower() for item in cc_recipients if str(item or "").strip()})
    payload = {
        "folder": str(row.get("folder") or "inbox").strip().lower() or "inbox",
        "internet_message_id": str(row.get("internet_message_id") or "").strip(),
        "conversation_id": str(row.get("conversation_id") or "").strip(),
        "subject": str(row.get("subject") or "").strip(),
        "sender_email": str(row.get("sender_email") or "").strip().lower(),
        "sender_name": str(row.get("sender_name") or "").strip(),
        "to_recipients": normalized_to,
        "cc_recipients": normalized_cc,
        "received_at": str(row.get("received_at") or "").strip(),
        "sent_at": str(row.get("sent_at") or "").strip(),
        "body_preview": str(row.get("body_preview") or "").strip(),
        "body_text": str(row.get("body_text") or "").strip(),
        "web_link": str(row.get("web_link") or "").strip(),
        "has_attachments": bool(row.get("has_attachments")),
    }
    return _stable_json_hash(payload)


def _outlook_mail_index_for_actor(actor, rows: list[dict]) -> tuple[int, str]:
    if actor is None or not rows:
        return 0, ""
    _ensure_runtime_cache_dirs()
    try:
        import chromadb
    except Exception:
        return 0, "chromadb package is not installed"

    knowledge_path = _user_knowledge_db_path(actor)
    try:
        client = chromadb.PersistentClient(path=str(knowledge_path))
        collection = client.get_or_create_collection(name="outlook_mail")
    except Exception as exc:
        return 0, str(exc)

    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict[str, object]] = []
    row_hashes: list[str] = []
    for row in rows:
        message_id = str(row.get("message_id") or "").strip()
        if not message_id:
            continue
        context_hash = _outlook_mail_context_hash(row)
        ids.append(message_id)
        docs.append(_outlook_mail_document(row))
        row_hashes.append(context_hash)
        metas.append(
            {
                "source": "outlook_mail",
                "message_id": message_id,
                "subject": str(row.get("subject") or "").strip(),
                "sender_email": str(row.get("sender_email") or "").strip().lower(),
                "sender_name": str(row.get("sender_name") or "").strip(),
                "received_at": str(row.get("received_at") or "").strip(),
                "body_preview": str(row.get("body_preview") or "").strip(),
                "conversation_id": str(row.get("conversation_id") or "").strip(),
                "web_link": str(row.get("web_link") or "").strip(),
                "has_attachments": bool(row.get("has_attachments")),
                "mail_context_hash": context_hash,
            }
        )
    if not ids:
        return 0, ""
    existing_hashes = _collection_metadata_values(
        collection,
        record_ids=ids,
        key="mail_context_hash",
    )
    filtered_ids: list[str] = []
    filtered_docs: list[str] = []
    filtered_metas: list[dict[str, object]] = []
    for idx, item_id in enumerate(ids):
        current_hash = row_hashes[idx] if idx < len(row_hashes) else ""
        if existing_hashes.get(item_id, "") == current_hash:
            continue
        filtered_ids.append(item_id)
        filtered_docs.append(docs[idx] if idx < len(docs) else "")
        filtered_metas.append(metas[idx] if idx < len(metas) else {})
    if not filtered_ids:
        return 0, ""
    try:
        collection.upsert(ids=filtered_ids, documents=filtered_docs, metadatas=filtered_metas)
    except Exception as exc:
        return 0, str(exc)
    return len(filtered_ids), ""


def _outlook_mail_vector_search_for_actor(actor, *, query: str, limit: int) -> tuple[list[dict], str]:
    if actor is None:
        return [], "authenticated user identity is required"
    resolved_query = str(query or "").strip()
    if not resolved_query:
        return [], ""
    _ensure_runtime_cache_dirs()
    try:
        import chromadb
    except Exception:
        return [], "chromadb package is not installed"

    knowledge_path = _user_knowledge_db_path(actor)
    if not knowledge_path.exists():
        return [], ""
    try:
        client = chromadb.PersistentClient(path=str(knowledge_path))
        collection = client.get_collection(name="outlook_mail")
    except Exception:
        return [], ""

    resolved_limit = max(1, min(int(limit or 20), 100))
    try:
        payload = collection.query(
            query_texts=[resolved_query],
            n_results=resolved_limit,
        )
    except Exception as exc:
        return [], f"chroma query failed: {exc}"

    ids = (payload.get("ids") or [[]])[0]
    docs = (payload.get("documents") or [[]])[0]
    metas = (payload.get("metadatas") or [[]])[0]
    dists = (payload.get("distances") or [[]])[0]
    rows: list[dict] = []
    for idx, item_id in enumerate(ids):
        metadata = metas[idx] if idx < len(metas) and isinstance(metas[idx], dict) else {}
        rows.append(
            {
                "message_id": str(item_id or ""),
                "document": str(docs[idx] or "") if idx < len(docs) else "",
                "distance": dists[idx] if idx < len(dists) else None,
                "metadata": metadata,
            }
        )
    return rows, ""


def _microsoft_graph_list_messages_with_delegated_token(
    *,
    access_token: str,
    folder: str = "inbox",
    limit: int = 80,
    include_body: bool = False,
) -> tuple[list[dict], str]:
    token = str(access_token or "").strip()
    if not token:
        return [], "Microsoft token is not available."

    resolved_folder = str(folder or "inbox").strip().lower() or "inbox"
    folder_path_map = {
        "inbox": "Inbox",
        "sent": "SentItems",
        "sentitems": "SentItems",
        "drafts": "Drafts",
        "archive": "Archive",
        "deleted": "DeletedItems",
        "deleteditems": "DeletedItems",
        "junk": "JunkEmail",
        "junkemail": "JunkEmail",
        "all": "",
        "any": "",
    }
    folder_segment = folder_path_map.get(resolved_folder, "")
    endpoint = "https://graph.microsoft.com/v1.0/me/messages"
    default_folder = "inbox"
    if folder_segment:
        endpoint = f"https://graph.microsoft.com/v1.0/me/mailFolders/{folder_segment}/messages"
        default_folder = resolved_folder
    elif resolved_folder in {"all", "any"}:
        default_folder = "all"

    select_fields = [
        "id",
        "internetMessageId",
        "conversationId",
        "subject",
        "receivedDateTime",
        "sentDateTime",
        "bodyPreview",
        "hasAttachments",
        "isRead",
        "from",
        "toRecipients",
        "ccRecipients",
        "webLink",
    ]
    if include_body:
        select_fields.append("body")
    query_params = {
        "$top": max(1, min(int(limit or 80), 200)),
        "$orderby": "receivedDateTime desc",
        "$select": ",".join(select_fields),
    }
    try:
        response = requests.get(
            endpoint,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            params=query_params,
            timeout=20,
        )
    except requests.RequestException:
        return [], "Unable to reach Microsoft Graph right now."
    if int(response.status_code) >= 400:
        try:
            parsed = response.json() if response.content else {}
        except ValueError:
            parsed = {}
        if not isinstance(parsed, dict):
            parsed = {}
        error_obj = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
        error_message = str(error_obj.get("message") or "").strip()
        if int(response.status_code) in {401, 403}:
            return [], "Microsoft authorization expired. Reconnect Microsoft from Settings."
        if error_message:
            return [], f"Microsoft Graph API error: {error_message}"
        return [], f"Microsoft Graph API error (HTTP {int(response.status_code)})."

    payload = response.json() if response.content else {}
    rows: list[dict] = []
    for item in (payload.get("value") or []):
        if not isinstance(item, dict):
            continue
        normalized = _outlook_mail_normalize_graph_message(item, default_folder=default_folder)
        if str(normalized.get("message_id") or "").strip():
            rows.append(normalized)
    return rows, ""


def _microsoft_graph_read_message_with_delegated_token(
    *,
    access_token: str,
    message_id: str,
) -> tuple[dict | None, str]:
    token = str(access_token or "").strip()
    resolved_message_id = str(message_id or "").strip()
    if not token:
        return None, "Microsoft token is not available."
    if not resolved_message_id:
        return None, "message_id is required"

    endpoint = f"https://graph.microsoft.com/v1.0/me/messages/{resolved_message_id}"
    query_params = {
        "$select": ",".join(
            [
                "id",
                "internetMessageId",
                "conversationId",
                "subject",
                "receivedDateTime",
                "sentDateTime",
                "bodyPreview",
                "body",
                "hasAttachments",
                "isRead",
                "from",
                "toRecipients",
                "ccRecipients",
                "webLink",
            ]
        )
    }
    try:
        response = requests.get(
            endpoint,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            params=query_params,
            timeout=20,
        )
    except requests.RequestException:
        return None, "Unable to reach Microsoft Graph right now."
    if int(response.status_code) >= 400:
        try:
            parsed = response.json() if response.content else {}
        except ValueError:
            parsed = {}
        if not isinstance(parsed, dict):
            parsed = {}
        error_obj = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
        error_message = str(error_obj.get("message") or "").strip()
        if int(response.status_code) in {401, 403}:
            return None, "Microsoft authorization expired. Reconnect Microsoft from Settings."
        if int(response.status_code) == 404:
            return None, "message not found"
        if error_message:
            return None, f"Microsoft Graph API error: {error_message}"
        return None, f"Microsoft Graph API error (HTTP {int(response.status_code)})."

    payload = response.json() if response.content else {}
    if not isinstance(payload, dict):
        return None, "invalid Microsoft Graph payload"
    normalized = _outlook_mail_normalize_graph_message(payload, default_folder="inbox")
    if not str(normalized.get("message_id") or "").strip():
        return None, "message not found"
    return normalized, ""


def _parse_ymd_date(value: str) -> datetime | None:
    resolved = str(value or "").strip()
    if not resolved:
        return None
    try:
        return datetime.strptime(resolved, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _outlook_calendar_row_sort_key(row: dict) -> tuple[str, str, str]:
    due_date = str(row.get("due_date") or "").strip()
    due_time = str(row.get("due_time") or "").strip()
    title = str(row.get("title") or "").strip().lower()
    if not due_date:
        return ("9999-12-31", "99:99", title)
    return (due_date, due_time or "99:99", title)


def _outlook_calendar_context_line(row: dict) -> str:
    title = str(row.get("title") or "").strip() or "Untitled event"
    due_date = str(row.get("due_date") or "").strip()
    due_time = str(row.get("due_time") or "").strip()
    status = str(row.get("status") or "").strip().lower() or ("completed" if bool(row.get("is_completed")) else "open")
    when = due_date if due_date else "unscheduled"
    if due_time:
        when = f"{when} {due_time}"
    return f"[outlook] {title} | {when} | {status}"


def _tool_outlook_mail_for_actor(
    actor,
    args: dict,
    *,
    conversation_id: str = "",
    channel: str = "web_chat",
) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    action = str(args.get("action") or "search").strip().lower() or "search"
    if action in {"list", "inbox", "query"}:
        action = "search"
    if action in {"get"}:
        action = "read"
    if action in {"compose"}:
        action = "send"
    if action not in {"search", "read", "send"}:
        return {"ok": False, "error": "action must be one of: search, read, send"}

    if action == "send":
        send_mode = str(args.get("send_mode") or args.get("auth_mode") or args.get("sender") or "").strip().lower()
        if send_mode in {"support", "support_inbox", "shared", "app"}:
            to_addresses = _outlook_mail_parse_addresses(args.get("to"))
            if len(to_addresses) != 1:
                return {"ok": False, "error": "support_inbox send requires exactly one recipient email"}
            return _tool_support_inbox_send_mail_for_actor(
                actor,
                {
                    "to": to_addresses[0],
                    "subject": str(args.get("subject") or "").strip(),
                    "body": str(args.get("body") or "").strip(),
                },
                conversation_id=conversation_id,
                channel=channel,
            )

        access_token, token_error = _microsoft_delegated_access_token_for_user(actor)
        if not access_token:
            return {"ok": False, "error": token_error or "Microsoft is not connected for this user."}

        to_addresses = _outlook_mail_parse_addresses(args.get("to"))
        cc_addresses = _outlook_mail_parse_addresses(args.get("cc"))
        if not to_addresses:
            return {"ok": False, "error": "to is required"}
        all_addresses = list(dict.fromkeys(to_addresses + cc_addresses))
        invalid_addresses = [address for address in all_addresses if not _OUTLOOK_MAIL_EMAIL_RE.match(address)]
        if invalid_addresses:
            return {"ok": False, "error": f"invalid recipient email(s): {', '.join(invalid_addresses[:5])}"}
        if len(all_addresses) > 25:
            return {"ok": False, "error": "too many recipients (max 25)"}

        subject = str(args.get("subject") or "").strip()
        body = str(args.get("body") or "").strip()
        if not body:
            return {"ok": False, "error": "body is required"}
        if len(body) > 10000:
            body = body[:10000]

        sent, error = _microsoft_graph_send_mail_with_delegated_token(
            access_token=access_token,
            subject=subject,
            body_text=body,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
        )
        if not sent:
            return {"ok": False, "error": error or "Unable to send email right now."}
        return {
            "ok": True,
            "tool": "outlook_mail",
            "action": "send",
            "provider": "microsoft",
            "auth_mode": "delegated",
            "recipient_count": len(all_addresses),
            "to_recipients": to_addresses,
            "cc_recipients": cc_addresses,
            "subject": subject[:255],
            "sent": True,
        }

    access_token, token_error = _microsoft_delegated_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": token_error or "Microsoft is not connected for this user."}

    if action == "read":
        message_id = str(args.get("message_id") or args.get("id") or "").strip()
        if not message_id:
            return {"ok": False, "error": "message_id is required for action=read"}
        message_row, read_error = _microsoft_graph_read_message_with_delegated_token(
            access_token=access_token,
            message_id=message_id,
        )
        from_cache = False
        if message_row is None:
            cached = get_user_outlook_mail_cache_message(actor, message_id=message_id)
            if cached is None:
                return {"ok": False, "error": read_error or "message not found"}
            message_row = cached
            from_cache = True
        else:
            upsert_user_outlook_mail_cache(actor, messages=[message_row])
            _outlook_mail_index_for_actor(actor, [message_row])
        return {
            "ok": True,
            "tool": "outlook_mail",
            "action": "read",
            "auth_mode": "delegated",
            "from_cache": from_cache,
            "member_db_path": str(_user_db_path(actor)),
            "knowledge_path": str(_user_knowledge_db_path(actor)),
            "message": message_row,
        }

    query = str(args.get("query") or "").strip()
    folder = str(args.get("folder") or "inbox").strip().lower() or "inbox"
    refresh = bool(args.get("refresh", True))
    include_body = bool(args.get("include_body", False))
    try:
        limit = int(args.get("limit", 12) or 12)
    except Exception:
        limit = 12
    resolved_limit = max(1, min(limit, 50))

    refresh_error = ""
    fetched_rows: list[dict] = []
    indexed_count = 0
    if refresh:
        fetch_limit = max(resolved_limit * 4, 60)
        fetched_rows, refresh_error = _microsoft_graph_list_messages_with_delegated_token(
            access_token=access_token,
            folder=folder,
            limit=fetch_limit,
            include_body=bool(include_body or query),
        )
        if fetched_rows:
            upsert_user_outlook_mail_cache(actor, messages=fetched_rows)
            indexed_count, _index_error = _outlook_mail_index_for_actor(actor, fetched_rows)

    cache_scan_limit = max(resolved_limit * 4, 80)
    cached_rows = list_user_outlook_mail_cache(
        actor,
        query=query,
        limit=cache_scan_limit,
        folder=folder,
        include_body=bool(include_body or query),
    )
    cached_by_id = {
        str(row.get("message_id") or "").strip(): row
        for row in cached_rows
        if str(row.get("message_id") or "").strip()
    }
    vector_rows: list[dict] = []
    vector_error = ""
    if query:
        vector_rows, vector_error = _outlook_mail_vector_search_for_actor(
            actor,
            query=query,
            limit=cache_scan_limit,
        )

    merged: list[dict] = []
    seen_ids: set[str] = set()
    for row in vector_rows:
        message_id = str(row.get("message_id") or "").strip()
        if not message_id or message_id in seen_ids:
            continue
        seen_ids.add(message_id)
        if message_id in cached_by_id:
            merged_row = dict(cached_by_id[message_id])
            merged_row["match_source"] = "vector"
            merged_row["distance"] = row.get("distance")
            merged.append(merged_row)
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        merged.append(
            {
                "message_id": message_id,
                "subject": str(metadata.get("subject") or "").strip(),
                "sender_email": str(metadata.get("sender_email") or "").strip().lower(),
                "sender_name": str(metadata.get("sender_name") or "").strip(),
                "received_at": str(metadata.get("received_at") or "").strip(),
                "body_preview": str(metadata.get("body_preview") or "").strip(),
                "conversation_id": str(metadata.get("conversation_id") or "").strip(),
                "web_link": str(metadata.get("web_link") or "").strip(),
                "has_attachments": bool(metadata.get("has_attachments")),
                "is_read": bool(metadata.get("is_read")),
                "match_source": "vector",
                "distance": row.get("distance"),
            }
        )
    for row in cached_rows:
        message_id = str(row.get("message_id") or "").strip()
        if not message_id or message_id in seen_ids:
            continue
        seen_ids.add(message_id)
        merged_row = dict(row)
        merged_row["match_source"] = "cache"
        merged_row["distance"] = None
        merged.append(merged_row)

    return {
        "ok": True,
        "tool": "outlook_mail",
        "action": "search",
        "auth_mode": "delegated",
        "query": query,
        "folder": folder,
        "refresh": refresh,
        "refresh_error": refresh_error,
        "vector_error": vector_error,
        "limit": resolved_limit,
        "fetched_count": len(fetched_rows),
        "indexed_count": indexed_count,
        "cached_count": len(cached_rows),
        "vector_result_count": len(vector_rows),
        "result_count": len(merged[:resolved_limit]),
        "member_db_path": str(_user_db_path(actor)),
        "knowledge_path": str(_user_knowledge_db_path(actor)),
        "results": merged[:resolved_limit],
    }


def _tool_outlook_calendar_for_actor(actor, args: dict) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}

    query = str(args.get("query") or "").strip().lower()
    start_date = str(args.get("start_date") or "").strip()
    end_date = str(args.get("end_date") or "").strip()
    include_completed = bool(args.get("include_completed", False))
    include_unscheduled = bool(args.get("include_unscheduled", False))
    refresh_requested = bool(args.get("refresh", True))
    try:
        limit = int(args.get("limit", 80) or 80)
    except Exception:
        limit = 80
    resolved_limit = max(1, min(limit, 500))

    start_dt = _parse_ymd_date(start_date)
    end_dt = _parse_ymd_date(end_date)
    if start_date and start_dt is None:
        return {"ok": False, "error": "start_date must be YYYY-MM-DD", "results": []}
    if end_date and end_dt is None:
        return {"ok": False, "error": "end_date must be YYYY-MM-DD", "results": []}
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        return {"ok": False, "error": "end_date must be on/after start_date", "results": []}

    refresh_result: dict = {}
    refresh_error = ""
    refresh_applied = False
    if refresh_requested:
        try:
            refresh_result = refresh_calendar_cache_for_user(
                actor,
                provider="outlook",
                force=False,
            )
            provider_result = refresh_result.get("outlook") if isinstance(refresh_result, dict) else {}
            refresh_applied = bool(provider_result.get("refresh_attempted")) if isinstance(provider_result, dict) else False
        except Exception as exc:
            refresh_error = str(exc)

    rows = list_user_calendar_event_cache(
        actor,
        provider="outlook",
        limit=5000,
        include_completed=include_completed,
    )
    filtered: list[dict] = []
    for row in rows:
        due_date = str(row.get("due_date") or "").strip()
        due_time = str(row.get("due_time") or "").strip()
        title = str(row.get("title") or "").strip()
        status = str(row.get("status") or "").strip().lower()
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        is_completed = bool(row.get("is_completed"))

        if not include_unscheduled and not due_date:
            continue

        due_dt = _parse_ymd_date(due_date) if due_date else None
        if start_dt is not None and (due_dt is None or due_dt < start_dt):
            continue
        if end_dt is not None and (due_dt is None or due_dt > end_dt):
            continue
        if query:
            haystack = " ".join(
                [
                    title,
                    status,
                    due_date,
                    due_time,
                    str(row.get("source_url") or ""),
                    str(payload),
                ]
            ).lower()
            if query not in haystack:
                continue

        filtered.append(
            {
                "provider": "outlook",
                "event_id": str(row.get("event_id") or "").strip(),
                "title": title,
                "due_date": due_date,
                "due_time": due_time,
                "is_completed": is_completed,
                "status": status or ("completed" if is_completed else "open"),
                "source_url": str(row.get("source_url") or "").strip(),
                "payload": payload,
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
        )

    filtered.sort(key=_outlook_calendar_row_sort_key)
    limited = filtered[:resolved_limit]

    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    next_7d_key = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")
    completed_count = 0
    open_count = 0
    overdue_open_count = 0
    today_open_count = 0
    next_7d_open_count = 0
    for row in filtered:
        is_completed = bool(row.get("is_completed"))
        due_date = str(row.get("due_date") or "").strip()
        if is_completed:
            completed_count += 1
            continue
        open_count += 1
        if due_date:
            if due_date < today_key:
                overdue_open_count += 1
            if due_date == today_key:
                today_open_count += 1
            if today_key <= due_date <= next_7d_key:
                next_7d_open_count += 1

    return {
        "ok": True,
        "tool": "outlook_calendar",
        "provider": "outlook",
        "query": str(args.get("query") or ""),
        "start_date": start_date,
        "end_date": end_date,
        "refresh_requested": refresh_requested,
        "refresh_applied": refresh_applied,
        "refresh_error": refresh_error,
        "refresh_result": refresh_result,
        "include_completed": include_completed,
        "include_unscheduled": include_unscheduled,
        "limit": resolved_limit,
        "result_count": len(limited),
        "total_filtered_count": len(filtered),
        "summary": {
            "open_count": open_count,
            "completed_count": completed_count,
            "overdue_open_count": overdue_open_count,
            "today_open_count": today_open_count,
            "next_7d_open_count": next_7d_open_count,
        },
        "context_lines": [_outlook_calendar_context_line(row) for row in limited[:50]],
        "results": limited,
    }


def _tool_support_inbox_send_mail_for_actor(
    actor,
    args: dict,
    *,
    conversation_id: str = "",
    channel: str = "web_chat",
) -> dict:
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    to_address = str(args.get("to") or "").strip().lower()
    if not to_address:
        return {"ok": False, "error": "to is required"}
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", to_address):
        return {"ok": False, "error": "invalid recipient email"}

    subject = str(args.get("subject") or "").strip()
    body = str(args.get("body") or "").strip()
    if not body:
        return {"ok": False, "error": "body is required"}
    if len(body) > 10000:
        body = body[:10000]

    sent, error = send_support_inbox_email(
        recipient_email=to_address,
        subject=subject,
        body_text=body,
        initiated_by_user_id=int(getattr(actor, "id", 0) or 0),
        initiated_by_username=str(getattr(actor, "username", "") or "").strip(),
        initiated_by_email=str(getattr(actor, "email", "") or "").strip().lower(),
        initiated_by_channel=str(channel or "").strip() or "web_chat",
        initiated_by_conversation_id=str(conversation_id or "").strip(),
    )
    if not sent:
        return {"ok": False, "error": error or "Unable to send support inbox email right now."}
    return {
        "ok": True,
        "provider": "microsoft",
        "auth_mode": "support_inbox_app",
        "to_recipient": to_address,
        "subject": subject[:255],
        "sent": True,
        "initiated_by_user_id": int(getattr(actor, "id", 0) or 0),
        "initiated_by_username": str(getattr(actor, "username", "") or "").strip(),
        "initiated_by_channel": str(channel or "").strip() or "web_chat",
        "initiated_by_conversation_id": str(conversation_id or "").strip(),
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
    used_tool_names = {
        "search_kb",
        "alert_filter_prompt",
        "search_users",
        "directory",
        "sms",
        "resource_health_check",
        "resource_logs",
        "resource_kb",
        "resource_ssh_exec",
        "outlook_mail",
        "outlook_calendar",
        "set_reminder",
        "edit_reminder",
        "delete_reminder",
        "list_reminders",
    }
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
                "name": "alert_filter_prompt",
                "description": "Read or update your alert filtering prompt (used to decide if alerts are sent).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "description": "One of: get, replace, append, clear.",
                        },
                        "prompt": {
                            "type": "string",
                            "description": "Prompt text to store when action is replace or append.",
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_users",
                "description": "Search user records by similarity and/or phone number (superuser only).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "phone": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "directory",
                "description": "Look up users you are allowed to contact (team-scoped unless superuser).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "username": {"type": "string"},
                        "email": {"type": "string"},
                        "phone": {"type": "string"},
                        "limit": {"type": "integer"},
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
        {
            "type": "function",
            "function": {
                "name": "resource_logs",
                "description": "Query structured logs for an accessible resource (supports recency filter).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "resource_uuid": {"type": "string"},
                        "limit": {"type": "integer"},
                        "level": {"type": "string"},
                        "contains": {"type": "string"},
                        "since_minutes": {"type": "integer"},
                    },
                    "required": ["resource_uuid"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "resource_kb",
                "description": "Search the resource-scoped knowledge base for a specific resource.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "resource_uuid": {"type": "string"},
                        "query": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["resource_uuid"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "resource_ssh_exec",
                "description": "Execute a one-shot SSH command on an accessible VM resource.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "resource_uuid": {"type": "string"},
                        "command": {"type": "string"},
                        "timeout_seconds": {"type": "integer"},
                        "max_output_chars": {"type": "integer"},
                    },
                    "required": ["resource_uuid", "command"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "sms",
                "description": (
                    "Send SMS through Twilio to a team-contactable user (or direct phone when allowed). "
                    "Use for short summaries only."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "message": {"type": "string"},
                        "username": {"type": "string"},
                        "phone_number": {"type": "string"},
                    },
                    "required": ["message"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "set_reminder",
                "description": "Create a reminder for yourself or teammates in your contact scope.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "remind_at": {
                            "type": "string",
                            "description": "ISO datetime for when to dispatch (UTC recommended).",
                        },
                        "message": {"type": "string"},
                        "recipients": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Usernames (without @) to receive the reminder. Defaults to yourself.",
                        },
                        "action": {
                            "type": "string",
                            "description": "Reminder action. Use notify_user.",
                        },
                        "channels": {
                            "type": "object",
                            "description": "Channel map, e.g. {APP:true,SMS:false,EMAIL:true}.",
                        },
                        "metadata": {
                            "type": "object",
                            "description": "Optional context for reminder dispatch (resource_uuid, wiki_query, etc).",
                        },
                    },
                    "required": ["title", "remind_at"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "edit_reminder",
                "description": "Update an existing reminder (time, recipients, channels, status, or message).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reminder_id": {"type": "integer"},
                        "title": {"type": "string"},
                        "remind_at": {"type": "string"},
                        "message": {"type": "string"},
                        "recipients": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "action": {"type": "string"},
                        "channels": {"type": "object"},
                        "status": {
                            "type": "string",
                            "description": "Optional status override: scheduled, sent, canceled, error.",
                        },
                        "metadata": {"type": "object"},
                        "last_error": {"type": "string"},
                    },
                    "required": ["reminder_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "delete_reminder",
                "description": "Cancel (default) or hard-delete a reminder.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reminder_id": {"type": "integer"},
                        "hard_delete": {"type": "boolean"},
                    },
                    "required": ["reminder_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_reminders",
                "description": "List reminders, optionally filtered by status.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "statuses": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Optional status filters: scheduled, sent, canceled, error.",
                        },
                        "status": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "outlook_mail",
                "description": (
                    "Unified Outlook mail tool for delegated inbox operations. "
                    "Use action=search to search/read from cached+indexed inbox mail, "
                    "action=read for one message, and action=send to send email."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "description": "One of: search, read, send. Default: search.",
                        },
                        "query": {
                            "type": "string",
                            "description": "Search phrase for inbox lookup (semantic + cached search).",
                        },
                        "folder": {
                            "type": "string",
                            "description": "Mailbox folder for search. Usually inbox, sentitems, drafts, archive, all.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum messages/events to return.",
                        },
                        "refresh": {
                            "type": "boolean",
                            "description": "When true, refreshes inbox from Microsoft before searching cache.",
                        },
                        "include_body": {
                            "type": "boolean",
                            "description": "Include fuller message body text for search/read context.",
                        },
                        "message_id": {
                            "type": "string",
                            "description": "Required for action=read.",
                        },
                        "to": {
                            "description": "One or more recipient email addresses (comma-separated) for action=send.",
                            "type": "string",
                        },
                        "cc": {
                            "description": "Optional CC recipients (comma-separated) for action=send.",
                            "type": "string",
                        },
                        "subject": {
                            "type": "string",
                            "description": "Email subject for action=send.",
                        },
                        "body": {
                            "type": "string",
                            "description": "Email body text for action=send.",
                        },
                        "send_mode": {
                            "type": "string",
                            "description": "For action=send: delegated (default) or support_inbox.",
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "outlook_calendar",
                "description": "Query Outlook calendar/task cache for the current user.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Optional text filter against event title/status/metadata.",
                        },
                        "start_date": {
                            "type": "string",
                            "description": "Optional YYYY-MM-DD start date filter.",
                        },
                        "end_date": {
                            "type": "string",
                            "description": "Optional YYYY-MM-DD end date filter.",
                        },
                        "refresh": {
                            "type": "boolean",
                            "description": "When true, refresh Outlook provider cache before filtering.",
                        },
                        "include_completed": {
                            "type": "boolean",
                            "description": "Include completed events/tasks.",
                        },
                        "include_unscheduled": {
                            "type": "boolean",
                            "description": "Include events without a due date.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum events to return.",
                        },
                    },
                    "required": [],
                },
            },
        },
    ]
    if extra_tools:
        tools.extend(extra_tools)
    return tools


def _run_ask_tool_for_actor(
    *,
    actor,
    tool_name: str,
    args: dict,
    conversation_id: str = "",
    channel: str = "web_chat",
) -> dict:
    if tool_name == "search_kb":
        return _tool_search_kb_for_actor(actor, args)
    if tool_name == "alert_filter_prompt":
        return _tool_alert_filter_prompt_for_actor(actor, args)
    if tool_name == "search_users":
        return _tool_search_users_for_actor(actor, args)
    if tool_name == "directory":
        return _tool_directory_for_actor(actor, args)
    if tool_name == "resource_health_check":
        return _tool_resource_health_check_for_actor(actor, args)
    if tool_name == "resource_logs":
        return _tool_resource_logs_for_actor(actor, args)
    if tool_name == "resource_kb":
        return _tool_resource_kb_for_actor(actor, args)
    if tool_name == "resource_ssh_exec":
        return _tool_resource_ssh_exec_for_actor(actor, args)
    if tool_name == "set_reminder":
        return _tool_set_reminder_for_actor(actor, args)
    if tool_name == "edit_reminder":
        return _tool_edit_reminder_for_actor(actor, args)
    if tool_name == "delete_reminder":
        return _tool_delete_reminder_for_actor(actor, args)
    if tool_name == "list_reminders":
        return _tool_list_reminders_for_actor(actor, args)
    if tool_name == "sms":
        return _tool_sms_for_actor(actor, args)
    if tool_name == "outlook_mail":
        return _tool_outlook_mail_for_actor(
            actor,
            args,
            conversation_id=conversation_id,
            channel=channel,
        )
    if tool_name == "outlook_calendar":
        return _tool_outlook_calendar_for_actor(actor, args)
    if tool_name == "microsoft_send_mail":
        legacy_args = dict(args or {})
        legacy_args.setdefault("action", "send")
        return _tool_outlook_mail_for_actor(
            actor,
            legacy_args,
            conversation_id=conversation_id,
            channel=channel,
        )
    if tool_name == "support_inbox_send_mail":
        legacy_args = dict(args or {})
        legacy_args.setdefault("action", "send")
        legacy_args.setdefault("send_mode", "support_inbox")
        return _tool_outlook_mail_for_actor(
            actor,
            legacy_args,
            conversation_id=conversation_id,
            channel=channel,
        )
    if tool_name == "calendar_context":
        return _tool_outlook_calendar_for_actor(actor, args)
    return {"ok": False, "error": f"unknown tool: {tool_name}"}


def _default_ask_mcp_tool_lines() -> list[str]:
    return [
        "- search_kb(query): searches personal KB (top 4), global KB (top 3), and accessible Workspace Wiki matches (top 4).",
        "  Note: when asked a question, use search_kb first to pinpoint which specific resources are relevant.",
        "- alert_filter_prompt(action?, prompt?): reads/updates your personal alert filtering prompt used before sending alerts.",
        "  Note: use action=append to add a preference like 'do not notify me about low-priority email alerts'.",
        "- search_users(query?, phone?, limit?): searches global user_records by semantic similarity and/or exact phone (superuser only).",
        "- directory(query?, username?, email?, phone?, limit?): lookup users you can contact (team-scoped unless superuser).",
        "  Example: directory(username='jane').",
        "  Example: directory(query='Miami Dade on-call').",
        "- resource_health_check(resource_uuid): runs a health check on an accessible resource.",
        "- resource_logs(resource_uuid, limit?, level?, contains?, since_minutes?): queries resource logs with optional recency filter.",
        "  Example: resource_logs(resource_uuid='<uuid>', since_minutes=10, level='error').",
        "- resource_kb(resource_uuid, query?, limit?): searches the resource-specific knowledge base (includes resource wiki + notes).",
        "- resource_ssh_exec(resource_uuid, command, timeout_seconds?, max_output_chars?): executes a remote SSH command on an accessible VM resource.",
        "- sms(message, username?, phone_number?): sends short SMS through Twilio.",
        "  Note: sms is for brief summaries; do not send long logs or multi-section reports via sms.",
        "  Example: sms(username='jane', message='DB alert cleared. Full report sent to your email.').",
        "- set_reminder(title, remind_at, message?, recipients?, action?, channels?, metadata?): schedules a reminder.",
        "  Recipients must be yourself or users in your team-contact scope; defaults to yourself when omitted.",
        "  channels example: {APP:true,SMS:true,EMAIL:false}.",
        "  For richer email reminders with context, include metadata like {resource_uuid:'<uuid>', wiki_query:'deployment runbook'}.",
        "  Example: set_reminder(title='Follow up with Jane', remind_at='2026-03-01T22:00:00Z', recipients=['jane'], channels={APP:true,SMS:true,EMAIL:true}, message='Review the latest prod errors.').",
        "- edit_reminder(reminder_id, ...): updates time, recipients, channels, message, or status.",
        "- delete_reminder(reminder_id, hard_delete?): cancels (default) or deletes a reminder.",
        "- list_reminders(statuses?, limit?): lists your reminders and their dispatch status.",
        "- outlook_mail(action, ...): unified Outlook mail tool with delegated safety.",
        "  Action search: outlook_mail(action='search', query='deploy failure', folder='inbox', refresh=true, limit=8).",
        "  Action read: outlook_mail(action='read', message_id='<id>').",
        "  Action send delegated: outlook_mail(action='send', to='user@example.com', subject='Summary', body='...').",
        "  Action send support inbox: outlook_mail(action='send', send_mode='support_inbox', to='user@example.com', subject='Report', body='...').",
        "  Note: use delegated mode for user-private inbox/calendar context; support_inbox mode is for shared outbound reporting.",
        "  Note: for logs/reports larger than a brief paragraph, prefer outlook_mail over sms.",
        "  Example workflow for 'send logs to <user>': directory(username='<user>') -> resource_logs(resource_uuid='<uuid>', since_minutes=10) -> outlook_mail(action='send', to='<resolved_email>', subject='Last 10 minutes logs', body='<concise report>').",
        "  Note: search combines mailbox refresh + member.db cache + vector-indexed lookup in user knowledge.db collection 'outlook_mail'.",
        "- outlook_calendar(query?, start_date?, end_date?, refresh?, include_completed?, include_unscheduled?, limit?): Outlook calendar context tool.",
        "  Example: outlook_calendar(start_date='2026-02-27', end_date='2026-03-05', refresh=true).",
        "  Example: outlook_calendar(query='incident review', include_completed=false, limit=25).",
        "  Note: use resource_kb for detailed information about a specific resource once identified.",
    ]


def _ask_channel_playbook_lines(
    *,
    channel: str,
    user_email: str,
    user_phone: str,
) -> list[str]:
    resolved_channel = str(channel or "web_chat").strip().lower() or "web_chat"
    has_email = bool(user_email and user_email != "(none)")
    has_phone = bool(user_phone and user_phone != "(none)")

    base_lines = [
        "Cross-channel communication guidance:",
        "- If the user asks you to send a report/summary, use outlook_mail(action='send', ...).",
        "- For requests targeting another person (e.g., '<user>'), resolve recipient details with directory(...) first.",
        "- For scheduled follow-ups or task nudges, use set_reminder(...) and choose channels thoughtfully (SMS short; email for dossiers).",
        "- If the user says 'send it to me', use the user context email/phone defaults before asking follow-ups.",
        "- Prefer delegated send_mode by default; only use send_mode='support_inbox' when explicitly requested or clearly appropriate.",
        "- Prefer email for long content (logs, diagnostics, multi-section reports). Use sms only for short summaries/alerts.",
    ]
    if resolved_channel == "sms":
        base_lines.extend(
            [
                "SMS channel playbook:",
                "- Keep confirmation replies short and explicit.",
                (
                    f"- Default 'email me this' recipient is user context email: {user_email}."
                    if has_email
                    else "- If user asks for email delivery and user context email is missing, ask for the destination email first."
                ),
                "- Example: user says 'email me today's resource report' -> call outlook_mail(action='send', to='<user_email>', subject='Resource report', body='<concise report>').",
                "- Example: user says 'send logs for the past 10 minutes to jane' -> directory(username='jane') -> resource_logs(resource_uuid='<uuid>', since_minutes=10) -> outlook_mail(action='send', to='<jane_email>', subject='Past 10 minutes logs', body='<concise report>').",
                "- Example: user says 'send this from support inbox' -> call outlook_mail(action='send', send_mode='support_inbox', to='<user_email>', subject='Resource report', body='<concise report>').",
                "- Do not send raw/long logs over sms; send a short sms pointer after emailing if needed.",
            ]
        )
        return base_lines

    if resolved_channel == "web_chat":
        base_lines.extend(
            [
                "Web chat playbook:",
                "- Provide the requested details in chat first, then send the same summary by email when asked.",
                (
                    f"- When user says 'email it to me', default to: {user_email}."
                    if has_email
                    else "- If no account email is available, ask which email address to use."
                ),
                "- Example: user asks in chat for diagnostics and then 'email me that' -> call outlook_mail(action='send', to='<user_email>', subject='Diagnostics summary', body='<summary>').",
                "- Example: user asks 'send this to jane by sms' -> directory(username='jane') -> sms(username='jane', message='<short summary>').",
                "- Example: user asks 'send logs to jane' -> directory(username='jane') -> resource_logs(resource_uuid='<uuid>', since_minutes=10) -> outlook_mail(action='send', to='<jane_email>', subject='Last 10 minutes logs', body='<report>').",
            ]
        )
        return base_lines

    if resolved_channel == "webrtc":
        base_lines.extend(
            [
                "WebRTC voice playbook:",
                "- Confirm intent briefly in natural language, then use tools exactly as in text channels.",
                (
                    f"- If user says 'send that to my email', default to: {user_email}."
                    if has_email
                    else "- If user asks for email but no account email exists, ask for the destination email."
                ),
                "- Example: user asks by voice for a summary email -> call outlook_mail(action='send', to='<user_email>', subject='Requested summary', body='<summary>').",
                "- Example: user asks by voice to notify teammate -> directory(username='<name>') first, then sms(...) for short alerts or outlook_mail(...) for detailed reports.",
            ]
        )
        return base_lines

    if resolved_channel == "email":
        base_lines.extend(
            [
                "Email channel playbook:",
                "- Keep responses structured and concise, and include the requested report content directly.",
                (
                    f"- For 'send to me' follow-up emails, default to: {user_email}."
                    if has_email
                    else "- Ask for recipient email when account email is unavailable."
                ),
                "- Example: user asks for a follow-up report email -> call outlook_mail(action='send', to='<recipient>', subject='Follow-up report', body='<summary>').",
                "- For teammate routing requests, use directory(...) to resolve contact details before sending.",
            ]
        )
        return base_lines

    base_lines.extend(
        [
            f"Generic channel playbook ({resolved_channel}):",
            (
                f"- Default recipient for 'send to me' email requests: {user_email}."
                if has_email
                else "- If no account email exists, ask for recipient email before sending."
            ),
            (
                f"- User context phone is available: {user_phone}."
                if has_phone
                else "- User context phone is unavailable; do not assume an SMS destination."
            ),
            "- Example: call outlook_mail(action='send', to='<recipient>', subject='Requested report', body='<summary>').",
        ]
    )
    return base_lines


def _build_ask_system_prompt_for_user(
    *,
    user,
    current_dt_text: str,
    channel: str = "web_chat",
    sms_from: str = "",
    sms_to: str = "",
    extra_context_lines: list[str] | None = None,
    mcp_tool_lines: list[str] | None = None,
) -> str:
    setup = get_setup_state()
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
    email_agent_mailbox = str(getattr(setup, "microsoft_mailbox_email", "") or "").strip() or "(not configured)"

    channel_context_lines = [f"- channel: {str(channel or 'web_chat').strip()}"]
    if sms_from:
        channel_context_lines.append(f"- sms_from: {sms_from}")
    if sms_to:
        channel_context_lines.append(f"- sms_to: {sms_to}")
    resolved_extra_context_lines = [
        str(item or "").strip()
        for item in (extra_context_lines or [])
        if str(item or "").strip()
    ]

    return "\n".join(
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
            f"- email_agent_mailbox: {email_agent_mailbox}",
            f"- is_superuser: {'true' if is_superuser else 'false'}",
            f"- teams: {team_text}",
            *channel_context_lines,
            *resolved_extra_context_lines,
            "",
            "Channel Playbook:",
            *_ask_channel_playbook_lines(
                channel=channel,
                user_email=user_email,
                user_phone=user_phone,
            ),
            "",
            "MCP Tools:",
            *(mcp_tool_lines or _default_ask_mcp_tool_lines()),
        ]
    )


def _extract_realtime_client_secret(session_payload: dict | None) -> str:
    if not isinstance(session_payload, dict):
        return ""
    candidate = session_payload.get("client_secret")
    if isinstance(candidate, dict):
        value = str(candidate.get("value") or candidate.get("secret") or "").strip()
        if value:
            return value
    return str(candidate or session_payload.get("secret") or "").strip()


def _get_request_ip_address(request) -> str:
    forwarded = str(request.META.get("HTTP_X_FORWARDED_FOR") or "").strip()
    if forwarded:
        return str(forwarded.split(",")[0] or "").strip()
    return str(request.META.get("REMOTE_ADDR") or "").strip()


def _user_has_connected_social_account(user, provider: str) -> bool:
    resolved_provider = str(provider or "").strip().lower()
    if user is None or not resolved_provider:
        return False
    try:
        return SocialAccount.objects.filter(user=user, provider=resolved_provider).exists()
    except (OperationalError, ProgrammingError):
        return False
    except Exception:
        return False


def _ask_alshival_generate_reply_for_user(
    *,
    user,
    raw_message: str,
    conversation_id: str = "default",
    channel: str = "web_chat",
    sms_from: str = "",
    sms_to: str = "",
    extra_context_lines: list[str] | None = None,
    history_reader=None,
    chat_writer=None,
    tool_event_writer=None,
) -> tuple[str, dict[str, str]]:
    resolved_message = str(raw_message or "").strip()
    if not resolved_message:
        return "", {"error": "message_required"}
    if len(resolved_message) > 8000:
        resolved_message = resolved_message[:8000]
    resolved_conversation_id = str(conversation_id or "").strip() or "default"

    setup = get_setup_state()
    api_key = str(getattr(setup, "openai_api_key", "") or "").strip()
    if not api_key:
        return "", {"error": "openai_not_configured"}

    model = (
        str(getattr(settings, "ALSHIVAL_OPENAI_CHAT_MODEL", "") or "").strip()
        or str(getattr(setup, "default_model", "") or "").strip()
        or get_alshival_default_model()
    )
    current_dt = datetime.now(timezone.utc).astimezone()
    current_dt_text = current_dt.strftime("%A, %B %d, %Y, %H:%M:%S %Z").strip()

    github_tool_specs: list[dict] = []
    github_tool_name_map: dict[str, str] = {}
    github_user_connected = _user_has_connected_social_account(user, "github")
    github_mcp_enabled = bool(
        setup
        and bool(getattr(setup, "ask_github_mcp_enabled", False))
        and is_github_connector_configured()
        and github_user_connected
    )
    github_mcp_error = ""
    if github_mcp_enabled:
        github_tool_specs, github_tool_name_map, github_mcp_error = _github_mcp_list_tools()

    mcp_tool_lines = _default_ask_mcp_tool_lines()
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

    system_prompt = _build_ask_system_prompt_for_user(
        user=user,
        current_dt_text=current_dt_text,
        channel=channel,
        sms_from=sms_from,
        sms_to=sms_to,
        extra_context_lines=extra_context_lines,
        mcp_tool_lines=mcp_tool_lines,
    )
    if history_reader is None:
        history_items = list_ask_chat_messages(user, conversation_id=resolved_conversation_id, limit=24)
    else:
        history_items = list(history_reader(resolved_conversation_id, 24) or [])
    chat_input: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    for item in history_items:
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            chat_input.append({"role": role, "content": content})
    chat_input.append({"role": "user", "content": resolved_message})
    try:
        if chat_writer is None:
            add_ask_chat_message(user, conversation_id=resolved_conversation_id, role="user", content=resolved_message)
        else:
            chat_writer(resolved_conversation_id, "user", resolved_message)
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
            return "", {"error": "openai_unreachable"}
        if response.status_code >= 400:
            return "", {"error": "openai_error", "status_code": str(response.status_code)}

        data = response.json() if response.content else {}
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            return "", {"error": "openai_empty_response"}
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        if not isinstance(message, dict):
            return "", {"error": "openai_invalid_response"}

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
                    result = _run_ask_tool_for_actor(
                        actor=user,
                        tool_name=tool_name,
                        args=parsed_args,
                        conversation_id=resolved_conversation_id,
                        channel=channel,
                    )
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
                    if tool_event_writer is None:
                        add_ask_chat_tool_event(
                            user,
                            conversation_id=resolved_conversation_id,
                            kind="tool_call",
                            tool_name=tool_name or "unknown",
                            tool_call_id=call_id,
                            tool_args_json=raw_args,
                            content=f"[tool_call] {tool_name or 'unknown'}",
                        )
                        add_ask_chat_tool_event(
                            user,
                            conversation_id=resolved_conversation_id,
                            kind="tool_result",
                            tool_name=tool_name or "unknown",
                            tool_call_id=call_id,
                            tool_result_json=result_json,
                            content=f"[tool_result] {tool_name or 'unknown'}",
                        )
                    else:
                        tool_event_writer(
                            resolved_conversation_id,
                            "tool_call",
                            tool_name or "unknown",
                            call_id,
                            raw_args,
                            "",
                            f"[tool_call] {tool_name or 'unknown'}",
                        )
                        tool_event_writer(
                            resolved_conversation_id,
                            "tool_result",
                            tool_name or "unknown",
                            call_id,
                            "",
                            result_json,
                            f"[tool_result] {tool_name or 'unknown'}",
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
            if chat_writer is None:
                add_ask_chat_message(
                    user,
                    conversation_id=resolved_conversation_id,
                    role="assistant",
                    content=reply,
                )
            else:
                chat_writer(resolved_conversation_id, "assistant", reply)
        except Exception:
            pass
        return reply, {"conversation_id": resolved_conversation_id}

    return "", {"error": "tool_loop_limit_reached", "conversation_id": resolved_conversation_id}


def _twilio_signature(url: str, params: list[tuple[str, str]], auth_token: str) -> str:
    payload = str(url or "")
    for key, value in sorted(params, key=lambda item: (item[0], item[1])):
        payload += f"{key}{value}"
    digest = hmac.new(
        str(auth_token or "").encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def _twilio_request_url_candidates(request) -> list[str]:
    raw = request.build_absolute_uri()
    parsed = urlsplit(raw)
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").strip()
    forwarded_host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").strip()
    candidates = [raw]
    if forwarded_proto or forwarded_host:
        candidates.append(
            urlunsplit(
                (
                    forwarded_proto or parsed.scheme,
                    forwarded_host or parsed.netloc,
                    parsed.path,
                    parsed.query,
                    parsed.fragment,
                )
            )
        )
    candidates.extend(
        [
            urlunsplit((urlsplit(item).scheme, urlsplit(item).netloc, urlsplit(item).path, "", ""))
            for item in list(candidates)
        ]
    )
    seen: set[str] = set()
    deduped: list[str] = []
    for item in candidates:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _twilio_form_params(request) -> list[tuple[str, str]]:
    try:
        body = request.body.decode("utf-8")
    except Exception:
        return []
    if not body:
        return []
    return [(str(k or ""), str(v or "")) for k, v in parse_qsl(body, keep_blank_values=True)]


def _twilio_request_valid(request) -> bool:
    twilio_sig = str(request.headers.get("x-twilio-signature") or "").strip()
    if not twilio_sig:
        return False
    auth_token = get_twilio_auth_token()
    if not auth_token:
        return False
    params = _twilio_form_params(request)
    expected_matches = [
        hmac.compare_digest(_twilio_signature(url, params, auth_token), twilio_sig)
        for url in _twilio_request_url_candidates(request)
    ]
    return any(expected_matches)


def _twiml_sms_response(message: str) -> HttpResponse:
    safe_message = str(message or "").strip()
    if len(safe_message) > 1200:
        safe_message = safe_message[:1200]
    xml_body = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Response>"
        f"<Message>{safe_message.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')}</Message>"
        "</Response>"
    )
    return HttpResponse(xml_body, content_type="application/xml")


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
    conversation_id = str(payload.get("conversation_id") or "").strip() or "default"
    reply, meta = _ask_alshival_generate_reply_for_user(
        user=request.user,
        raw_message=raw_message,
        conversation_id=conversation_id,
        channel="web_chat",
    )
    if reply:
        return JsonResponse({"reply": reply, "conversation_id": str(meta.get("conversation_id") or conversation_id)})

    error = str(meta.get("error") or "openai_error")
    if error == "openai_not_configured":
        return JsonResponse({"error": error}, status=503)
    if error == "message_required":
        return JsonResponse({"error": error}, status=400)
    if error == "tool_loop_limit_reached":
        return JsonResponse({"error": error, "conversation_id": str(meta.get("conversation_id") or conversation_id)}, status=502)
    if error == "openai_error":
        return JsonResponse({"error": error, "status_code": int(str(meta.get("status_code") or "502"))}, status=502)
    return JsonResponse({"error": error, "conversation_id": str(meta.get("conversation_id") or conversation_id)}, status=502)


@login_required
@require_POST
def ask_alshival_voice_token(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    setup = get_setup_state()
    api_key = str(getattr(setup, "openai_api_key", "") or "").strip()
    if not api_key:
        return JsonResponse({"error": "openai_not_configured"}, status=503)

    realtime_model = str(getattr(settings, "ALSHIVAL_OPENAI_REALTIME_MODEL", "") or "").strip() or "gpt-4o-realtime-preview"
    realtime_voice = str(getattr(settings, "ALSHIVAL_OPENAI_REALTIME_VOICE", "") or "").strip() or "alloy"
    page_url = str(payload.get("page_url") or payload.get("pageUrl") or request.META.get("HTTP_REFERER") or "").strip()
    page_text = str(payload.get("page_text") or payload.get("pageText") or "").strip()
    page_text = re.sub(r"\s+", " ", page_text).strip()
    if len(page_text) > 4000:
        page_text = page_text[:4000]
    client_ip = _get_request_ip_address(request)
    current_dt = datetime.now(timezone.utc).astimezone()
    current_dt_text = current_dt.strftime("%A, %B %d, %Y, %H:%M:%S %Z").strip()
    extra_context_lines = []
    if client_ip:
        extra_context_lines.append(f"- client_ip: {client_ip}")
    if page_url:
        extra_context_lines.append(f"- current_page_url: {page_url}")
    if page_text:
        extra_context_lines.append(f"- current_page_text_excerpt: {page_text}")

    instructions = _build_ask_system_prompt_for_user(
        user=request.user,
        current_dt_text=current_dt_text,
        channel="webrtc",
        extra_context_lines=extra_context_lines,
    )

    try:
        response = requests.post(
            "https://api.openai.com/v1/realtime/sessions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "OpenAI-Beta": "realtime=v1",
            },
            json={
                "model": realtime_model,
                "voice": realtime_voice,
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe"},
            },
            timeout=20,
        )
    except requests.RequestException:
        return JsonResponse({"error": "openai_unreachable"}, status=503)

    if response.status_code >= 400:
        return JsonResponse(
            {
                "error": "openai_error",
                "status_code": int(response.status_code),
            },
            status=503,
        )

    data = response.json() if response.content else {}
    client_secret = _extract_realtime_client_secret(data)
    if not client_secret:
        return JsonResponse({"error": "voice_credentials_missing"}, status=503)

    return JsonResponse(
        {
            "client_secret": client_secret,
            "model": realtime_model,
            "voice": realtime_voice,
            "instructions": instructions,
        }
    )


@login_required
@require_POST
def ask_alshival_voice_log(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)
    if not isinstance(payload, dict):
        payload = {}

    role = str(payload.get("role") or "").strip().lower()
    content = str(payload.get("content") or "").strip()
    if role not in {"user", "assistant"} or not content:
        return JsonResponse({"error": "invalid_voice_log_payload"}, status=400)

    conversation_id = str(payload.get("conversation_id") or "").strip() or "default"
    try:
        add_ask_chat_message(
            request.user,
            conversation_id=conversation_id,
            role=role,
            content=content[:8000],
        )
    except Exception:
        return JsonResponse({"error": "voice_log_failed"}, status=502)
    return JsonResponse({"ok": True})


@login_required
@require_POST
def ask_alshival_team_chat(request):
    payload: dict[str, object] = {}
    attachment_upload = request.FILES.get("attachment")
    content_type = str(getattr(request, "content_type", "") or "").strip().lower()
    if content_type.startswith("application/json"):
        try:
            parsed = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return JsonResponse({"error": "invalid_json"}, status=400)
        if isinstance(parsed, dict):
            payload = parsed
    else:
        payload = {
            "message": str(request.POST.get("message") or ""),
            "team_id": str(request.POST.get("team_id") or ""),
            "conversation_id": str(request.POST.get("conversation_id") or ""),
        }

    raw_message = str(payload.get("message") or "").strip()
    if len(raw_message) > 8000:
        raw_message = raw_message[:8000]

    try:
        team_id = int(payload.get("team_id") or 0)
    except Exception:
        team_id = 0
    if team_id <= 0:
        return JsonResponse({"error": "team_required"}, status=400)

    team = Group.objects.filter(id=team_id).first()
    if team is None:
        return JsonResponse({"error": "team_not_found"}, status=404)
    if not request.user.is_superuser and not request.user.groups.filter(id=team_id).exists():
        return JsonResponse({"error": "team_access_denied"}, status=403)

    attachment_name = ""
    attachment_content_type = ""
    attachment_blob: bytes | None = None
    if attachment_upload and int(getattr(attachment_upload, "size", 0) or 0) > 0:
        if int(attachment_upload.size) > _TEAM_CHAT_ATTACHMENT_MAX_BYTES:
            return JsonResponse({"error": "attachment_too_large"}, status=400)
        attachment_name = (Path(str(getattr(attachment_upload, "name", "") or "attachment")).name or "attachment").strip()
        attachment_content_type = str(getattr(attachment_upload, "content_type", "") or "").strip().lower()
        if not _team_chat_attachment_allowed(file_name=attachment_name, content_type=attachment_content_type):
            return JsonResponse({"error": "attachment_type_not_supported"}, status=400)
        attachment_blob = attachment_upload.read()
        if not attachment_blob:
            return JsonResponse({"error": "attachment_empty"}, status=400)
        if not attachment_content_type or attachment_content_type == "application/octet-stream":
            guessed_type, _guessed_encoding = mimetypes.guess_type(attachment_name)
            attachment_content_type = str(guessed_type or "").strip().lower() or "application/octet-stream"

    if not raw_message and not attachment_blob:
        return JsonResponse({"error": "message_required"}, status=400)

    conversation_id = str(payload.get("conversation_id") or "").strip() or f"team-{team_id}"
    add_team_chat_message(
        team,
        actor_user=request.user,
        conversation_id=conversation_id,
        role="user",
        content=raw_message,
        attachment_name=attachment_name,
        attachment_content_type=attachment_content_type,
        attachment_blob=attachment_blob,
    )

    sender_id = int(request.user.id or 0)
    recipient_users = list(team.user_set.filter(is_active=True).exclude(id=sender_id).order_by("id"))
    twilio_sms_available = is_twilio_configured()
    email_notifications_available = is_support_inbox_email_alerts_enabled()
    subject = f"[Alshival Team Chat] {team.name}"
    attachment_line = f"Attachment: {attachment_name}" if attachment_name else ""
    body = "\n".join(
        [
            f"Team: {str(team.name or '').strip()}",
            f"From: {str(request.user.username or '').strip() or 'Team member'}",
            "",
            str(raw_message or "").strip(),
            "",
            attachment_line,
        ]
    ).strip()
    sms_attachment = f" [attachment: {attachment_name}]" if attachment_name else ""
    sms_body = (
        f"{str(team.name or '').strip()}: "
        f"{str(request.user.username or '').strip() or 'Team member'}: "
        f"{str(raw_message or '').strip()}{sms_attachment}"
    )
    if len(sms_body) > 1200:
        sms_body = f"{sms_body[:1197]}..."

    for recipient in recipient_users:
        recipient_id = int(getattr(recipient, "id", 0) or 0)
        if recipient_id <= 0:
            continue
        settings_payload = get_team_chat_notification_settings(team, user_id=recipient_id)
        candidate_channels: list[str] = []
        if bool(settings_payload.get("team_chat_app_enabled", True)):
            candidate_channels.append("app")
        if twilio_sms_available and bool(settings_payload.get("team_chat_sms_enabled", False)):
            candidate_channels.append("sms")
        if email_notifications_available and bool(settings_payload.get("team_chat_email_enabled", False)):
            candidate_channels.append("email")
        allowed_channels = _alert_filter_allowed_channels(
            recipient=recipient,
            alert_kind="team_chat_message",
            candidate_channels=candidate_channels,
            subject=subject,
            body=body,
            context={
                "team_id": int(team.id),
                "team_name": str(team.name or "").strip(),
                "conversation_id": conversation_id,
                "sender_user_id": sender_id,
                "sender_username": str(request.user.username or "").strip(),
            },
        )
        if not allowed_channels:
            continue

        if "app" in allowed_channels:
            add_user_notification(
                recipient,
                kind="team_chat_message",
                title=subject,
                body=body,
                level="info",
                channel="app",
                metadata={
                    "source": "team_chat",
                    "team_id": int(team.id),
                    "team_name": str(team.name or "").strip(),
                    "conversation_id": conversation_id,
                    "sender_user_id": sender_id,
                    "sender_username": str(request.user.username or "").strip(),
                },
            )
        if "sms" in allowed_channels:
            _send_team_chat_sms(recipient=recipient, message=sms_body)
        if "email" in allowed_channels:
            _send_team_chat_email(recipient=recipient, subject=subject, message=body)

    latest_rows = list_team_chat_messages(team, conversation_id=conversation_id, limit=1)
    latest = latest_rows[0] if latest_rows else {
        "role": "user",
        "content": raw_message,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "author_user_id": str(int(request.user.id or 0)),
        "author_username": str(request.user.username or "").strip(),
        "attachment_id": None,
        "attachment_name": attachment_name,
        "attachment_content_type": attachment_content_type,
        "attachment_size": len(attachment_blob) if attachment_blob else 0,
    }
    latest_author_user_id = int(str(latest.get("author_user_id") or "0") or 0)
    latest_attachment_id = int(latest.get("attachment_id") or 0) if latest.get("attachment_id") is not None else 0
    avatar_urls = resolve_user_avatar_urls([latest_author_user_id] if latest_author_user_id > 0 else [])
    return JsonResponse(
        {
            "ok": True,
            "conversation_id": conversation_id,
            "message": {
                "role": str(latest.get("role") or "user"),
                "content": str(latest.get("content") or raw_message),
                "created_at": str(latest.get("created_at") or ""),
                "author_user_id": latest_author_user_id,
                "author_username": str(latest.get("author_username") or "").strip(),
                "author_avatar_url": avatar_urls.get(latest_author_user_id, ""),
                "attachment_id": latest_attachment_id or None,
                "attachment_name": str(latest.get("attachment_name") or "").strip(),
                "attachment_content_type": str(latest.get("attachment_content_type") or "").strip(),
                "attachment_size": int(latest.get("attachment_size") or 0),
                "attachment_is_image": str(latest.get("attachment_content_type") or "").strip().lower().startswith("image/"),
                "attachment_url": (
                    reverse("team_chat_attachment", kwargs={"team_id": int(team.id), "attachment_id": latest_attachment_id})
                    if latest_attachment_id > 0
                    else ""
                ),
            },
        }
    )


@login_required
@require_POST
def update_team_chat_notification_settings(request):
    try:
        team_id = int(request.POST.get("team_id") or 0)
    except Exception:
        team_id = 0
    if team_id <= 0:
        return redirect("team_page")

    team = Group.objects.filter(id=team_id).first()
    if team is None:
        return redirect("team_page")
    if not request.user.is_superuser and not request.user.groups.filter(id=team_id).exists():
        raise PermissionDenied("You do not have access to this team.")

    twilio_sms_available = is_twilio_configured()
    email_notifications_available = is_support_inbox_email_alerts_enabled()
    payload = {
        "team_chat_app_enabled": _post_flag(request.POST, "team_chat_app_enabled"),
        "team_chat_sms_enabled": _post_flag(request.POST, "team_chat_sms_enabled") if twilio_sms_available else False,
        "team_chat_email_enabled": _post_flag(request.POST, "team_chat_email_enabled") if email_notifications_available else False,
    }
    upsert_team_chat_notification_settings(
        team,
        user_id=int(request.user.id or 0),
        payload=payload,
    )
    messages.success(request, "Team chat notification settings updated.")
    return redirect(f"{reverse('team_page')}#team-chat-alerts")


@login_required
def team_chat_messages(request):
    try:
        team_id = int(request.GET.get("team_id") or 0)
    except Exception:
        team_id = 0
    if team_id <= 0:
        return JsonResponse({"error": "team_required"}, status=400)

    team = Group.objects.filter(id=team_id).first()
    if team is None:
        return JsonResponse({"error": "team_not_found"}, status=404)
    if not request.user.is_superuser and not request.user.groups.filter(id=team_id).exists():
        return JsonResponse({"error": "team_access_denied"}, status=403)

    conversation_id = str(request.GET.get("conversation_id") or "").strip() or f"team-{team_id}"
    try:
        limit = int(request.GET.get("limit") or 180)
    except Exception:
        limit = 180
    resolved_limit = max(1, min(limit, 500))

    rows = list_team_chat_messages(team, conversation_id=conversation_id, limit=resolved_limit)
    author_ids = sorted(
        {
            int(str(row.get("author_user_id") or "0") or 0)
            for row in rows
            if int(str(row.get("author_user_id") or "0") or 0) > 0
        }
    )
    author_avatar_urls = resolve_user_avatar_urls(author_ids)
    messages_payload = []
    for row in rows:
        role = str(row.get("role") or "").strip().lower()
        if role != "user":
            continue
        content = str(row.get("content") or "").strip()
        attachment_id = int(row.get("attachment_id") or 0) if row.get("attachment_id") is not None else 0
        if not content and attachment_id <= 0:
            continue
        author_user_id = int(str(row.get("author_user_id") or "0") or 0)
        author_username = str(row.get("author_username") or "").strip()
        attachment_content_type = str(row.get("attachment_content_type") or "").strip()
        messages_payload.append(
            {
                "role": "user",
                "content": content,
                "created_at": str(row.get("created_at") or ""),
                "author_user_id": author_user_id,
                "author_username": author_username or "Team member",
                "author_avatar_url": author_avatar_urls.get(author_user_id, ""),
                "is_mine": int(request.user.id or 0) == author_user_id and author_user_id > 0,
                "attachment_id": attachment_id or None,
                "attachment_name": str(row.get("attachment_name") or "").strip(),
                "attachment_content_type": attachment_content_type,
                "attachment_size": int(row.get("attachment_size") or 0),
                "attachment_is_image": attachment_content_type.lower().startswith("image/"),
                "attachment_url": (
                    reverse("team_chat_attachment", kwargs={"team_id": int(team.id), "attachment_id": attachment_id})
                    if attachment_id > 0
                    else ""
                ),
            }
        )

    return JsonResponse(
        {
            "ok": True,
            "conversation_id": conversation_id,
            "team_id": team_id,
            "messages": messages_payload,
        }
    )


@login_required
@require_GET
def team_chat_attachment(request, team_id: int, attachment_id: int):
    resolved_team_id = int(team_id or 0)
    resolved_attachment_id = int(attachment_id or 0)
    if resolved_team_id <= 0 or resolved_attachment_id <= 0:
        return HttpResponse(status=404)

    team = Group.objects.filter(id=resolved_team_id).first()
    if team is None:
        return HttpResponse(status=404)
    if not request.user.is_superuser and not request.user.groups.filter(id=resolved_team_id).exists():
        raise PermissionDenied("You do not have access to this team.")

    attachment = get_team_chat_attachment(team, attachment_id=resolved_attachment_id)
    if not attachment:
        return HttpResponse(status=404)

    content_type = str(attachment.get("content_type") or "application/octet-stream").strip() or "application/octet-stream"
    response = HttpResponse(attachment.get("file_blob") or b"", content_type=content_type)
    response["Content-Length"] = str(int(attachment.get("file_size") or 0))
    file_name = str(attachment.get("file_name") or "attachment").replace('"', "").strip() or "attachment"
    disposition = "inline" if _team_chat_attachment_is_inline(content_type=content_type) else "attachment"
    response["Content-Disposition"] = f'{disposition}; filename="{file_name}"'
    return response


@csrf_exempt
@require_POST
def twilio_sms_webhook(request):
    if not _twilio_request_valid(request):
        return HttpResponse("invalid twilio signature", status=403)

    from_number = str(request.POST.get("From") or request.POST.get("from") or "").strip()
    to_number = str(request.POST.get("To") or request.POST.get("to") or "").strip()
    body = str(request.POST.get("Body") or request.POST.get("body") or "").strip()
    if not body:
        return _twiml_sms_response("I did not receive a message body. Please try again.")

    user = resolve_user_by_phone(from_number)
    if user is None:
        return _twiml_sms_response("Your number is not linked to an Alshival account yet.")

    # Shared conversation id to maximize continuity between web and SMS channels.
    conversation_id = "default"
    reply, meta = _ask_alshival_generate_reply_for_user(
        user=user,
        raw_message=body,
        conversation_id=conversation_id,
        channel="sms",
        sms_from=from_number,
        sms_to=to_number,
    )
    if reply:
        return _twiml_sms_response(reply)

    error = str(meta.get("error") or "openai_error")
    if error == "openai_not_configured":
        return _twiml_sms_response("Alshival is not configured yet. Please contact your administrator.")
    return _twiml_sms_response("I couldn't process that right now. Please try again shortly.")


@csrf_exempt
@require_POST
def twilio_sms_group_webhook(request):
    return twilio_sms_webhook(request)


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

    next_url = str(request.POST.get("next") or "").strip()
    if next_url.startswith("/") and not next_url.startswith("//"):
        return redirect(next_url)
    return redirect("resources")


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
