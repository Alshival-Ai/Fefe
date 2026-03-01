from __future__ import annotations

import base64
from contextvars import ContextVar
import functools
import html
import hashlib
import hmac
import inspect
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urljoin, urlsplit, urlunsplit

from asgiref.sync import sync_to_async
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
import requests
try:
    from mcp.server.fastmcp import FastMCP
except ModuleNotFoundError:  # pragma: no cover - compatibility fallback
    from fastmcp import FastMCP

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "alshival.settings")

import django  # noqa: E402

django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from django.db.models import Q  # noqa: E402
from django.db.utils import OperationalError, ProgrammingError  # noqa: E402
from allauth.socialaccount.models import SocialAccount, SocialToken  # noqa: E402
from dashboard.health import check_health  # noqa: E402
from dashboard.models import (
    ResourcePackageOwner,
    ResourceRouteAlias,
    SystemSetup,
    UserNotificationSettings,
    WikiPage,
)  # noqa: E402
from dashboard.request_auth import authenticate_api_key, get_twilio_auth_token, resolve_user_by_phone, user_can_access_resource  # noqa: E402
from dashboard.resource_ssh_exec import execute_resource_ssh_command  # noqa: E402
from dashboard.resources_store import (
    _global_owner_dir,
    _user_db_path,
    _user_knowledge_db_path,
    get_user_outlook_mail_cache_message,
    get_user_alert_filter_prompt,
    get_resource_by_uuid,
    get_resource_owner_context,
    list_user_calendar_event_cache,
    list_user_outlook_mail_cache,
    list_resource_logs,
    upsert_user_outlook_mail_cache,
    update_user_alert_filter_prompt,
)  # noqa: E402
from dashboard.user_knowledge_store import query_user_records  # noqa: E402
from dashboard.calendar_sync_service import (
    DEFAULT_CALENDAR_REFRESH_MIN_INTERVAL_SECONDS,
    refresh_calendar_cache_for_user,
)  # noqa: E402
from dashboard.views import (  # noqa: E402
    _asana_access_token_for_user,
    _asana_api_request_json,
    _asana_api_list,
    _asana_error_requires_refresh,
)

API_KEY_HEADER = (os.getenv("MCP_API_KEY_HEADER") or "x-api-key").strip() or "x-api-key"
USERNAME_HEADER = (os.getenv("MCP_USERNAME_HEADER") or "x-user-username").strip() or "x-user-username"
EMAIL_HEADER = (os.getenv("MCP_EMAIL_HEADER") or "x-user-email").strip() or "x-user-email"
PHONE_HEADER = (os.getenv("MCP_PHONE_HEADER") or "x-user-phone").strip() or "x-user-phone"
RESOURCE_UUID_HEADER = (os.getenv("MCP_RESOURCE_HEADER") or "x-resource-uuid").strip() or "x-resource-uuid"
GITHUB_MCP_UPSTREAM_URL = (os.getenv("MCP_GITHUB_UPSTREAM_URL") or "").strip()
ASANA_MCP_UPSTREAM_URL = (os.getenv("MCP_ASANA_UPSTREAM_URL") or "").strip()
TWILIO_SIGNATURE_HEADER = (os.getenv("MCP_TWILIO_SIGNATURE_HEADER") or "x-twilio-signature").strip() or "x-twilio-signature"
_REQUEST_AUTH = ContextVar("mcp_request_auth", default=None)


def _ensure_runtime_cache_dirs() -> None:
    candidates = []
    current = str(os.getenv("XDG_CACHE_HOME") or "").strip()
    if current:
        candidates.append(Path(current))
    candidates.append(BASE_DIR / "var" / "cache")
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


def _extract_api_key(request: Request) -> str:
    explicit = (request.headers.get(API_KEY_HEADER) or "").strip()
    if explicit:
        return explicit
    auth_header = (request.headers.get("authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return ""


def _set_request_auth_context(
    *,
    auth_scope: str,
    user_id: int = 0,
    username: str = "",
    email: str = "",
    phone: str = "",
) -> None:
    _REQUEST_AUTH.set(
        {
            "auth_scope": str(auth_scope or "").strip(),
            "user_id": int(user_id or 0),
            "username": str(username or "").strip(),
            "email": str(email or "").strip(),
            "phone": str(phone or "").strip(),
        }
    )


def _request_auth_payload() -> dict[str, Any]:
    payload = _REQUEST_AUTH.get()
    if isinstance(payload, dict):
        return payload
    return {}


def _request_actor():
    payload = _request_auth_payload()
    user_id = int(payload.get("user_id", 0) or 0)
    if user_id <= 0:
        return None
    User = get_user_model()
    return User.objects.filter(id=user_id, is_active=True).first()


def _resolve_resource_for_health_check(resource_uuid: str):
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

    actor = _request_actor()
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
        try:
            resource = get_resource_by_uuid(owner_user, resolved_uuid)
        except Exception:
            continue
        if resource is not None:
            return owner_user, resource
    return None, None


def _actor_can_check_resource(*, actor, owner_user, resource_uuid: str) -> bool:
    if actor is None:
        return False
    if bool(getattr(actor, "is_superuser", False)):
        return True

    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return False

    owner_row = (
        ResourcePackageOwner.objects.select_related("owner_team")
        .filter(resource_uuid=resolved_uuid)
        .first()
    )
    if owner_row and str(getattr(owner_row, "owner_scope", "")).strip().lower() == ResourcePackageOwner.OWNER_SCOPE_GLOBAL:
        return True

    # Keep non-global access policy aligned with dashboard auth checks.
    return user_can_access_resource(user=actor, resource_uuid=resolved_uuid)


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


def _request_url_candidates(request: Request) -> list[str]:
    raw = str(request.url)
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
    # Twilio signature behavior can vary across proxies; also try versions without query string.
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


async def _twilio_form_params(request: Request) -> list[tuple[str, str]]:
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/x-www-form-urlencoded" not in content_type:
        return []
    body = await request.body()
    if not body:
        return []
    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError:
        return []
    return [(str(key or ""), str(value or "")) for key, value in parse_qsl(decoded, keep_blank_values=True)]


async def _authenticate_twilio_phone_request(request: Request) -> tuple[bool, str]:
    twilio_sig = (request.headers.get(TWILIO_SIGNATURE_HEADER) or "").strip()
    if not twilio_sig:
        return False, "missing_twilio_signature"

    auth_token = await sync_to_async(get_twilio_auth_token, thread_sensitive=True)()
    if not auth_token:
        return False, "twilio_not_configured"

    params = await _twilio_form_params(request)
    expected_matches = [
        hmac.compare_digest(_twilio_signature(url, params, auth_token), twilio_sig)
        for url in _request_url_candidates(request)
    ]
    if not any(expected_matches):
        return False, "invalid_twilio_signature"

    param_lookup = {str(key).lower(): str(value or "") for key, value in params}
    phone = (
        (request.headers.get(PHONE_HEADER) or "").strip()
        or str(param_lookup.get("from") or "").strip()
    )
    if not phone:
        return False, "missing_phone_identity"

    user = await sync_to_async(resolve_user_by_phone, thread_sensitive=True)(phone)
    if user is None:
        return False, "unknown_phone_identity"

    resource_uuid = (request.headers.get(RESOURCE_UUID_HEADER) or "").strip()
    if resource_uuid:
        allowed = await sync_to_async(user_can_access_resource, thread_sensitive=True)(
            user=user,
            resource_uuid=resource_uuid,
        )
        if not allowed:
            return False, "resource_access_denied"

    request.state.auth_user_id = int(getattr(user, "id", 0) or 0)
    request.state.auth_username = str(getattr(user, "username", "") or "")
    request.state.auth_email = str(getattr(user, "email", "") or "")
    request.state.auth_phone = phone
    request.state.auth_scope = "twilio_phone"
    return True, ""


def _clean(value: str | None) -> str:
    return str(value or "").strip()


def _normalize_phone(value: str | None) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    keep_plus = raw.startswith("+")
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return ""
    return f"+{digits}" if keep_plus else digits


def _twilio_sms_credentials() -> tuple[str, str, str]:
    try:
        setup = SystemSetup.objects.order_by("-updated_at", "-created_at").first()
    except Exception:
        setup = None
    account_sid = _clean(os.getenv("TWILIO_ACCOUNT_SID"))
    from_number = _clean(os.getenv("TWILIO_FROM_NUMBER"))
    auth_token = _clean(get_twilio_auth_token())
    if setup is not None:
        if not account_sid:
            account_sid = _clean(getattr(setup, "twilio_account_sid", ""))
        if not from_number:
            from_number = _clean(getattr(setup, "twilio_from_number", ""))
    return account_sid, auth_token, from_number


def _target_phone_from_username(username: str):
    resolved_username = _clean(username)
    if not resolved_username:
        return "", None
    User = get_user_model()
    user = User.objects.filter(username__iexact=resolved_username, is_active=True).first()
    if user is None:
        return "", None
    phone_raw = (
        UserNotificationSettings.objects.filter(user_id=int(getattr(user, "id", 0) or 0))
        .values_list("phone_number", flat=True)
        .first()
        or ""
    )
    return _normalize_phone(phone_raw), user


mcp = FastMCP("alshival-mcp", stateless_http=True)


def mcp_threaded_tool(*tool_args, **tool_kwargs):
    """
    Register a sync tool in FastMCP by executing it in a sync thread.
    This avoids Django's SynchronousOnlyOperation inside async request handling.
    """

    def _decorator(fn):
        @functools.wraps(fn)
        async def _wrapped(*args, **kwargs):
            return await sync_to_async(fn, thread_sensitive=True)(*args, **kwargs)

        _wrapped.__signature__ = inspect.signature(fn)
        return mcp.tool(*tool_args, **tool_kwargs)(_wrapped)

    return _decorator


@mcp_threaded_tool()
def ping() -> dict[str, str]:
    """Dummy MCP tool used to validate MCP auth wiring."""
    return {
        "ok": "true",
        "message": "pong",
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def _query_chroma_resources(
    *,
    knowledge_path: Path,
    query: str,
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    _ensure_runtime_cache_dirs()
    try:
        import chromadb
    except Exception:
        return [], "chromadb package is not installed"

    resolved_path = Path(knowledge_path)
    if not resolved_path.exists():
        return [], ""

    client = chromadb.PersistentClient(path=str(resolved_path))
    try:
        collection = client.get_collection(name="resources")
    except Exception:
        return [], ""

    n_results = max(1, min(int(limit or 5), 50))
    where_filter: dict[str, Any] | None = None

    resolved_query = str(query or "").strip()
    rows: list[dict[str, Any]] = []
    if resolved_query:
        try:
            payload = collection.query(
                query_texts=[resolved_query],
                n_results=n_results,
                where=where_filter,
            )
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


def _workspace_wiki_results_for_actor(*, actor, query: str, limit: int) -> list[dict[str, Any]]:
    if actor is None:
        return []
    qs = WikiPage.objects.filter(
        scope=WikiPage.SCOPE_WORKSPACE,
        resource_uuid="",
    ).prefetch_related("team_access")
    if not bool(getattr(actor, "is_superuser", False)):
        team_ids = list(actor.groups.values_list("id", flat=True))
        draft_filter = Q(is_draft=True, created_by_id=int(getattr(actor, "id", 0) or 0))
        published_filter = Q(is_draft=False)
        if not team_ids:
            qs = qs.filter(draft_filter | (published_filter & Q(team_access__isnull=True))).distinct()
        else:
            qs = qs.filter(
                draft_filter
                | (published_filter & (Q(team_access__isnull=True) | Q(team_access__id__in=team_ids)))
            ).distinct()
    # Public published workspace pages are indexed into global Chroma and returned via global KB search.
    # Keep this DB fallback focused on non-global pages to avoid duplicate results.
    qs = qs.exclude(is_draft=False, team_access__isnull=True).distinct()
    if query:
        qs = qs.filter(
            Q(title__icontains=query)
            | Q(path__icontains=query)
            | Q(body_markdown__icontains=query)
        )
    rows = list(qs.order_by("-updated_at")[: max(1, min(int(limit or 4), 20))])
    results: list[dict[str, Any]] = []
    lowered_query = str(query or "").strip().lower()
    for page in rows:
        markdown = str(getattr(page, "body_markdown", "") or "")
        if lowered_query:
            lowered = markdown.lower()
            idx = lowered.find(lowered_query)
            if idx >= 0:
                start = max(0, idx - 120)
                end = min(len(markdown), idx + 240)
                snippet = markdown[start:end].strip()
            else:
                snippet = markdown[:240].strip()
        else:
            snippet = markdown[:240].strip()
        results.append(
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
    return results


def _parse_ymd(value: str) -> datetime | None:
    resolved = str(value or "").strip()
    if not resolved:
        return None
    try:
        return datetime.strptime(resolved, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _calendar_row_sort_key(row: dict[str, Any]) -> tuple[str, str, str]:
    due_date = str(row.get("due_date") or "").strip()
    due_time = str(row.get("due_time") or "").strip()
    title = str(row.get("title") or "").strip().lower()
    if not due_date:
        return ("9999-12-31", "99:99", title)
    return (due_date, due_time or "99:99", title)


def _calendar_context_line(row: dict[str, Any]) -> str:
    provider = str(row.get("provider") or "").strip() or "calendar"
    title = str(row.get("title") or "").strip() or "Untitled event"
    due_date = str(row.get("due_date") or "").strip()
    due_time = str(row.get("due_time") or "").strip()
    status = str(row.get("status") or "").strip().lower() or ("completed" if bool(row.get("is_completed")) else "open")
    when = due_date if due_date else "unscheduled"
    if due_time:
        when = f"{when} {due_time}"
    return f"[{provider}] {title} | {when} | {status}"


_OUTLOOK_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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


def _outlook_parse_addresses(raw_value: object) -> list[str]:
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


def _outlook_recipients(payload: object) -> list[str]:
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


def _outlook_strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _stable_json_hash(value: object) -> str:
    try:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        payload = str(value or "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
    result: dict[str, str] = {}
    for idx, item_id in enumerate(payload_ids):
        resolved_id = str(item_id or "").strip()
        if not resolved_id:
            continue
        metadata = payload_metas[idx] if idx < len(payload_metas) and isinstance(payload_metas[idx], dict) else {}
        result[resolved_id] = str(metadata.get(key) or "").strip()
    return result


def _outlook_normalize_graph_message(item: dict[str, Any], *, default_folder: str = "inbox") -> dict[str, Any]:
    body_obj = item.get("body") if isinstance(item.get("body"), dict) else {}
    body_type = str(body_obj.get("contentType") or "").strip().lower()
    body_content = str(body_obj.get("content") or "").strip()
    body_text = _outlook_strip_html(body_content) if body_type == "html" else re.sub(r"\s+", " ", body_content).strip()
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
        "to_recipients": _outlook_recipients(item.get("toRecipients")),
        "cc_recipients": _outlook_recipients(item.get("ccRecipients")),
        "received_at": str(item.get("receivedDateTime") or "").strip(),
        "sent_at": str(item.get("sentDateTime") or "").strip(),
        "body_preview": body_preview,
        "body_text": body_text,
        "web_link": str(item.get("webLink") or "").strip(),
        "is_read": bool(item.get("isRead")),
        "has_attachments": bool(item.get("hasAttachments")),
        "raw_payload": item,
    }


def _outlook_document(row: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"From: {str(row.get('sender_email') or '').strip() or 'unknown'}",
            f"Subject: {str(row.get('subject') or '').strip() or '(no subject)'}",
            f"Received: {str(row.get('received_at') or '').strip() or '(unknown)'}",
            "",
            str(row.get("body_text") or row.get("body_preview") or "").strip(),
        ]
    ).strip()


def _outlook_context_hash(row: dict[str, Any]) -> str:
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


def _outlook_index_for_actor(actor, rows: list[dict[str, Any]]) -> tuple[int, str]:
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
    metas: list[dict[str, str | int | float | bool]] = []
    row_hashes: list[str] = []
    for row in rows:
        message_id = str(row.get("message_id") or "").strip()
        if not message_id:
            continue
        context_hash = _outlook_context_hash(row)
        ids.append(message_id)
        docs.append(_outlook_document(row))
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
    filtered_metas: list[dict[str, str | int | float | bool]] = []
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


def _outlook_vector_search_for_actor(actor, *, query: str, limit: int) -> tuple[list[dict[str, Any]], str]:
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
        payload = collection.query(query_texts=[resolved_query], n_results=resolved_limit)
    except Exception as exc:
        return [], f"chroma query failed: {exc}"

    ids = (payload.get("ids") or [[]])[0]
    docs = (payload.get("documents") or [[]])[0]
    metas = (payload.get("metadatas") or [[]])[0]
    dists = (payload.get("distances") or [[]])[0]
    rows: list[dict[str, Any]] = []
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


def _microsoft_graph_send_mail(
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
    payload: dict[str, Any] = {
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


def _microsoft_graph_list_messages(
    *,
    access_token: str,
    folder: str = "inbox",
    limit: int = 80,
    include_body: bool = False,
) -> tuple[list[dict[str, Any]], str]:
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
    rows: list[dict[str, Any]] = []
    for item in (payload.get("value") or []):
        if not isinstance(item, dict):
            continue
        normalized = _outlook_normalize_graph_message(item, default_folder=default_folder)
        if str(normalized.get("message_id") or "").strip():
            rows.append(normalized)
    return rows, ""


def _microsoft_graph_read_message(
    *,
    access_token: str,
    message_id: str,
) -> tuple[dict[str, Any] | None, str]:
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
    normalized = _outlook_normalize_graph_message(payload, default_folder="inbox")
    if not str(normalized.get("message_id") or "").strip():
        return None, "message not found"
    return normalized, ""


def _search_kb_sync(query: str = "") -> dict[str, Any]:
    """
    Search both personal and global knowledge bases for the authenticated user.
    Returns up to 4 personal matches and 3 global matches.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}

    personal_path = _user_knowledge_db_path(actor)
    global_path = _global_owner_dir() / "knowledge.db"

    personal_results, personal_error = _query_chroma_resources(
        knowledge_path=personal_path,
        query=query,
        limit=4,
    )
    if personal_error:
        return {"ok": False, "error": personal_error, "results": []}

    global_results, global_error = _query_chroma_resources(
        knowledge_path=global_path,
        query=query,
        limit=3,
    )
    if global_error:
        return {"ok": False, "error": global_error, "results": []}

    wiki_limit = 4
    wiki_results = _workspace_wiki_results_for_actor(
        actor=actor,
        query=query,
        limit=wiki_limit,
    )
    merged = list(personal_results) + list(global_results) + list(wiki_results)
    return {
        "ok": True,
        "collection": "resources",
        "knowledge_paths": {
            "user": str(personal_path),
            "global": str(global_path),
        },
        "query": str(query or ""),
        "user_limit": 4,
        "global_limit": 3,
        "wiki_limit": wiki_limit,
        "user_result_count": len(personal_results),
        "global_result_count": len(global_results),
        "wiki_result_count": len(wiki_results),
        "result_count": len(merged),
        "user_results": personal_results,
        "global_results": global_results,
        "wiki_results": wiki_results,
        "results": merged,
    }


@mcp.tool()
async def search_kb(
    query: str = "",
) -> dict[str, Any]:
    # FastMCP calls tools from an async request lifecycle; run Django/SQLite work in a sync thread.
    return await sync_to_async(_search_kb_sync, thread_sensitive=True)(query)


@mcp_threaded_tool()
def outlook_mail(
    action: str = "search",
    query: str = "",
    folder: str = "inbox",
    message_id: str = "",
    to: str = "",
    cc: str = "",
    subject: str = "",
    body: str = "",
    send_mode: str = "delegated",
    refresh: bool = True,
    include_body: bool = False,
    limit: int = 12,
) -> dict[str, Any]:
    """
    Unified Outlook mailbox operations for the authenticated user.

    Examples:
    - Search inbox semantically and by cache:
      `outlook_mail(action="search", query="incident summary", folder="inbox", refresh=True, limit=8)`
    - Read a message by id:
      `outlook_mail(action="read", message_id="<message-id>")`
    - Send from delegated mailbox:
      `outlook_mail(action="send", to="user@example.com", subject="Report", body="...")`
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    resolved_action = str(action or "search").strip().lower() or "search"
    if resolved_action in {"list", "inbox", "query"}:
        resolved_action = "search"
    if resolved_action in {"get"}:
        resolved_action = "read"
    if resolved_action in {"compose"}:
        resolved_action = "send"
    if resolved_action not in {"search", "read", "send"}:
        return {"ok": False, "error": "action must be one of: search, read, send"}

    access_token, token_error = _microsoft_delegated_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": token_error or "Microsoft is not connected for this user."}

    if resolved_action == "send":
        resolved_send_mode = str(send_mode or "delegated").strip().lower() or "delegated"
        if resolved_send_mode != "delegated":
            return {
                "ok": False,
                "error": "MCP outlook_mail supports delegated send only (support_inbox send is chat-agent scoped).",
            }
        to_addresses = _outlook_parse_addresses(to)
        cc_addresses = _outlook_parse_addresses(cc)
        if not to_addresses:
            return {"ok": False, "error": "to is required"}
        all_addresses = list(dict.fromkeys(to_addresses + cc_addresses))
        invalid_addresses = [address for address in all_addresses if not _OUTLOOK_EMAIL_RE.match(address)]
        if invalid_addresses:
            return {"ok": False, "error": f"invalid recipient email(s): {', '.join(invalid_addresses[:5])}"}
        if len(all_addresses) > 25:
            return {"ok": False, "error": "too many recipients (max 25)"}
        resolved_body = str(body or "").strip()
        if not resolved_body:
            return {"ok": False, "error": "body is required"}
        if len(resolved_body) > 10000:
            resolved_body = resolved_body[:10000]
        sent, send_error = _microsoft_graph_send_mail(
            access_token=access_token,
            subject=str(subject or "").strip(),
            body_text=resolved_body,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
        )
        if not sent:
            return {"ok": False, "error": send_error or "Unable to send email right now."}
        return {
            "ok": True,
            "tool": "outlook_mail",
            "action": "send",
            "auth_mode": "delegated",
            "recipient_count": len(all_addresses),
            "to_recipients": to_addresses,
            "cc_recipients": cc_addresses,
            "subject": str(subject or "").strip()[:255],
            "sent": True,
        }

    if resolved_action == "read":
        resolved_message_id = str(message_id or "").strip()
        if not resolved_message_id:
            return {"ok": False, "error": "message_id is required for action=read"}
        message_row, read_error = _microsoft_graph_read_message(
            access_token=access_token,
            message_id=resolved_message_id,
        )
        from_cache = False
        if message_row is None:
            cached = get_user_outlook_mail_cache_message(actor, message_id=resolved_message_id)
            if cached is None:
                return {"ok": False, "error": read_error or "message not found"}
            message_row = cached
            from_cache = True
        else:
            upsert_user_outlook_mail_cache(actor, messages=[message_row])
            _outlook_index_for_actor(actor, [message_row])
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

    resolved_query = str(query or "").strip()
    resolved_folder = str(folder or "inbox").strip().lower() or "inbox"
    resolved_refresh = bool(refresh)
    resolved_include_body = bool(include_body)
    resolved_limit = max(1, min(int(limit or 12), 50))

    refresh_error = ""
    fetched_rows: list[dict[str, Any]] = []
    indexed_count = 0
    if resolved_refresh:
        fetch_limit = max(resolved_limit * 4, 60)
        fetched_rows, refresh_error = _microsoft_graph_list_messages(
            access_token=access_token,
            folder=resolved_folder,
            limit=fetch_limit,
            include_body=bool(resolved_include_body or resolved_query),
        )
        if fetched_rows:
            upsert_user_outlook_mail_cache(actor, messages=fetched_rows)
            indexed_count, _ = _outlook_index_for_actor(actor, fetched_rows)

    cache_scan_limit = max(resolved_limit * 4, 80)
    cached_rows = list_user_outlook_mail_cache(
        actor,
        query=resolved_query,
        limit=cache_scan_limit,
        folder=resolved_folder,
        include_body=bool(resolved_include_body or resolved_query),
    )
    cached_by_id = {
        str(row.get("message_id") or "").strip(): row
        for row in cached_rows
        if str(row.get("message_id") or "").strip()
    }
    vector_rows: list[dict[str, Any]] = []
    vector_error = ""
    if resolved_query:
        vector_rows, vector_error = _outlook_vector_search_for_actor(
            actor,
            query=resolved_query,
            limit=cache_scan_limit,
        )

    merged: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in vector_rows:
        mid = str(row.get("message_id") or "").strip()
        if not mid or mid in seen_ids:
            continue
        seen_ids.add(mid)
        if mid in cached_by_id:
            merged_row = dict(cached_by_id[mid])
            merged_row["match_source"] = "vector"
            merged_row["distance"] = row.get("distance")
            merged.append(merged_row)
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        merged.append(
            {
                "message_id": mid,
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
        mid = str(row.get("message_id") or "").strip()
        if not mid or mid in seen_ids:
            continue
        seen_ids.add(mid)
        merged_row = dict(row)
        merged_row["match_source"] = "cache"
        merged_row["distance"] = None
        merged.append(merged_row)

    return {
        "ok": True,
        "tool": "outlook_mail",
        "action": "search",
        "auth_mode": "delegated",
        "query": resolved_query,
        "folder": resolved_folder,
        "refresh": resolved_refresh,
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


@mcp_threaded_tool()
def outlook_calendar(
    query: str = "",
    start_date: str = "",
    end_date: str = "",
    refresh: bool = True,
    include_completed: bool = False,
    include_unscheduled: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    """
    Query Outlook calendar context from per-user cache in member.db.

    Examples:
    - `outlook_calendar(start_date="2026-02-27", end_date="2026-03-05", refresh=True)`
    - `outlook_calendar(query="incident review", include_completed=False, limit=25)`
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}

    resolved_query = str(query or "").strip().lower()
    resolved_limit = max(1, min(int(limit or 200), 1000))
    start_dt = _parse_ymd(start_date)
    end_dt = _parse_ymd(end_date)
    if start_date and start_dt is None:
        return {"ok": False, "error": "start_date must be YYYY-MM-DD", "results": []}
    if end_date and end_dt is None:
        return {"ok": False, "error": "end_date must be YYYY-MM-DD", "results": []}
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        return {"ok": False, "error": "end_date must be on/after start_date", "results": []}

    refresh_requested = bool(refresh)
    refresh_applied = False
    refresh_result: dict[str, Any] = {}
    refresh_error = ""
    refresh_min_interval_seconds = int(DEFAULT_CALENDAR_REFRESH_MIN_INTERVAL_SECONDS)
    if refresh_requested:
        try:
            refresh_result = refresh_calendar_cache_for_user(
                actor,
                provider="outlook",
                force=False,
                min_interval_seconds=refresh_min_interval_seconds,
            )
            provider_result = refresh_result.get("outlook") if isinstance(refresh_result, dict) else {}
            refresh_applied = bool(provider_result.get("refresh_attempted")) if isinstance(provider_result, dict) else False
        except Exception as exc:
            refresh_error = str(exc)

    rows = list_user_calendar_event_cache(
        actor,
        provider="outlook",
        limit=5000,
        include_completed=bool(include_completed),
    )
    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_title = str(row.get("title") or "").strip()
        row_due_date = str(row.get("due_date") or "").strip()
        row_due_time = str(row.get("due_time") or "").strip()
        row_status = str(row.get("status") or "").strip().lower()
        row_source_url = str(row.get("source_url") or "").strip()
        row_payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        row_completed = bool(row.get("is_completed"))

        if not include_unscheduled and not row_due_date:
            continue

        row_due_dt = _parse_ymd(row_due_date) if row_due_date else None
        if start_dt is not None and (row_due_dt is None or row_due_dt < start_dt):
            continue
        if end_dt is not None and (row_due_dt is None or row_due_dt > end_dt):
            continue

        if resolved_query:
            haystack = " ".join(
                [
                    row_title,
                    row_status,
                    row_source_url,
                    row_due_date,
                    row_due_time,
                    str(row_payload),
                ]
            ).lower()
            if resolved_query not in haystack:
                continue

        filtered.append(
            {
                "provider": "outlook",
                "event_id": str(row.get("event_id") or "").strip(),
                "title": row_title,
                "due_date": row_due_date,
                "due_time": row_due_time,
                "is_completed": row_completed,
                "status": row_status or ("completed" if row_completed else "open"),
                "source_url": row_source_url,
                "payload": row_payload,
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
        )

    filtered.sort(key=_calendar_row_sort_key)
    limited = filtered[:resolved_limit]

    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    seven_day_key = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")
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
            if today_key <= due_date <= seven_day_key:
                next_7d_open_count += 1

    context_lines = [_calendar_context_line(row) for row in limited[:50]]
    return {
        "ok": True,
        "tool": "outlook_calendar",
        "query": str(query or ""),
        "provider": "outlook",
        "refresh_requested": refresh_requested,
        "refresh_applied": refresh_applied,
        "refresh_error": refresh_error,
        "refresh_min_interval_seconds": refresh_min_interval_seconds,
        "refresh_result": refresh_result,
        "start_date": str(start_date or ""),
        "end_date": str(end_date or ""),
        "include_completed": bool(include_completed),
        "include_unscheduled": bool(include_unscheduled),
        "limit": resolved_limit,
        "result_count": len(limited),
        "total_filtered_count": len(filtered),
        "summary": {
            "open_count": open_count,
            "completed_count": completed_count,
            "overdue_open_count": overdue_open_count,
            "today_open_count": today_open_count,
            "next_7d_open_count": next_7d_open_count,
            "provider_counts": {"outlook": len(filtered)},
        },
        "context_lines": context_lines,
        "results": limited,
    }


@mcp_threaded_tool()
def asana_calendar(
    query: str = "",
    start_date: str = "",
    end_date: str = "",
    board: str = "",
    section: str = "",
    refresh: bool = True,
    include_completed: bool = False,
    include_unscheduled: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    """
    Query Asana tasks from the per-user calendar cache in member.db.

    Examples:
    - `asana_calendar(start_date="2026-02-28", end_date="2026-03-07", refresh=True)`
    - `asana_calendar(query="infrastructure", board="Engineering", include_completed=False, limit=25)`
    - `asana_calendar(section="In Progress", include_unscheduled=True)`
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}

    resolved_query = str(query or "").strip().lower()
    resolved_board = str(board or "").strip().lower()
    resolved_section = str(section or "").strip().lower()
    resolved_limit = max(1, min(int(limit or 200), 1000))
    start_dt = _parse_ymd(start_date)
    end_dt = _parse_ymd(end_date)
    if start_date and start_dt is None:
        return {"ok": False, "error": "start_date must be YYYY-MM-DD", "results": []}
    if end_date and end_dt is None:
        return {"ok": False, "error": "end_date must be YYYY-MM-DD", "results": []}
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        return {"ok": False, "error": "end_date must be on/after start_date", "results": []}

    refresh_requested = bool(refresh)
    refresh_applied = False
    refresh_result: dict[str, Any] = {}
    refresh_error = ""
    refresh_min_interval_seconds = int(DEFAULT_CALENDAR_REFRESH_MIN_INTERVAL_SECONDS)
    if refresh_requested:
        try:
            refresh_result = refresh_calendar_cache_for_user(
                actor,
                provider="asana",
                force=False,
                min_interval_seconds=refresh_min_interval_seconds,
            )
            provider_result = refresh_result.get("asana") if isinstance(refresh_result, dict) else {}
            refresh_applied = bool(provider_result.get("refresh_attempted")) if isinstance(provider_result, dict) else False
        except Exception as exc:
            refresh_error = str(exc)

    rows = list_user_calendar_event_cache(
        actor,
        provider="asana",
        limit=5000,
        include_completed=bool(include_completed),
    )

    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_title = str(row.get("title") or "").strip()
        row_due_date = str(row.get("due_date") or "").strip()
        row_due_time = str(row.get("due_time") or "").strip()
        row_status = str(row.get("status") or "").strip().lower()
        row_source_url = str(row.get("source_url") or "").strip()
        row_payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        row_completed = bool(row.get("is_completed"))

        if not include_unscheduled and not row_due_date:
            continue

        row_due_dt = _parse_ymd(row_due_date) if row_due_date else None
        if start_dt is not None and (row_due_dt is None or row_due_dt < start_dt):
            continue
        if end_dt is not None and (row_due_dt is None or row_due_dt > end_dt):
            continue

        # Extract Asana-specific fields from payload
        memberships = row_payload.get("memberships") if isinstance(row_payload, dict) else []
        memberships = memberships if isinstance(memberships, list) else []
        row_boards: list[str] = []
        row_sections: list[str] = []
        for m in memberships:
            if not isinstance(m, dict):
                continue
            project = m.get("project") if isinstance(m.get("project"), dict) else {}
            project_name = str(project.get("name") or "").strip()
            if project_name:
                row_boards.append(project_name)
            sec = m.get("section") if isinstance(m.get("section"), dict) else {}
            sec_name = str(sec.get("name") or "").strip()
            if sec_name:
                row_sections.append(sec_name)
        row_notes = str(row_payload.get("notes") or "").strip() if isinstance(row_payload, dict) else ""
        workspace_name = ""
        if isinstance(row_payload, dict):
            ws = row_payload.get("workspace")
            if isinstance(ws, dict):
                workspace_name = str(ws.get("name") or "").strip()

        # Filter by board name
        if resolved_board:
            if not any(resolved_board in b.lower() for b in row_boards):
                continue

        # Filter by section name
        if resolved_section:
            if not any(resolved_section in s.lower() for s in row_sections):
                continue

        # Filter by query (title, notes, board, section, source_url)
        if resolved_query:
            haystack = " ".join(
                [
                    row_title,
                    row_notes,
                    row_status,
                    row_source_url,
                    row_due_date,
                    " ".join(row_boards),
                    " ".join(row_sections),
                    workspace_name,
                ]
            ).lower()
            if resolved_query not in haystack:
                continue

        filtered.append(
            {
                "provider": "asana",
                "event_id": str(row.get("event_id") or "").strip(),
                "title": row_title,
                "due_date": row_due_date,
                "due_time": row_due_time,
                "is_completed": row_completed,
                "status": row_status or ("completed" if row_completed else "open"),
                "source_url": row_source_url,
                "boards": row_boards,
                "sections": row_sections,
                "workspace": workspace_name,
                "notes": row_notes,
                "payload": row_payload,
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
        )

    filtered.sort(key=_calendar_row_sort_key)
    limited = filtered[:resolved_limit]

    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    seven_day_key = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")
    completed_count = 0
    open_count = 0
    overdue_open_count = 0
    today_open_count = 0
    next_7d_open_count = 0
    board_counts: dict[str, int] = {}
    for row in filtered:
        is_completed = bool(row.get("is_completed"))
        due_date = str(row.get("due_date") or "").strip()
        for b in row.get("boards") or []:
            board_counts[b] = board_counts.get(b, 0) + 1
        if is_completed:
            completed_count += 1
            continue
        open_count += 1
        if due_date:
            if due_date < today_key:
                overdue_open_count += 1
            if due_date == today_key:
                today_open_count += 1
            if today_key <= due_date <= seven_day_key:
                next_7d_open_count += 1

    context_lines = [_calendar_context_line(row) for row in limited[:50]]
    return {
        "ok": True,
        "tool": "asana_calendar",
        "query": str(query or ""),
        "board_filter": str(board or ""),
        "section_filter": str(section or ""),
        "provider": "asana",
        "refresh_requested": refresh_requested,
        "refresh_applied": refresh_applied,
        "refresh_error": refresh_error,
        "refresh_min_interval_seconds": refresh_min_interval_seconds,
        "refresh_result": refresh_result,
        "start_date": str(start_date or ""),
        "end_date": str(end_date or ""),
        "include_completed": bool(include_completed),
        "include_unscheduled": bool(include_unscheduled),
        "limit": resolved_limit,
        "result_count": len(limited),
        "total_filtered_count": len(filtered),
        "summary": {
            "open_count": open_count,
            "completed_count": completed_count,
            "overdue_open_count": overdue_open_count,
            "today_open_count": today_open_count,
            "next_7d_open_count": next_7d_open_count,
            "board_counts": board_counts,
        },
        "context_lines": context_lines,
        "results": limited,
    }


@mcp_threaded_tool()
def asana_get_subtasks(task_gid: str) -> dict[str, Any]:
    """
    List subtasks for an Asana task.

    Returns a list of subtask objects with gid, name, completed, due_on, assignee.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "subtasks": []}
    resolved_gid = str(task_gid or "").strip()
    if not resolved_gid:
        return {"ok": False, "error": "task_gid is required", "subtasks": []}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected"), "subtasks": []}
    subtasks, _trunc, fetch_error = _asana_api_list(
        access_token=access_token, path=f"/tasks/{resolved_gid}/subtasks",
        params={"opt_fields": "gid,name,completed,due_on,assignee.name"},
        max_items=100,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            subtasks, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed, path=f"/tasks/{resolved_gid}/subtasks",
                params={"opt_fields": "gid,name,completed,due_on,assignee.name"},
                max_items=100,
            )
    if fetch_error:
        return {"ok": False, "error": fetch_error, "subtasks": []}
    rows = [
        {"gid": t.get("gid"), "name": t.get("name"), "completed": t.get("completed"),
         "due_on": t.get("due_on"), "assignee": (t.get("assignee") or {}).get("name")}
        for t in (subtasks or [])
    ]
    return {"ok": True, "task_gid": resolved_gid, "subtasks": rows}


@mcp_threaded_tool()
def asana_list_sections(project_gid: str) -> dict[str, Any]:
    """
    List sections for an Asana project/board.

    Returns a list of section objects with gid and name.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "sections": []}
    resolved_gid = str(project_gid or "").strip()
    if not resolved_gid:
        return {"ok": False, "error": "project_gid is required", "sections": []}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected"), "sections": []}
    sections, _trunc, fetch_error = _asana_api_list(
        access_token=access_token, path=f"/projects/{resolved_gid}/sections",
        params={"opt_fields": "gid,name"},
        max_items=200,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            sections, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed, path=f"/projects/{resolved_gid}/sections",
                params={"opt_fields": "gid,name"},
                max_items=200,
            )
    if fetch_error:
        return {"ok": False, "error": fetch_error, "sections": []}
    rows = [{"gid": s.get("gid"), "name": s.get("name")} for s in (sections or [])]
    return {"ok": True, "project_gid": resolved_gid, "sections": rows}


@mcp_threaded_tool()
def asana_move_task_to_section(task_gid: str, section_gid: str) -> dict[str, Any]:
    """
    Move an Asana task into a specific section.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}
    resolved_task = str(task_gid or "").strip()
    resolved_section = str(section_gid or "").strip()
    if not resolved_task or not resolved_section:
        return {"ok": False, "error": "task_gid and section_gid are required"}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected")}
    _result, move_error = _asana_api_request_json(
        method="POST", access_token=access_token,
        path=f"/sections/{resolved_section}/addTask",
        body={"data": {"task": resolved_task}},
    )
    if _asana_error_requires_refresh(move_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            _result, move_error = _asana_api_request_json(
                method="POST", access_token=refreshed,
                path=f"/sections/{resolved_section}/addTask",
                body={"data": {"task": resolved_task}},
            )
    if move_error:
        return {"ok": False, "error": move_error}
    return {"ok": True, "task_gid": resolved_task, "section_gid": resolved_section}


@mcp_threaded_tool()
def asana_update_assignee(task_gid: str, assignee_gid: str = "") -> dict[str, Any]:
    """
    Update (or clear) the assignee on an Asana task.

    Pass an empty assignee_gid to unassign the task.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}
    resolved_task = str(task_gid or "").strip()
    if not resolved_task:
        return {"ok": False, "error": "task_gid is required"}
    resolved_assignee = str(assignee_gid or "").strip()
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected")}
    body_data = {"data": {"assignee": resolved_assignee if resolved_assignee else None}}
    _result, assign_error = _asana_api_request_json(
        method="PUT", access_token=access_token,
        path=f"/tasks/{resolved_task}", body=body_data,
    )
    if _asana_error_requires_refresh(assign_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            _result, assign_error = _asana_api_request_json(
                method="PUT", access_token=refreshed,
                path=f"/tasks/{resolved_task}", body=body_data,
            )
    if assign_error:
        return {"ok": False, "error": assign_error}
    return {"ok": True, "task_gid": resolved_task, "assignee_gid": resolved_assignee}


@mcp_threaded_tool()
def asana_list_workspace_members(workspace_gid: str) -> dict[str, Any]:
    """
    List members of an Asana workspace.

    Returns a list of user objects with gid, name, email.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "members": []}
    resolved_gid = str(workspace_gid or "").strip()
    if not resolved_gid:
        return {"ok": False, "error": "workspace_gid is required", "members": []}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected"), "members": []}
    members, _trunc, fetch_error = _asana_api_list(
        access_token=access_token, path=f"/workspaces/{resolved_gid}/users",
        params={"opt_fields": "gid,name,email"},
        max_items=500,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            members, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed, path=f"/workspaces/{resolved_gid}/users",
                params={"opt_fields": "gid,name,email"},
                max_items=500,
            )
    if fetch_error:
        return {"ok": False, "error": fetch_error, "members": []}
    rows = [{"gid": m.get("gid"), "name": m.get("name"), "email": m.get("email")} for m in (members or [])]
    return {"ok": True, "workspace_gid": resolved_gid, "members": rows}


@mcp_threaded_tool()
def asana_get_dependencies(task_gid: str) -> dict[str, Any]:
    """
    List dependencies for an Asana task.

    Returns a list of dependency task objects with gid, name, completed.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "dependencies": []}
    resolved_gid = str(task_gid or "").strip()
    if not resolved_gid:
        return {"ok": False, "error": "task_gid is required", "dependencies": []}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected"), "dependencies": []}
    deps, _trunc, fetch_error = _asana_api_list(
        access_token=access_token, path=f"/tasks/{resolved_gid}/dependencies",
        params={"opt_fields": "gid,name,completed"},
        max_items=100,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            deps, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed, path=f"/tasks/{resolved_gid}/dependencies",
                params={"opt_fields": "gid,name,completed"},
                max_items=100,
            )
    if fetch_error:
        return {"ok": False, "error": fetch_error, "dependencies": []}
    rows = [{"gid": d.get("gid"), "name": d.get("name"), "completed": d.get("completed")} for d in (deps or [])]
    return {"ok": True, "task_gid": resolved_gid, "dependencies": rows}


@mcp_threaded_tool()
def asana_add_dependency(task_gid: str, dependency_gid: str) -> dict[str, Any]:
    """
    Add a dependency to an Asana task (task_gid depends on dependency_gid).
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}
    resolved_task = str(task_gid or "").strip()
    resolved_dep = str(dependency_gid or "").strip()
    if not resolved_task or not resolved_dep:
        return {"ok": False, "error": "task_gid and dependency_gid are required"}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected")}
    _result, add_error = _asana_api_request_json(
        method="POST", access_token=access_token,
        path=f"/tasks/{resolved_task}/addDependencies",
        body={"data": {"dependencies": [resolved_dep]}},
    )
    if _asana_error_requires_refresh(add_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            _result, add_error = _asana_api_request_json(
                method="POST", access_token=refreshed,
                path=f"/tasks/{resolved_task}/addDependencies",
                body={"data": {"dependencies": [resolved_dep]}},
            )
    if add_error:
        return {"ok": False, "error": add_error}
    return {"ok": True, "task_gid": resolved_task, "dependency_gid": resolved_dep}


@mcp_threaded_tool()
def asana_remove_dependency(task_gid: str, dependency_gid: str) -> dict[str, Any]:
    """
    Remove a dependency from an Asana task.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}
    resolved_task = str(task_gid or "").strip()
    resolved_dep = str(dependency_gid or "").strip()
    if not resolved_task or not resolved_dep:
        return {"ok": False, "error": "task_gid and dependency_gid are required"}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected")}
    _result, remove_error = _asana_api_request_json(
        method="POST", access_token=access_token,
        path=f"/tasks/{resolved_task}/removeDependencies",
        body={"data": {"dependencies": [resolved_dep]}},
    )
    if _asana_error_requires_refresh(remove_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            _result, remove_error = _asana_api_request_json(
                method="POST", access_token=refreshed,
                path=f"/tasks/{resolved_task}/removeDependencies",
                body={"data": {"dependencies": [resolved_dep]}},
            )
    if remove_error:
        return {"ok": False, "error": remove_error}
    return {"ok": True, "task_gid": resolved_task, "dependency_gid": resolved_dep}


@mcp_threaded_tool()
def asana_get_project_status(project_gid: str) -> dict[str, Any]:
    """
    Get the latest status update for an Asana project/board.

    Returns the most recent status entry with title, color, text, author, and created_at.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "latest_status": None}
    resolved_gid = str(project_gid or "").strip()
    if not resolved_gid:
        return {"ok": False, "error": "project_gid is required", "latest_status": None}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected"), "latest_status": None}
    statuses, _trunc, fetch_error = _asana_api_list(
        access_token=access_token, path=f"/projects/{resolved_gid}/project_statuses",
        params={"opt_fields": "gid,title,color,text,created_at,author.name", "limit": 5},
        max_items=5,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            statuses, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed, path=f"/projects/{resolved_gid}/project_statuses",
                params={"opt_fields": "gid,title,color,text,created_at,author.name", "limit": 5},
                max_items=5,
            )
    if fetch_error:
        return {"ok": False, "error": fetch_error, "latest_status": None}
    latest = statuses[0] if statuses else None
    return {"ok": True, "project_gid": resolved_gid, "latest_status": latest}


@mcp_threaded_tool()
def asana_get_attachments(task_gid: str) -> dict[str, Any]:
    """
    List attachments for an Asana task.

    Returns a list of attachment objects with gid, name, download_url, view_url, created_at, size.
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "attachments": []}
    resolved_gid = str(task_gid or "").strip()
    if not resolved_gid:
        return {"ok": False, "error": "task_gid is required", "attachments": []}
    access_token, token_error = _asana_access_token_for_user(actor)
    if not access_token:
        return {"ok": False, "error": str(token_error or "asana_not_connected"), "attachments": []}
    attachments, _trunc, fetch_error = _asana_api_list(
        access_token=access_token, path=f"/tasks/{resolved_gid}/attachments",
        params={"opt_fields": "gid,name,download_url,view_url,created_at,size"},
        max_items=100,
    )
    if _asana_error_requires_refresh(fetch_error):
        refreshed, _ = _asana_access_token_for_user(actor, force_refresh=True)
        if refreshed:
            attachments, _trunc, fetch_error = _asana_api_list(
                access_token=refreshed, path=f"/tasks/{resolved_gid}/attachments",
                params={"opt_fields": "gid,name,download_url,view_url,created_at,size"},
                max_items=100,
            )
    if fetch_error:
        return {"ok": False, "error": fetch_error, "attachments": []}
    rows = [
        {"gid": a.get("gid"), "name": a.get("name"), "download_url": a.get("download_url"),
         "view_url": a.get("view_url"), "created_at": a.get("created_at"), "size": a.get("size")}
        for a in (attachments or [])
    ]
    return {"ok": True, "task_gid": resolved_gid, "attachments": rows}


@mcp_threaded_tool()
def alert_filter_prompt(
    action: str = "get",
    prompt: str = "",
) -> dict[str, Any]:
    """
    Read or update the authenticated user's alert filtering prompt.

    Actions:
    - get
    - replace (or set)
    - append
    - clear
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    resolved_action = str(action or "get").strip().lower() or "get"
    if resolved_action in {"get", "read"}:
        payload = get_user_alert_filter_prompt(actor)
        return {
            "ok": True,
            "action": "get",
            "prompt": str(payload.get("prompt") or ""),
            "updated_at": str(payload.get("updated_at") or ""),
        }

    if resolved_action in {"set"}:
        resolved_action = "replace"

    if resolved_action not in {"replace", "append", "clear"}:
        return {"ok": False, "error": "action must be one of: get, replace, append, clear"}

    try:
        payload = update_user_alert_filter_prompt(
            actor,
            prompt=prompt,
            mode=resolved_action,
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    return {
        "ok": True,
        "action": resolved_action,
        "prompt": str(payload.get("prompt") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
    }


@mcp_threaded_tool()
def resource_health_check(resource_uuid: str) -> dict[str, Any]:
    """
    Run a health check for a resource and return the latest status details.

    Access policy:
    - superuser: allowed
    - global resources: allowed for any authenticated user
    - user resources: owner only
    - team-shared resources: members of shared teams
    """
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return {"ok": False, "error": "resource_uuid is required"}

    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    owner_user, resource = _resolve_resource_for_health_check(resolved_uuid)
    if owner_user is None or resource is None:
        return {"ok": False, "error": f"resource not found: {resolved_uuid}"}

    if not _actor_can_check_resource(actor=actor, owner_user=owner_user, resource_uuid=resolved_uuid):
        return {"ok": False, "error": f"access denied for resource: {resolved_uuid}"}

    try:
        result = check_health(int(resource.id), user=owner_user, emit_transition_log=True)
    except Exception as exc:
        return {"ok": False, "error": f"health check failed: {exc}"}

    return {
        "ok": True,
        "resource_uuid": resolved_uuid,
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


@mcp_threaded_tool()
def resource_logs(
    resource_uuid: str,
    limit: int = 200,
    level: str = "",
    contains: str = "",
) -> dict[str, Any]:
    """
    Query structured logs for a resource.

    Access policy:
    - superuser: allowed
    - global resources: allowed for any authenticated user
    - user resources: owner only
    - team-shared resources: members of shared teams
    """
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return {"ok": False, "error": "resource_uuid is required"}

    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    owner_user, resource = _resolve_resource_for_health_check(resolved_uuid)
    if owner_user is None or resource is None:
        return {"ok": False, "error": f"resource not found: {resolved_uuid}"}

    if not _actor_can_check_resource(actor=actor, owner_user=owner_user, resource_uuid=resolved_uuid):
        return {"ok": False, "error": f"access denied for resource: {resolved_uuid}"}

    resolved_limit = max(1, min(int(limit or 200), 1000))
    rows = list_resource_logs(owner_user, resolved_uuid, limit=resolved_limit)

    resolved_level = str(level or "").strip().lower()
    resolved_contains = str(contains or "").strip().lower()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_level = str(row.get("level") or "").strip().lower()
        row_message = str(row.get("message") or "").strip()
        row_logger = str(row.get("logger") or "").strip()
        if resolved_level and row_level != resolved_level:
            continue
        if resolved_contains:
            haystack = f"{row_message} {row_logger}".lower()
            if resolved_contains not in haystack:
                continue
        filtered.append(row)

    return {
        "ok": True,
        "resource_uuid": resolved_uuid,
        "resource_name": str(getattr(resource, "name", "") or ""),
        "owner_username": str(getattr(owner_user, "username", "") or ""),
        "limit": resolved_limit,
        "level": resolved_level,
        "contains": resolved_contains,
        "result_count": len(filtered),
        "results": filtered,
    }


@mcp_threaded_tool()
def resource_ssh_exec(
    resource_uuid: str,
    command: str,
    timeout_seconds: int = 30,
    max_output_chars: int = 12000,
) -> dict[str, Any]:
    """
    Execute a one-shot SSH command on an accessible VM resource.

    Access policy:
    - superuser: allowed
    - global resources: allowed for any authenticated user
    - user resources: owner only
    - team-shared resources: members of shared teams
    """
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return {"ok": False, "error": "resource_uuid is required"}

    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}

    owner_user, resource = _resolve_resource_for_health_check(resolved_uuid)
    if owner_user is None or resource is None:
        return {"ok": False, "error": f"resource not found: {resolved_uuid}"}

    if not _actor_can_check_resource(actor=actor, owner_user=owner_user, resource_uuid=resolved_uuid):
        return {"ok": False, "error": f"access denied for resource: {resolved_uuid}"}

    result = execute_resource_ssh_command(
        owner_user=owner_user,
        resource=resource,
        command=command,
        timeout_seconds=timeout_seconds,
        max_output_chars=max_output_chars,
    )
    result["resource_uuid"] = resolved_uuid
    result["resource_name"] = str(getattr(resource, "name", "") or "")
    result["owner_username"] = str(getattr(owner_user, "username", "") or "")
    return result


@mcp_threaded_tool()
def resource_kb(
    resource_uuid: str,
    query: str = "",
    limit: int = 8,
) -> dict[str, Any]:
    """
    Search the resource-scoped knowledge base for a specific resource.
    """
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return {"ok": False, "error": "resource_uuid is required", "results": []}

    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}

    owner_user, resource = _resolve_resource_for_health_check(resolved_uuid)
    if owner_user is None or resource is None:
        return {"ok": False, "error": f"resource not found: {resolved_uuid}", "results": []}

    if not _actor_can_check_resource(actor=actor, owner_user=owner_user, resource_uuid=resolved_uuid):
        return {"ok": False, "error": f"access denied for resource: {resolved_uuid}", "results": []}

    owner_context = get_resource_owner_context(owner_user, resolved_uuid)
    resource_dir = Path(owner_context.get("resource_dir") or "")
    resource_kb_path = resource_dir / "knowledge.db" if resource_dir else Path("")
    resolved_limit = max(1, min(int(limit or 8), 50))
    rows, query_error = _query_chroma_resources(
        knowledge_path=resource_kb_path,
        query=query,
        limit=resolved_limit,
    )
    if query_error:
        return {"ok": False, "error": query_error, "results": []}

    return {
        "ok": True,
        "resource_uuid": resolved_uuid,
        "resource_name": str(getattr(resource, "name", "") or ""),
        "owner_username": str(getattr(owner_user, "username", "") or ""),
        "query": str(query or ""),
        "limit": resolved_limit,
        "knowledge_path": str(resource_kb_path),
        "result_count": len(rows),
        "results": rows,
    }


@mcp_threaded_tool()
def search_users(
    query: str = "",
    phone: str = "",
    limit: int = 10,
) -> dict[str, Any]:
    """
    Search user records in global Chroma user_records collection.

    Access policy:
    - superuser: allowed
    - non-superuser: denied
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}
    if not bool(getattr(actor, "is_superuser", False)):
        return {"ok": False, "error": "superuser access required", "results": []}

    resolved_query = str(query or "").strip()
    resolved_phone = str(phone or "").strip()
    resolved_limit = max(1, min(int(limit or 10), 100))
    rows, query_error = query_user_records(
        query=resolved_query,
        phone=resolved_phone,
        limit=resolved_limit,
    )
    if query_error:
        return {"ok": False, "error": query_error, "results": []}

    return {
        "ok": True,
        "collection": "user_records",
        "query": resolved_query,
        "phone": resolved_phone,
        "limit": resolved_limit,
        "result_count": len(rows),
        "results": rows,
    }


@mcp_threaded_tool()
def directory(
    query: str = "",
    phone: str = "",
) -> dict[str, Any]:
    """
    Search user records in global Chroma user_records collection and return top 4 results.

    Access policy:
    - superuser: allowed
    - non-superuser: denied
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required", "results": []}
    if not bool(getattr(actor, "is_superuser", False)):
        return {"ok": False, "error": "superuser access required", "results": []}

    resolved_query = str(query or "").strip()
    resolved_phone = str(phone or "").strip()
    fixed_limit = 4
    rows, query_error = query_user_records(
        query=resolved_query,
        phone=resolved_phone,
        limit=fixed_limit,
    )
    if query_error:
        return {"ok": False, "error": query_error, "results": []}

    return {
        "ok": True,
        "collection": "user_records",
        "query": resolved_query,
        "phone": resolved_phone,
        "limit": fixed_limit,
        "result_count": len(rows),
        "results": rows,
    }


@mcp_threaded_tool()
def sms(
    message: str,
    username: str = "",
    phone_number: str = "",
) -> dict[str, Any]:
    """
    Send an SMS via Twilio to a username's configured phone number or a direct phone number.

    Access policy:
    - superuser: allowed
    - non-superuser: denied
    """
    actor = _request_actor()
    if actor is None:
        return {"ok": False, "error": "authenticated user identity is required"}
    if not bool(getattr(actor, "is_superuser", False)):
        return {"ok": False, "error": "superuser access required"}

    body = str(message or "").strip()
    if not body:
        return {"ok": False, "error": "message is required"}
    body = body[:1200]

    resolved_username = _clean(username)
    resolved_phone_input = _clean(phone_number)
    if not resolved_username and not resolved_phone_input:
        return {"ok": False, "error": "either username or phone_number is required"}

    target_phone = ""
    target_user = None
    if resolved_phone_input:
        target_phone = _normalize_phone(resolved_phone_input)
        if not target_phone:
            return {"ok": False, "error": "invalid phone_number"}
    else:
        target_phone, target_user = _target_phone_from_username(resolved_username)
        if target_user is None:
            return {"ok": False, "error": f"user not found: {resolved_username}"}
        if not target_phone:
            return {"ok": False, "error": f"user has no phone number: {resolved_username}"}

    account_sid, auth_token, from_number = _twilio_sms_credentials()
    if not (account_sid and auth_token and from_number):
        return {"ok": False, "error": "twilio_not_configured"}

    try:
        response = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            data={
                "To": target_phone,
                "From": from_number,
                "Body": body,
            },
            auth=(account_sid, auth_token),
            timeout=10,
        )
    except requests.RequestException as exc:
        return {"ok": False, "error": f"twilio_request_failed:{exc}"}

    response_payload: dict[str, Any] = {}
    try:
        parsed = response.json() if response.content else {}
        if isinstance(parsed, dict):
            response_payload = parsed
    except Exception:
        response_payload = {}

    if not (200 <= int(response.status_code) < 300):
        return {
            "ok": False,
            "error": f"twilio_status_{int(response.status_code)}",
            "status_code": int(response.status_code),
            "details": str(response.text or "")[:500],
        }

    return {
        "ok": True,
        "status_code": int(response.status_code),
        "to": target_phone,
        "from": from_number,
        "username": str(getattr(target_user, "username", "") or resolved_username),
        "user_id": int(getattr(target_user, "id", 0) or 0),
        "message_sid": str(response_payload.get("sid") or ""),
        "message_status": str(response_payload.get("status") or ""),
    }


mcp.settings.streamable_http_path = "/"
mcp_app = mcp.streamable_http_app()
app = FastAPI(lifespan=lambda app: mcp.session_manager.run())
app.mount("/mcp/", mcp_app)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _proxy_headers(request: Request) -> dict[str, str]:
    forwarded: dict[str, str] = {}
    for key, value in request.headers.items():
        lowered = key.lower()
        if lowered in {"host", "content-length"}:
            continue
        forwarded[key] = value
    return forwarded


def _proxy_mcp_request(*, request: Request, upstream_url: str, body: bytes, suffix: str = ""):
    resolved_base = str(upstream_url or "").strip()
    if not resolved_base:
        return JSONResponse({"detail": "MCP upstream is not configured"}, status_code=503)
    target_url = urljoin(resolved_base.rstrip("/") + "/", suffix.lstrip("/"))
    try:
        response = requests.request(
            method=request.method,
            url=target_url,
            params=request.query_params,
            headers=_proxy_headers(request),
            data=body,
            timeout=60,
        )
    except requests.RequestException as exc:
        return JSONResponse({"detail": f"MCP upstream request failed: {exc}"}, status_code=502)

    content_type = response.headers.get("content-type") or ""
    if "application/json" in content_type.lower():
        try:
            payload = response.json()
        except Exception:
            payload = {"detail": response.text}
        return JSONResponse(payload, status_code=response.status_code)

    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=content_type or None,
    )


@app.api_route("/github/", methods=["GET", "POST"])
@app.api_route("/github/{path:path}", methods=["GET", "POST"])
async def github_proxy(request: Request, path: str = ""):
    body = await request.body() if request.method in {"POST", "PUT", "PATCH"} else b""
    return _proxy_mcp_request(
        request=request,
        upstream_url=GITHUB_MCP_UPSTREAM_URL,
        body=body,
        suffix=path,
    )


@app.api_route("/asana/", methods=["GET", "POST"])
@app.api_route("/asana/{path:path}", methods=["GET", "POST"])
async def asana_proxy(request: Request, path: str = ""):
    body = await request.body() if request.method in {"POST", "PUT", "PATCH"} else b""
    return _proxy_mcp_request(
        request=request,
        upstream_url=ASANA_MCP_UPSTREAM_URL,
        body=body,
        suffix=path,
    )


@app.middleware("http")
async def require_global_api_key(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS" or path == "/health":
        return await call_next(request)
    token = _REQUEST_AUTH.set(None)

    api_key = _extract_api_key(request)
    if not api_key:
        # Only attempt Twilio auth when a Twilio signature header is present.
        # This keeps regular requests independent from Twilio config/state.
        twilio_signature = (request.headers.get(TWILIO_SIGNATURE_HEADER) or "").strip()
        if twilio_signature:
            twilio_ok, twilio_error = await _authenticate_twilio_phone_request(request)
            if not twilio_ok:
                try:
                    return JSONResponse(
                        {
                            "detail": "Invalid Twilio authentication",
                            "twilio_error": twilio_error,
                        },
                        status_code=401,
                    )
                finally:
                    _REQUEST_AUTH.reset(token)
            _set_request_auth_context(
                auth_scope=str(getattr(request.state, "auth_scope", "twilio_phone")),
                user_id=int(getattr(request.state, "auth_user_id", 0) or 0),
                username=str(getattr(request.state, "auth_username", "") or ""),
                email=str(getattr(request.state, "auth_email", "") or ""),
                phone=str(getattr(request.state, "auth_phone", "") or ""),
            )
            try:
                return await call_next(request)
            finally:
                _REQUEST_AUTH.reset(token)
        try:
            return JSONResponse(
                {
                    "detail": f"Missing API key (expected {API_KEY_HEADER})",
                },
                status_code=401,
            )
        finally:
            _REQUEST_AUTH.reset(token)

    username = (request.headers.get(USERNAME_HEADER) or "").strip()
    email = (request.headers.get(EMAIL_HEADER) or "").strip()
    phone = (request.headers.get(PHONE_HEADER) or "").strip()
    resource_uuid = (request.headers.get(RESOURCE_UUID_HEADER) or "").strip()
    auth = await sync_to_async(authenticate_api_key, thread_sensitive=True)(
        api_key=api_key,
        username=username,
        email=email,
        phone=phone,
        resource_uuid=resource_uuid,
        require_resource_access=bool(resource_uuid),
    )
    if not auth.ok:
        try:
            return JSONResponse({"detail": "Invalid API key"}, status_code=401)
        finally:
            _REQUEST_AUTH.reset(token)

    request.state.auth_scope = auth.key_scope
    if auth.user is not None:
        request.state.auth_user_id = int(getattr(auth.user, "id", 0) or 0)
        request.state.auth_username = str(getattr(auth.user, "username", "") or "")
        request.state.auth_email = str(getattr(auth.user, "email", "") or "")

    _set_request_auth_context(
        auth_scope=auth.key_scope,
        user_id=int(getattr(request.state, "auth_user_id", 0) or 0),
        username=str(getattr(request.state, "auth_username", "") or ""),
        email=str(getattr(request.state, "auth_email", "") or ""),
        phone=str(getattr(request.state, "auth_phone", "") or ""),
    )
    try:
        return await call_next(request)
    finally:
        _REQUEST_AUTH.reset(token)
