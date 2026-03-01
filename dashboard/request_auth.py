from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import re

from django.contrib.auth import get_user_model

from .global_api_key_store import is_valid_global_team_api_key
from .models import ResourcePackageOwner, ResourceRouteAlias, ResourceTeamShare, SystemSetup, UserNotificationSettings
from .resources_store import resolve_api_key_scope

logger = logging.getLogger(__name__)


@dataclass
class APIKeyAuthResult:
    ok: bool
    reason: str
    key_scope: str
    user: object | None
    identity_type: str
    identity_value: str


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


def get_twilio_auth_token() -> str:
    env_token = _clean(os.getenv("TWILIO_AUTH_TOKEN"))
    if env_token:
        return env_token
    setup = SystemSetup.objects.order_by("-updated_at", "-created_at").first()
    if not setup:
        return ""
    return _clean(getattr(setup, "twilio_auth_token", ""))


def resolve_user_by_phone(phone: str):
    normalized = _normalize_phone(phone)
    if not normalized:
        return None
    row = (
        UserNotificationSettings.objects.select_related("user")
        .filter(phone_number__in={normalized, normalized.lstrip("+")}, user__is_active=True)
        .first()
    )
    if row is not None and row.user_id:
        return row.user

    # Fallback for legacy unnormalized records.
    for candidate in UserNotificationSettings.objects.select_related("user").filter(user__is_active=True):
        stored = _normalize_phone(getattr(candidate, "phone_number", ""))
        if stored and stored == normalized and candidate.user_id:
            return candidate.user
    return None


def _resolve_user_identity(*, username: str = "", email: str = "", phone: str = "") -> tuple[object | None, str, str]:
    User = get_user_model()

    resolved_username = _clean(username)
    if resolved_username:
        user = User.objects.filter(username__iexact=resolved_username, is_active=True).first()
        if user is not None:
            return user, "username", resolved_username

    resolved_email = _clean(email)
    if resolved_email:
        user = User.objects.filter(email__iexact=resolved_email, is_active=True).first()
        if user is not None:
            return user, "email", resolved_email

    resolved_phone = _clean(phone)
    if resolved_phone:
        user = resolve_user_by_phone(resolved_phone)
        if user is not None:
            return user, "phone", resolved_phone

    return None, "", ""


def _owner_for_resource(resource_uuid: str):
    resolved_uuid = _clean(resource_uuid)
    if not resolved_uuid:
        return None

    package = (
        ResourcePackageOwner.objects.select_related("owner_user", "owner_team")
        .filter(resource_uuid=resolved_uuid)
        .first()
    )
    if package is not None and package.owner_user_id:
        return package.owner_user

    alias = (
        ResourceRouteAlias.objects.select_related("owner_user")
        .filter(resource_uuid=resolved_uuid)
        .order_by("-is_current", "-updated_at", "-created_at")
        .first()
    )
    if alias is not None and alias.owner_user_id:
        return alias.owner_user
    return None


def _resolve_user_by_member_api_key(*, api_key: str, resource_uuid: str = "") -> tuple[object | None, str]:
    resolved_key = _clean(api_key)
    resolved_uuid = _clean(resource_uuid)
    if not resolved_key:
        return None, ""

    User = get_user_model()
    matched_user = None
    matched_scope = ""
    for candidate in User.objects.filter(is_active=True).order_by("id"):
        try:
            scope = resolve_api_key_scope(candidate, resolved_key, resolved_uuid)
        except Exception:
            logger.debug(
                "Skipping API key scope lookup for user id=%s due to lookup error.",
                getattr(candidate, "id", None),
                exc_info=True,
            )
            continue
        if scope not in {"account", "resource"}:
            continue
        if matched_user is not None and int(getattr(matched_user, "id", 0) or 0) != int(getattr(candidate, "id", 0) or 0):
            # Ambiguous key match should not happen; fail closed.
            return None, ""
        matched_user = candidate
        matched_scope = scope
        if scope == "account":
            break
    return matched_user, matched_scope


def user_can_access_resource(*, user, resource_uuid: str) -> bool:
    resolved_uuid = _clean(resource_uuid)
    if user is None or not resolved_uuid:
        return False
    if not bool(getattr(user, "is_authenticated", True)):
        return False
    if bool(getattr(user, "is_superuser", False)):
        return True

    package = (
        ResourcePackageOwner.objects.select_related("owner_user", "owner_team")
        .filter(resource_uuid=resolved_uuid)
        .first()
    )
    actor_team_ids = list(user.groups.values_list("id", flat=True))

    if package is None:
        if not actor_team_ids:
            return False
        return ResourceTeamShare.objects.filter(
            resource_uuid=resolved_uuid,
            team_id__in=actor_team_ids,
        ).exists()

    scope = _clean(getattr(package, "owner_scope", "")).lower()
    if scope == ResourcePackageOwner.OWNER_SCOPE_GLOBAL:
        return False

    if scope == ResourcePackageOwner.OWNER_SCOPE_TEAM and package.owner_team_id:
        if user.groups.filter(id=package.owner_team_id).exists():
            return True
        if not actor_team_ids:
            return False
        return ResourceTeamShare.objects.filter(
            resource_uuid=resolved_uuid,
            team_id__in=actor_team_ids,
        ).exists()

    owner = getattr(package, "owner_user", None)
    if owner is None:
        if not actor_team_ids:
            return False
        return ResourceTeamShare.objects.filter(
            resource_uuid=resolved_uuid,
            team_id__in=actor_team_ids,
        ).exists()
    if int(getattr(owner, "id", 0) or 0) == int(getattr(user, "id", 0) or 0):
        return True
    if not actor_team_ids:
        return False
    return ResourceTeamShare.objects.filter(
        owner=owner,
        resource_uuid=resolved_uuid,
        team_id__in=actor_team_ids,
    ).exists()


def authenticate_api_key(
    *,
    api_key: str,
    username: str = "",
    email: str = "",
    phone: str = "",
    resource_uuid: str = "",
    resource_owner=None,
    require_resource_access: bool = False,
) -> APIKeyAuthResult:
    resolved_key = _clean(api_key)
    resolved_uuid = _clean(resource_uuid)
    if not resolved_key:
        return APIKeyAuthResult(False, "missing_api_key", "", None, "", "")

    identity_user, identity_type, identity_value = _resolve_user_identity(
        username=username,
        email=email,
        phone=phone,
    )

    if is_valid_global_team_api_key(resolved_key):
        if require_resource_access and resolved_uuid:
            if identity_user is None:
                return APIKeyAuthResult(False, "identity_required", "", None, identity_type, identity_value)
            if not user_can_access_resource(user=identity_user, resource_uuid=resolved_uuid):
                return APIKeyAuthResult(False, "resource_access_denied", "", identity_user, identity_type, identity_value)
        return APIKeyAuthResult(True, "", "global", identity_user, identity_type, identity_value)

    if identity_user is None:
        inferred_user, inferred_scope = _resolve_user_by_member_api_key(
            api_key=resolved_key,
            resource_uuid=resolved_uuid,
        )
        if inferred_user is not None and inferred_scope in {"account", "resource"}:
            if require_resource_access and resolved_uuid:
                if not user_can_access_resource(user=inferred_user, resource_uuid=resolved_uuid):
                    return APIKeyAuthResult(False, "resource_access_denied", "", inferred_user, "api_key", "")
            return APIKeyAuthResult(True, "", inferred_scope, inferred_user, "api_key", "")

    if identity_user is not None:
        user_scope = resolve_api_key_scope(identity_user, resolved_key, resolved_uuid)
        if user_scope == "account":
            if require_resource_access and resolved_uuid:
                if not user_can_access_resource(user=identity_user, resource_uuid=resolved_uuid):
                    return APIKeyAuthResult(False, "resource_access_denied", "", identity_user, identity_type, identity_value)
            return APIKeyAuthResult(True, "", "account", identity_user, identity_type, identity_value)

        if user_scope == "resource":
            if require_resource_access and resolved_uuid:
                if not user_can_access_resource(user=identity_user, resource_uuid=resolved_uuid):
                    return APIKeyAuthResult(False, "resource_access_denied", "", identity_user, identity_type, identity_value)
            return APIKeyAuthResult(True, "", "resource", identity_user, identity_type, identity_value)

    owner = resource_owner
    if owner is None and resolved_uuid:
        owner = _owner_for_resource(resolved_uuid)
    if owner is not None and resolved_uuid:
        owner_scope = resolve_api_key_scope(owner, resolved_key, resolved_uuid)
        if owner_scope == "resource":
            return APIKeyAuthResult(True, "", "resource", identity_user, identity_type, identity_value)

    return APIKeyAuthResult(False, "invalid_api_key", "", identity_user, identity_type, identity_value)
