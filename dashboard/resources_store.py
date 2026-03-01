import base64
import hashlib
import json
import shutil
import sqlite3
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List

from django.conf import settings
from django.contrib.auth.models import Group
from django.db import transaction
from django.utils.text import slugify
from cryptography.fernet import Fernet, InvalidToken

from .api_key_utils import generate_api_key, hash_api_key, key_prefix, key_preview


@dataclass
class ResourceItem:
    id: int
    resource_uuid: str
    name: str
    access_scope: str
    team_names: list[str]
    resource_type: str
    target: str
    address: str
    port: str
    db_type: str
    healthcheck_url: str
    notes: str
    created_at: str
    last_status: str
    last_checked_at: str
    last_error: str
    ssh_key_name: str
    ssh_username: str
    ssh_key_text: str
    ssh_key_present: bool
    ssh_port: str
    resource_subtype: str
    resource_metadata: dict[str, Any]
    ssh_credential_id: str
    ssh_credential_scope: str


@dataclass
class SSHCredentialItem:
    id: str
    name: str
    scope: str
    team_names: list[str]
    created_at: str


@dataclass
class UserAPIKeyItem:
    id: int
    key_type: str
    name: str
    resource_uuid: str
    key_prefix: str
    created_at: str


@dataclass
class ResourceNoteItem:
    id: int
    resource_uuid: str
    body: str
    author_user_id: int
    author_username: str
    created_at: str
    attachment_id: int | None
    attachment_name: str
    attachment_content_type: str
    attachment_size: int


@dataclass
class ResourceCheckItem:
    id: int
    resource_id: int
    status: str
    checked_at: str
    target: str
    error: str
    check_method: str
    latency_ms: float | None
    packet_loss_pct: float | None


_SSH_KEY_PREFIX = "enc:"

REMINDER_STATUS_SCHEDULED = "scheduled"
REMINDER_STATUS_SENT = "sent"
REMINDER_STATUS_CANCELED = "canceled"
REMINDER_STATUS_ERROR = "error"
REMINDER_VALID_STATUSES = {
    REMINDER_STATUS_SCHEDULED,
    REMINDER_STATUS_SENT,
    REMINDER_STATUS_CANCELED,
    REMINDER_STATUS_ERROR,
}
REMINDER_DEFAULT_ACTION = "notify_user"
REMINDER_DEFAULT_CHANNELS = {
    "APP": True,
    "SMS": True,
    "EMAIL": False,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(value: Any, *, field_name: str) -> datetime:
    candidate = str(value or "").strip()
    if not candidate:
        raise ValueError(f"{field_name} is required")
    candidate = candidate.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO datetime for {field_name}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_reminder_recipients(recipients: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in recipients or []:
        username = str(raw or "").strip().lstrip("@").lower()
        if not username or username in seen:
            continue
        normalized.append(username)
        seen.add(username)
    return normalized


def _normalize_reminder_channels(channels: dict[str, Any] | None) -> dict[str, bool]:
    normalized = {name: bool(enabled) for name, enabled in REMINDER_DEFAULT_CHANNELS.items()}
    if not isinstance(channels, dict):
        return normalized
    alias_map = {
        "app": "APP",
        "sms": "SMS",
        "email": "EMAIL",
    }
    for raw_key, raw_value in channels.items():
        key = alias_map.get(str(raw_key or "").strip().lower())
        if not key:
            continue
        normalized[key] = bool(raw_value)
    return normalized


def _normalize_reminder_status(status: str | None) -> str:
    normalized = str(status or "").strip().lower()
    if normalized not in REMINDER_VALID_STATUSES:
        raise ValueError("status must be one of: scheduled, sent, canceled, error")
    return normalized


def _json_dumps_compact(value: Any) -> str:
    try:
        return json.dumps(value, separators=(",", ":"))
    except TypeError:
        return json.dumps(str(value), separators=(",", ":"))


def _json_loads_or(value: Any, *, default: Any) -> Any:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _row_to_reminder(row: sqlite3.Row) -> dict[str, Any]:
    recipients = _json_loads_or(row["recipients_json"], default=[])
    if not isinstance(recipients, list):
        recipients = []
    channels = _normalize_reminder_channels(_json_loads_or(row["channels_json"], default={}))
    metadata = _json_loads_or(row["metadata_json"], default={})
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "id": int(row["id"] or 0),
        "title": str(row["title"] or "").strip(),
        "message": str(row["message"] or "").strip(),
        "recipients": _normalize_reminder_recipients([str(item) for item in recipients]),
        "remind_at": str(row["remind_at"] or "").strip(),
        "status": str(row["status"] or REMINDER_STATUS_SCHEDULED).strip().lower() or REMINDER_STATUS_SCHEDULED,
        "action": str(row["action"] or REMINDER_DEFAULT_ACTION).strip().lower() or REMINDER_DEFAULT_ACTION,
        "channels": channels,
        "metadata": metadata,
        "last_error": str(row["last_error"] or "").strip(),
        "created_by_user_id": int(row["created_by_user_id"]) if row["created_by_user_id"] is not None else None,
        "created_by_username": str(row["created_by_username"] or "").strip(),
        "created_at": str(row["created_at"] or "").strip(),
        "updated_at": str(row["updated_at"] or "").strip(),
        "sent_at": str(row["sent_at"] or "").strip(),
    }


def _fernet_instances() -> list[Fernet]:
    keys = getattr(settings, 'SSH_KEY_MASTER_KEYS', None) or []
    instances: list[Fernet] = []
    for key in keys:
        try:
            instances.append(Fernet(key))
        except Exception:
            continue
    if not instances:
        digest = hashlib.sha256(settings.SECRET_KEY.encode('utf-8')).digest()
        fallback = base64.urlsafe_b64encode(digest)
        instances.append(Fernet(fallback))
    return instances


def _is_encrypted(value: str) -> bool:
    return value.startswith(_SSH_KEY_PREFIX)


def _encrypt_key_text(value: str) -> str:
    normalized = value.replace("\r", "").strip()
    if normalized:
        normalized = f"{normalized}\n"
    fernet = _fernet_instances()[0]
    token = fernet.encrypt(normalized.encode("utf-8")).decode("utf-8")
    return f"{_SSH_KEY_PREFIX}{token}"


def _decrypt_key_text(value: str) -> str | None:
    if not value or not _is_encrypted(value):
        return None
    token = value[len(_SSH_KEY_PREFIX):]
    for fernet in _fernet_instances():
        try:
            return fernet.decrypt(token.encode("utf-8")).decode("utf-8")
        except InvalidToken:
            continue
    return None


def _rotate_encrypted(value: str) -> str:
    if not value or not _is_encrypted(value):
        return value
    decrypted = _decrypt_key_text(value)
    if decrypted is None:
        return value
    return _encrypt_key_text(decrypted)


def _user_home_dir(user) -> Path:
    home_dir = _user_owner_dir(user) / "home"
    home_dir.mkdir(parents=True, exist_ok=True)
    return home_dir


def _user_app_data_dir_for_owner_root(owner_dir: Path) -> Path:
    owner_name = str(owner_dir.name or "").strip()
    if owner_name == ".alshival":
        data_dir = owner_dir
    else:
        data_dir = owner_dir / "home" / ".alshival"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def _user_app_data_dir(user) -> Path:
    owner_dir = _user_owner_dir(user)
    app_data_dir = _user_app_data_dir_for_owner_root(owner_dir)
    _migrate_legacy_user_root_files(owner_dir=owner_dir, app_data_dir=app_data_dir)
    return app_data_dir


def _migrate_legacy_user_file(*, owner_dir: Path, filename: str, target_path: Path) -> None:
    legacy_path = owner_dir / filename
    if not legacy_path.exists() or target_path.exists():
        return
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(legacy_path), str(target_path))
    except Exception:
        try:
            shutil.copy2(str(legacy_path), str(target_path))
        except Exception:
            return
        try:
            legacy_path.unlink(missing_ok=True)
        except Exception:
            return


def _migrate_legacy_resources_tree(*, owner_dir: Path, app_data_dir: Path) -> None:
    legacy_resources_dir = owner_dir / "resources"
    if not legacy_resources_dir.exists() or not legacy_resources_dir.is_dir():
        return
    target_resources_dir = app_data_dir / "resources"
    target_resources_dir.parent.mkdir(parents=True, exist_ok=True)
    if not target_resources_dir.exists():
        try:
            shutil.move(str(legacy_resources_dir), str(target_resources_dir))
        except Exception:
            return
        return

    for child in list(legacy_resources_dir.iterdir()):
        destination = target_resources_dir / child.name
        if destination.exists():
            continue
        try:
            shutil.move(str(child), str(destination))
        except Exception:
            continue
    try:
        legacy_resources_dir.rmdir()
    except Exception:
        return


def _migrate_legacy_user_root_files(*, owner_dir: Path, app_data_dir: Path) -> None:
    _migrate_legacy_user_file(owner_dir=owner_dir, filename="member.db", target_path=app_data_dir / "member.db")
    _migrate_legacy_user_file(owner_dir=owner_dir, filename="knowledge.db", target_path=app_data_dir / "knowledge.db")
    _migrate_legacy_resources_tree(owner_dir=owner_dir, app_data_dir=app_data_dir)


def _user_db_path(user) -> Path:
    owner_dir = _user_owner_dir(user)
    app_data_dir = _user_app_data_dir_for_owner_root(owner_dir)
    _migrate_legacy_user_root_files(owner_dir=owner_dir, app_data_dir=app_data_dir)
    db_path = app_data_dir / "member.db"
    return db_path


def _user_knowledge_db_path_for_owner_root(owner_dir: Path) -> Path:
    app_data_dir = _user_app_data_dir_for_owner_root(owner_dir)
    owner_name = str(owner_dir.name or "").strip()
    if owner_name != ".alshival":
        _migrate_legacy_user_root_files(owner_dir=owner_dir, app_data_dir=app_data_dir)
    knowledge_path = app_data_dir / "knowledge.db"
    return knowledge_path


def _user_knowledge_db_path(user) -> Path:
    owner_dir = _user_owner_dir(user)
    return _user_knowledge_db_path_for_owner_root(owner_dir)


def _user_data_dir(user) -> Path:
    return _user_app_data_dir(user)


def _base_user_data_root() -> Path:
    root = Path(getattr(settings, 'USER_DATA_ROOT', Path(settings.BASE_DIR) / 'var' / 'user_data'))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _base_team_data_root() -> Path:
    root = Path(getattr(settings, 'TEAM_DATA_ROOT', Path(settings.BASE_DIR) / 'var' / 'team_data'))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _base_global_data_root() -> Path:
    root = Path(getattr(settings, 'GLOBAL_DATA_ROOT', Path(settings.BASE_DIR) / 'var' / 'global_data'))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _user_owner_dir(user) -> Path:
    username = user.get_username() or f"user-{user.pk}"
    safe_username = slugify(username) or f"user-{user.pk}"
    path = _base_user_data_root() / f"{safe_username}-{user.pk}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _team_owner_dir(team: Group) -> Path:
    team_name = str(getattr(team, "name", "") or f"team-{getattr(team, 'pk', 0)}")
    safe_team_name = slugify(team_name) or f"team-{getattr(team, 'pk', 0)}"
    path = _base_team_data_root() / f"{safe_team_name}-{int(team.pk)}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _global_owner_dir() -> Path:
    path = _base_global_data_root()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _normalize_resource_owner_scope(raw_scope: str) -> str:
    normalized = str(raw_scope or "").strip().lower()
    if normalized in {"team", "global"}:
        return normalized
    return "user"


def _resolve_owner_from_scope(*, fallback_user, access_scope: str, team_names: list[str] | None):
    normalized_scope = str(access_scope or "").strip().lower()
    if normalized_scope == "global" and bool(getattr(fallback_user, "is_superuser", False)):
        return {"owner_scope": "global", "owner_user": None, "owner_team": None}
    if normalized_scope == "team":
        for team_name in team_names or []:
            resolved_name = str(team_name or "").strip()
            if not resolved_name:
                continue
            team = Group.objects.filter(name=resolved_name).first()
            if team:
                return {"owner_scope": "team", "owner_user": None, "owner_team": team}
    return {"owner_scope": "user", "owner_user": fallback_user, "owner_team": None}


def _get_package_owner_record(resource_uuid: str):
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return None
    from .models import ResourcePackageOwner

    return (
        ResourcePackageOwner.objects.select_related("owner_user", "owner_team")
        .filter(resource_uuid=resolved_uuid)
        .first()
    )


def _ensure_package_owner_record(
    *,
    resource_uuid: str,
    fallback_user=None,
    access_scope: str = "account",
    team_names: list[str] | None = None,
):
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return None
    from .models import ResourcePackageOwner

    with transaction.atomic():
        row = (
            ResourcePackageOwner.objects.select_for_update()
            .select_related("owner_user", "owner_team")
            .filter(resource_uuid=resolved_uuid)
            .first()
        )
        if row:
            return row
        if fallback_user is None:
            return None
        target_owner = _resolve_owner_from_scope(
            fallback_user=fallback_user,
            access_scope=access_scope,
            team_names=team_names,
        )
        return ResourcePackageOwner.objects.create(
            resource_uuid=resolved_uuid,
            owner_scope=target_owner["owner_scope"],
            owner_user=target_owner["owner_user"],
            owner_team=target_owner["owner_team"],
            created_by=fallback_user,
            updated_by=fallback_user,
        )


def _owner_root_dir_for_row(row, fallback_user=None) -> Path:
    scope = _normalize_resource_owner_scope(getattr(row, "owner_scope", "user"))
    owner_user = getattr(row, "owner_user", None)
    owner_team = getattr(row, "owner_team", None)
    if scope == "team" and owner_team is not None:
        return _team_owner_dir(owner_team)
    if scope == "global":
        return _global_owner_dir()
    if owner_user is not None:
        return _user_app_data_dir(owner_user)
    if fallback_user is not None:
        return _user_app_data_dir(fallback_user)
    raise ValueError("resource package user owner is missing")


def _resource_data_dir(user, resource_uuid: str, *, create: bool = True) -> Path:
    resolved_uuid = str(resource_uuid or "").strip().lower()
    safe_uuid = "".join(ch for ch in resolved_uuid if ch.isalnum() or ch in {"-", "_"})
    if not safe_uuid:
        safe_uuid = "unknown-resource"
    owner_row = _get_package_owner_record(resolved_uuid)
    if owner_row is None and user is not None:
        owner_row = _ensure_package_owner_record(
            resource_uuid=resolved_uuid,
            fallback_user=user,
        )
    if owner_row is not None:
        base_dir = _owner_root_dir_for_row(owner_row, fallback_user=user)
    elif user is not None:
        base_dir = _user_data_dir(user)
    else:
        base_dir = _base_global_data_root()
    path = base_dir / "resources" / safe_uuid
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _resource_db_path(user, resource_uuid: str) -> Path:
    return _resource_data_dir(user, resource_uuid) / "resource.db"


def get_resource_owner_context(user, resource_uuid: str) -> dict[str, Any]:
    resolved_uuid = str(resource_uuid or "").strip().lower()
    owner_row = _get_package_owner_record(resolved_uuid)
    if owner_row is None and user is not None:
        owner_row = _ensure_package_owner_record(
            resource_uuid=resolved_uuid,
            fallback_user=user,
        )
    if owner_row is not None:
        owner_root = _owner_root_dir_for_row(owner_row, fallback_user=user)
        return {
            "owner_scope": _normalize_resource_owner_scope(getattr(owner_row, "owner_scope", "user")),
            "owner_user_id": int(getattr(owner_row, "owner_user_id", 0) or 0),
            "owner_team_id": int(getattr(owner_row, "owner_team_id", 0) or 0),
            "owner_root": owner_root,
            "resource_dir": owner_root / "resources" / (
                "".join(ch for ch in resolved_uuid if ch.isalnum() or ch in {"-", "_"}) or "unknown-resource"
            ),
        }
    fallback_root = _user_data_dir(user) if user is not None else _base_global_data_root()
    safe_uuid = "".join(ch for ch in resolved_uuid if ch.isalnum() or ch in {"-", "_"}) or "unknown-resource"
    return {
        "owner_scope": "user" if user is not None else "global",
        "owner_user_id": int(getattr(user, "id", 0) or 0),
        "owner_team_id": 0,
        "owner_root": fallback_root,
        "resource_dir": fallback_root / "resources" / safe_uuid,
    }


def get_resource_knowledge_db_path(user, resource_uuid: str) -> Path:
    owner_context = get_resource_owner_context(user, resource_uuid)
    owner_root = Path(owner_context["owner_root"])
    owner_scope = str(owner_context.get("owner_scope") or "").strip().lower()
    owner_root.mkdir(parents=True, exist_ok=True)
    if owner_scope == "user":
        return _user_knowledge_db_path_for_owner_root(owner_root)
    return owner_root / "knowledge.db"


def transfer_resource_package(
    *,
    actor,
    resource_uuid: str,
    access_scope: str,
    team_names: list[str] | None = None,
) -> dict[str, str]:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return {"status": "skipped", "reason": "missing_resource_uuid"}
    from .models import ResourcePackageOwner

    target_owner = _resolve_owner_from_scope(
        fallback_user=actor,
        access_scope=access_scope,
        team_names=team_names,
    )
    with transaction.atomic():
        row = (
            ResourcePackageOwner.objects.select_for_update()
            .select_related("owner_user", "owner_team")
            .filter(resource_uuid=resolved_uuid)
            .first()
        )
        if row is None:
            safe_uuid = "".join(ch for ch in resolved_uuid.lower() if ch.isalnum() or ch in {"-", "_"}) or "unknown-resource"
            source_candidates = [
                _user_data_dir(actor) / "resources" / safe_uuid,
                _user_owner_dir(actor) / "resources" / safe_uuid,
            ]
            legacy_source_dir = next((path for path in source_candidates if path.exists()), source_candidates[0])
            if target_owner["owner_scope"] == "team" and target_owner["owner_team"] is not None:
                target_root = _team_owner_dir(target_owner["owner_team"])
            elif target_owner["owner_scope"] == "global":
                target_root = _global_owner_dir()
            elif target_owner["owner_user"] is not None:
                target_root = _user_app_data_dir(target_owner["owner_user"])
            else:
                target_root = _user_app_data_dir(actor)
            destination_dir = target_root / "resources" / safe_uuid
            destination_dir.parent.mkdir(parents=True, exist_ok=True)
            if legacy_source_dir.exists() and legacy_source_dir != destination_dir:
                if destination_dir.exists():
                    shutil.rmtree(destination_dir, ignore_errors=True)
                shutil.move(str(legacy_source_dir), str(destination_dir))
            row = ResourcePackageOwner.objects.create(
                resource_uuid=resolved_uuid,
                owner_scope=target_owner["owner_scope"],
                owner_user=target_owner["owner_user"],
                owner_team=target_owner["owner_team"],
                created_by=actor,
                updated_by=actor,
            )
            return {"status": "created", "owner_scope": row.owner_scope}

        current_scope = _normalize_resource_owner_scope(row.owner_scope)
        current_user_id = int(row.owner_user_id or 0)
        current_team_id = int(row.owner_team_id or 0)
        target_scope = _normalize_resource_owner_scope(target_owner["owner_scope"])
        target_user_id = int(getattr(target_owner["owner_user"], "id", 0) or 0)
        target_team_id = int(getattr(target_owner["owner_team"], "id", 0) or 0)
        if (
            current_scope == target_scope
            and current_user_id == target_user_id
            and current_team_id == target_team_id
        ):
            return {"status": "noop", "owner_scope": current_scope}

        source_dir = _owner_root_dir_for_row(row, fallback_user=actor) / "resources" / (
            "".join(ch for ch in resolved_uuid.lower() if ch.isalnum() or ch in {"-", "_"}) or "unknown-resource"
        )
        if target_scope == "team" and target_owner["owner_team"] is not None:
            target_root = _team_owner_dir(target_owner["owner_team"])
        elif target_scope == "global":
            target_root = _global_owner_dir()
        elif target_owner["owner_user"] is not None:
            target_root = _user_app_data_dir(target_owner["owner_user"])
        else:
            target_root = _user_app_data_dir(actor)
        destination_dir = target_root / "resources" / (
            "".join(ch for ch in resolved_uuid.lower() if ch.isalnum() or ch in {"-", "_"}) or "unknown-resource"
        )
        destination_dir.parent.mkdir(parents=True, exist_ok=True)
        if source_dir.exists():
            if destination_dir.exists():
                shutil.rmtree(destination_dir, ignore_errors=True)
            shutil.move(str(source_dir), str(destination_dir))

        row.owner_scope = target_scope
        row.owner_user = target_owner["owner_user"]
        row.owner_team = target_owner["owner_team"]
        row.updated_by = actor
        row.save(update_fields=["owner_scope", "owner_user", "owner_team", "updated_by", "updated_at"])
        return {"status": "moved", "owner_scope": target_scope}


def _connect(user) -> sqlite3.Connection:
    db_path = _user_db_path(user)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _connect_sqlite(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _team_ssh_db_path(team: Group) -> Path:
    return _team_owner_dir(team) / "team.db"


def _ensure_team_ssh_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ssh_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            encrypted_private_key TEXT NOT NULL,
            created_by_user_id INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_team_ssh_credentials_active
        ON ssh_credentials(is_active, updated_at)
        """
    )
    conn.commit()


def _parse_ssh_credential_ref(raw_value: str | int) -> tuple[str, int, int]:
    raw = str(raw_value or "").strip()
    if not raw:
        return "account", 0, 0
    if raw.startswith("team:"):
        # team:<team_id>:<credential_id>
        parts = raw.split(":")
        if len(parts) == 3:
            try:
                return "team", int(parts[1]), int(parts[2])
            except Exception:
                return "account", 0, 0
    if raw.startswith("account:"):
        try:
            return "account", 0, int(raw.split(":", 1)[1])
        except Exception:
            return "account", 0, 0
    if raw.startswith("local:"):
        try:
            return "account", 0, int(raw.split(":", 1)[1])
        except Exception:
            return "account", 0, 0
    try:
        return "account", 0, int(raw)
    except Exception:
        return "account", 0, 0


def _connect_resource(user, resource_uuid: str) -> sqlite3.Connection:
    db_path = _resource_db_path(user, resource_uuid)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_resource_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            ip_address TEXT,
            user_agent TEXT,
            log_count INTEGER NOT NULL DEFAULT 0,
            payload TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_note_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            content_type TEXT NOT NULL,
            file_size INTEGER NOT NULL DEFAULT 0,
            file_blob BLOB NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            body TEXT NOT NULL,
            author_user_id INTEGER NOT NULL DEFAULT 0,
            author_username TEXT NOT NULL,
            attachment_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL,
            checked_at TEXT NOT NULL,
            target TEXT,
            error TEXT,
            check_method TEXT,
            latency_ms REAL,
            packet_loss_pct REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            key_prefix TEXT NOT NULL,
            key_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_alert_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            health_alerts_app_enabled INTEGER NOT NULL DEFAULT 1,
            health_alerts_sms_enabled INTEGER NOT NULL DEFAULT 0,
            health_alerts_email_enabled INTEGER NOT NULL DEFAULT 0,
            cloud_log_errors_app_enabled INTEGER NOT NULL DEFAULT 1,
            cloud_log_errors_sms_enabled INTEGER NOT NULL DEFAULT 0,
            cloud_log_errors_email_enabled INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_agenda_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            source_item_id TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            due_date TEXT NOT NULL DEFAULT '',
            due_time TEXT NOT NULL DEFAULT '',
            due_at TEXT NOT NULL DEFAULT '',
            item_url TEXT NOT NULL DEFAULT '',
            item_meta TEXT NOT NULL DEFAULT '',
            is_completed INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            assigned_by_user_id INTEGER NOT NULL DEFAULT 0,
            assigned_by_username TEXT NOT NULL DEFAULT '',
            assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(item_id)
        )
        """
    )
    alert_setting_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(resource_alert_settings)").fetchall()
    }
    required_alert_setting_columns = {
        "health_alerts_app_enabled": "INTEGER NOT NULL DEFAULT 1",
        "health_alerts_sms_enabled": "INTEGER NOT NULL DEFAULT 0",
        "health_alerts_email_enabled": "INTEGER NOT NULL DEFAULT 0",
        "cloud_log_errors_app_enabled": "INTEGER NOT NULL DEFAULT 1",
        "cloud_log_errors_sms_enabled": "INTEGER NOT NULL DEFAULT 0",
        "cloud_log_errors_email_enabled": "INTEGER NOT NULL DEFAULT 0",
    }
    for column_name, definition in required_alert_setting_columns.items():
        if column_name in alert_setting_columns:
            continue
        conn.execute(
            f"ALTER TABLE resource_alert_settings ADD COLUMN {column_name} {definition}"
        )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_resource_alert_settings_user_id
        ON resource_alert_settings(user_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_resource_agenda_tasks_due
        ON resource_agenda_tasks(due_date, due_time, is_completed)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_resource_agenda_tasks_source
        ON resource_agenda_tasks(source, source_item_id, updated_at)
        """
    )
    conn.commit()


def _quote_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _resource_logs_table_name(resource_uuid: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(resource_uuid).strip().lower())
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    if not cleaned:
        cleaned = "unknown_resource"
    return f"resource_{cleaned}_logs"


def _create_resource_logs_table(conn: sqlite3.Connection, resource_uuid: str) -> str:
    table_name = _resource_logs_table_name(resource_uuid)
    table_identifier = _quote_identifier(table_name)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_identifier} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            ip_address TEXT,
            user_agent TEXT,
            log_count INTEGER NOT NULL DEFAULT 0,
            payload TEXT NOT NULL
        )
        """
    )
    return table_name


def _list_resource_team_access(conn: sqlite3.Connection, resource_uuid: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT team_name
        FROM resource_team_access
        WHERE resource_uuid = ?
        ORDER BY team_name COLLATE NOCASE ASC
        """,
        (resource_uuid,),
    ).fetchall()
    return [str(row["team_name"]).strip() for row in rows if str(row["team_name"] or "").strip()]


def _replace_resource_team_access(conn: sqlite3.Connection, resource_uuid: str, team_names: list[str] | None) -> None:
    conn.execute("DELETE FROM resource_team_access WHERE resource_uuid = ?", (resource_uuid,))
    for team_name in team_names or []:
        normalized = str(team_name or "").strip()
        if not normalized:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO resource_team_access (resource_uuid, team_name, created_at)
            VALUES (?, ?, datetime('now'))
            """,
            (resource_uuid, normalized),
        )


def _list_ssh_team_access(conn: sqlite3.Connection, credential_id: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT team_name
        FROM ssh_credential_team_access
        WHERE credential_id = ?
        ORDER BY team_name COLLATE NOCASE ASC
        """,
        (credential_id,),
    ).fetchall()
    return [str(row["team_name"]).strip() for row in rows if str(row["team_name"] or "").strip()]


def _replace_ssh_team_access(conn: sqlite3.Connection, credential_id: int, team_names: list[str] | None) -> None:
    conn.execute("DELETE FROM ssh_credential_team_access WHERE credential_id = ?", (credential_id,))
    for team_name in team_names or []:
        normalized = str(team_name or "").strip()
        if not normalized:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO ssh_credential_team_access (credential_id, team_name, created_at)
            VALUES (?, ?, datetime('now'))
            """,
            (int(credential_id), normalized),
        )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_uuid TEXT,
            name TEXT NOT NULL,
            access_scope TEXT NOT NULL DEFAULT 'account',
            team_name TEXT,
            resource_type TEXT NOT NULL,
            target TEXT NOT NULL,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            ssh_key_name TEXT,
            ssh_username TEXT,
            ssh_key_text TEXT,
            ssh_port TEXT,
            address TEXT,
            port TEXT,
            db_type TEXT,
            healthcheck_url TEXT,
            resource_subtype TEXT,
            resource_metadata TEXT,
            ssh_credential_id TEXT,
            ssh_credential_scope TEXT,
            last_status TEXT,
            last_checked_at TEXT,
            last_error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ssh_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'account',
            team_name TEXT,
            encrypted_private_key TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            checked_at TEXT NOT NULL,
            target TEXT,
            error TEXT,
            check_method TEXT,
            latency_ms REAL,
            packet_loss_pct REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_team_access (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_uuid TEXT NOT NULL,
            team_name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(resource_uuid, team_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ssh_credential_team_access (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            credential_id INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(credential_id, team_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_type TEXT NOT NULL,
            name TEXT NOT NULL,
            resource_uuid TEXT,
            key_prefix TEXT NOT NULL,
            key_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_note_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_uuid TEXT NOT NULL,
            file_name TEXT NOT NULL,
            content_type TEXT NOT NULL,
            file_size INTEGER NOT NULL DEFAULT 0,
            file_blob BLOB NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_uuid TEXT NOT NULL,
            body TEXT NOT NULL,
            author_user_id INTEGER NOT NULL DEFAULT 0,
            author_username TEXT NOT NULL,
            attachment_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ask_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT NOT NULL DEFAULT 'default',
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            resource_uuid TEXT,
            level TEXT NOT NULL DEFAULT 'info',
            channel TEXT NOT NULL DEFAULT 'app',
            metadata TEXT NOT NULL DEFAULT '{}',
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_filter_prompt (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            prompt TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO alert_filter_prompt (id, prompt, updated_at)
        VALUES (1, '', datetime('now'))
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asana_task_cache (
            cache_key TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL DEFAULT '{}',
            fetched_at_epoch INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asana_board_resource_map (
            board_gid TEXT NOT NULL,
            resource_uuid TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (board_gid, resource_uuid)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asana_task_resource_map (
            task_gid TEXT NOT NULL,
            resource_uuid TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (task_gid, resource_uuid)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agenda_item_resource_map (
            item_id TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            source_item_id TEXT NOT NULL DEFAULT '',
            resource_uuid TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            due_date TEXT NOT NULL DEFAULT '',
            due_time TEXT NOT NULL DEFAULT '',
            due_at TEXT NOT NULL DEFAULT '',
            item_url TEXT NOT NULL DEFAULT '',
            item_meta TEXT NOT NULL DEFAULT '',
            is_completed INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (item_id, resource_uuid)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_event_cache (
            provider TEXT NOT NULL,
            event_id TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            due_date TEXT NOT NULL DEFAULT '',
            due_time TEXT NOT NULL DEFAULT '',
            is_completed INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'open',
            source_url TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (provider, event_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_sync_state (
            provider TEXT PRIMARY KEY,
            fetched_at_epoch INTEGER NOT NULL DEFAULT 0,
            item_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'idle',
            message TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS outlook_mail_cache (
            message_id TEXT PRIMARY KEY,
            folder TEXT NOT NULL DEFAULT 'inbox',
            internet_message_id TEXT NOT NULL DEFAULT '',
            conversation_id TEXT NOT NULL DEFAULT '',
            subject TEXT NOT NULL DEFAULT '',
            sender_email TEXT NOT NULL DEFAULT '',
            sender_name TEXT NOT NULL DEFAULT '',
            to_recipients_json TEXT NOT NULL DEFAULT '[]',
            cc_recipients_json TEXT NOT NULL DEFAULT '[]',
            received_at TEXT NOT NULL DEFAULT '',
            sent_at TEXT NOT NULL DEFAULT '',
            body_preview TEXT NOT NULL DEFAULT '',
            body_text TEXT NOT NULL DEFAULT '',
            web_link TEXT NOT NULL DEFAULT '',
            is_read INTEGER NOT NULL DEFAULT 0,
            has_attachments INTEGER NOT NULL DEFAULT 0,
            raw_payload_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            message TEXT,
            recipients_json TEXT NOT NULL DEFAULT '[]',
            remind_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled',
            action TEXT NOT NULL DEFAULT 'notify_user',
            channels_json TEXT NOT NULL DEFAULT '{}',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            last_error TEXT NOT NULL DEFAULT '',
            created_by_user_id INTEGER,
            created_by_username TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            sent_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    reminder_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(reminders)").fetchall()
    }
    if "recipients_json" not in reminder_columns:
        conn.execute("ALTER TABLE reminders ADD COLUMN recipients_json TEXT NOT NULL DEFAULT '[]'")
        if "recipients" in reminder_columns:
            conn.execute(
                """
                UPDATE reminders
                SET recipients_json = COALESCE(NULLIF(trim(recipients_json), ''), COALESCE(recipients, '[]'))
                """
            )
    if "channels_json" not in reminder_columns:
        conn.execute("ALTER TABLE reminders ADD COLUMN channels_json TEXT NOT NULL DEFAULT '{}'")
        if "channels" in reminder_columns:
            conn.execute(
                """
                UPDATE reminders
                SET channels_json = COALESCE(NULLIF(trim(channels_json), ''), COALESCE(channels, '{}'))
                """
            )
    if "metadata_json" not in reminder_columns:
        conn.execute("ALTER TABLE reminders ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'")
        if "metadata" in reminder_columns:
            conn.execute(
                """
                UPDATE reminders
                SET metadata_json = COALESCE(NULLIF(trim(metadata_json), ''), COALESCE(metadata, '{}'))
                """
            )
    if "last_error" not in reminder_columns:
        conn.execute("ALTER TABLE reminders ADD COLUMN last_error TEXT NOT NULL DEFAULT ''")
    if "created_by_user_id" not in reminder_columns:
        conn.execute("ALTER TABLE reminders ADD COLUMN created_by_user_id INTEGER")
    if "created_by_username" not in reminder_columns:
        conn.execute("ALTER TABLE reminders ADD COLUMN created_by_username TEXT NOT NULL DEFAULT ''")
    if "sent_at" not in reminder_columns:
        conn.execute("ALTER TABLE reminders ADD COLUMN sent_at TEXT NOT NULL DEFAULT ''")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_notification_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            calendar_events_app_enabled INTEGER NOT NULL DEFAULT 1,
            calendar_events_sms_enabled INTEGER NOT NULL DEFAULT 0,
            calendar_events_email_enabled INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_notifications_read_created
        ON notifications(is_read, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_notifications_resource_created
        ON notifications(resource_uuid, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ask_chat_conv_created
        ON ask_chat_messages(conversation_id, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_calendar_event_cache_due
        ON calendar_event_cache(provider, due_date, due_time)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_calendar_event_cache_done
        ON calendar_event_cache(provider, is_completed, due_date)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_outlook_mail_cache_received
        ON outlook_mail_cache(received_at DESC, updated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_outlook_mail_cache_sender_subject
        ON outlook_mail_cache(sender_email, subject)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_user_reminders_status_time
        ON reminders(status, remind_at, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_user_reminders_remind_at
        ON reminders(remind_at, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_user_reminders_created_by
        ON reminders(created_by_user_id, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_asana_board_resource_map_board
        ON asana_board_resource_map(board_gid, resource_uuid)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_asana_task_resource_map_task
        ON asana_task_resource_map(task_gid, resource_uuid)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agenda_item_resource_map_item
        ON agenda_item_resource_map(item_id, source, resource_uuid)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agenda_item_resource_map_resource
        ON agenda_item_resource_map(resource_uuid, due_date, due_time)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_calendar_notification_settings_user
        ON calendar_notification_settings(user_id)
        """
    )
    _ensure_ask_chat_schema(conn)
    existing_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(resources)").fetchall()
    }
    if 'resource_uuid' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN resource_uuid TEXT")
    if 'access_scope' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN access_scope TEXT")
        conn.execute("UPDATE resources SET access_scope = 'account' WHERE access_scope IS NULL OR trim(access_scope) = ''")
    if 'team_name' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN team_name TEXT")
    uuid_rows = conn.execute(
        "SELECT id FROM resources WHERE resource_uuid IS NULL OR trim(resource_uuid) = ''"
    ).fetchall()
    for row in uuid_rows:
        conn.execute(
            "UPDATE resources SET resource_uuid = ? WHERE id = ?",
            (str(uuid.uuid4()), row["id"] if isinstance(row, sqlite3.Row) else row[0]),
        )
    if 'ssh_key_name' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN ssh_key_name TEXT")
    if 'ssh_username' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN ssh_username TEXT")
    if 'ssh_key_text' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN ssh_key_text TEXT")
    if 'ssh_port' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN ssh_port TEXT")
    if 'address' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN address TEXT")
    if 'port' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN port TEXT")
    if 'db_type' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN db_type TEXT")
    if 'healthcheck_url' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN healthcheck_url TEXT")
    if 'resource_subtype' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN resource_subtype TEXT")
    if 'resource_metadata' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN resource_metadata TEXT")
    if 'ssh_credential_id' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN ssh_credential_id TEXT")
    if 'ssh_credential_scope' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN ssh_credential_scope TEXT")
    if 'last_status' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN last_status TEXT")
    if 'last_checked_at' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN last_checked_at TEXT")
    if 'last_error' not in existing_columns:
        conn.execute("ALTER TABLE resources ADD COLUMN last_error TEXT")
    conn.commit()

    check_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(resource_checks)").fetchall()
    }
    if 'check_method' not in check_columns:
        conn.execute("ALTER TABLE resource_checks ADD COLUMN check_method TEXT")
    if 'latency_ms' not in check_columns:
        conn.execute("ALTER TABLE resource_checks ADD COLUMN latency_ms REAL")
    if 'packet_loss_pct' not in check_columns:
        conn.execute("ALTER TABLE resource_checks ADD COLUMN packet_loss_pct REAL")
    conn.commit()

    key_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(ssh_credentials)").fetchall()
    }

    # Legacy key records stored ssh_username/ssh_port. Migrate to a key-only table.
    if 'ssh_username' in key_columns or 'ssh_port' in key_columns:
        conn.execute("DROP TABLE IF EXISTS ssh_credentials_v2")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ssh_credentials_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                scope TEXT NOT NULL DEFAULT 'account',
                team_name TEXT,
                encrypted_private_key TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            INSERT INTO ssh_credentials_v2 (
                id, name, scope, team_name, encrypted_private_key, created_at, updated_at, is_active
            )
            SELECT
                id,
                name,
                COALESCE(scope, 'account'),
                COALESCE(team_name, ''),
                encrypted_private_key,
                COALESCE(created_at, datetime('now')),
                COALESCE(updated_at, COALESCE(created_at, datetime('now'))),
                COALESCE(is_active, 1)
            FROM ssh_credentials
            """
        )
        conn.execute("DROP TABLE ssh_credentials")
        conn.execute("ALTER TABLE ssh_credentials_v2 RENAME TO ssh_credentials")
        conn.commit()
        key_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(ssh_credentials)").fetchall()
        }

    if 'scope' not in key_columns:
        conn.execute("ALTER TABLE ssh_credentials ADD COLUMN scope TEXT")
        conn.execute("UPDATE ssh_credentials SET scope = 'account' WHERE scope IS NULL OR scope = ''")
    if 'team_name' not in key_columns:
        conn.execute("ALTER TABLE ssh_credentials ADD COLUMN team_name TEXT")
    if 'updated_at' not in key_columns:
        conn.execute("ALTER TABLE ssh_credentials ADD COLUMN updated_at TEXT")
        conn.execute("UPDATE ssh_credentials SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = ''")
    if 'is_active' not in key_columns:
        conn.execute("ALTER TABLE ssh_credentials ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    conn.commit()

    api_key_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(api_keys)").fetchall()
    }
    if 'updated_at' not in api_key_columns:
        conn.execute("ALTER TABLE api_keys ADD COLUMN updated_at TEXT")
        conn.execute("UPDATE api_keys SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = ''")
    if 'is_active' not in api_key_columns:
        conn.execute("ALTER TABLE api_keys ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if 'resource_uuid' not in api_key_columns:
        conn.execute("ALTER TABLE api_keys ADD COLUMN resource_uuid TEXT")
    conn.commit()


def _display_target(
    resource_type: str,
    target: str,
    address: str,
    port: str,
    healthcheck_url: str,
) -> str:
    if resource_type == 'api' and healthcheck_url:
        return healthcheck_url
    if resource_type == 'vm' and address:
        return address
    if resource_type == 'database' and address:
        return f"{address}:{port}" if port else address
    if healthcheck_url:
        return healthcheck_url
    if address and port:
        return f"{address}:{port}"
    if address:
        return address
    return target


def _load_resource_metadata(value: str) -> dict[str, Any]:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except (TypeError, ValueError):
        return {}
    if isinstance(loaded, dict):
        return loaded
    return {}


def list_resources(user) -> List[ResourceItem]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, name, resource_type, target, notes, created_at,
                   COALESCE(resource_uuid, '') as resource_uuid,
                   COALESCE(access_scope, 'account') as access_scope,
                   COALESCE(team_name, '') as team_name,
                   COALESCE(ssh_key_name, '') as ssh_key_name,
                   COALESCE(ssh_username, '') as ssh_username,
                   COALESCE(ssh_key_text, '') as ssh_key_text,
                   COALESCE(ssh_port, '') as ssh_port,
                   COALESCE(address, '') as address,
                   COALESCE(port, '') as port,
                   COALESCE(db_type, '') as db_type,
                   COALESCE(healthcheck_url, '') as healthcheck_url,
                   COALESCE(resource_subtype, '') as resource_subtype,
                   COALESCE(resource_metadata, '') as resource_metadata,
                   COALESCE(ssh_credential_id, '') as ssh_credential_id,
                   COALESCE(ssh_credential_scope, '') as ssh_credential_scope,
                   COALESCE(last_status, '') as last_status,
                   COALESCE(last_checked_at, '') as last_checked_at,
                   COALESCE(last_error, '') as last_error
            FROM resources
            ORDER BY id DESC
            """
        ).fetchall()
        updated = False
        for row in rows:
            ssh_key_text = row['ssh_key_text'] or ''
            if ssh_key_text and not _is_encrypted(ssh_key_text):
                conn.execute(
                    "UPDATE resources SET ssh_key_text = ? WHERE id = ?",
                    (_encrypt_key_text(ssh_key_text), row['id']),
                )
                updated = True
        if updated:
            conn.commit()
        return [
            ResourceItem(
                id=row['id'],
                resource_uuid=row['resource_uuid'],
                name=row['name'],
                access_scope=row['access_scope'] or 'account',
                team_names=_list_resource_team_access(conn, row['resource_uuid']),
                resource_type=row['resource_type'],
                target=_display_target(
                    row['resource_type'],
                    row['target'],
                    row['address'] or (row['target'] if row['resource_type'] == 'vm' else row['address']),
                    row['port'],
                    row['healthcheck_url'],
                ),
                address=row['address'] or (row['target'] if row['resource_type'] == 'vm' else row['address']),
                port=row['port'],
                db_type=row['db_type'],
                healthcheck_url=row['healthcheck_url'],
                notes=row['notes'] or '',
                created_at=row['created_at'],
                last_status=row['last_status'],
                last_checked_at=row['last_checked_at'],
                last_error=row['last_error'],
                ssh_key_name=row['ssh_key_name'],
                ssh_username=row['ssh_username'],
                ssh_key_text=row['ssh_key_text'],
                ssh_key_present=bool(row['ssh_key_text'] or row['ssh_credential_id']),
                ssh_port=row['ssh_port'],
                resource_subtype=row['resource_subtype'],
                resource_metadata=_load_resource_metadata(row['resource_metadata']),
                ssh_credential_id=row['ssh_credential_id'],
                ssh_credential_scope=row['ssh_credential_scope'],
            )
            for row in rows
        ]
    finally:
        conn.close()


def add_resource(
    user,
    name: str,
    resource_type: str,
    target: str,
    notes: str,
    address: str = '',
    port: str = '',
    db_type: str = '',
    healthcheck_url: str = '',
    ssh_key_name: str = '',
    ssh_username: str = '',
    ssh_key_text: str = '',
    ssh_port: str = '',
    resource_subtype: str = '',
    resource_metadata: dict[str, Any] | None = None,
    ssh_credential_id: str = '',
    ssh_credential_scope: str = '',
    access_scope: str = 'account',
    team_names: list[str] | None = None,
) -> int:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        resolved_key_text = _encrypt_key_text(ssh_key_text) if ssh_key_text else ''
        metadata_payload = json.dumps(resource_metadata or {}, separators=(",", ":"))
        new_uuid = str(uuid.uuid4())
        cursor = conn.execute(
            """
            INSERT INTO resources (
                resource_uuid, name, access_scope, team_name, resource_type, target, address, port, db_type, healthcheck_url,
                notes, ssh_key_name, ssh_username, ssh_key_text, ssh_port,
                resource_subtype, resource_metadata, ssh_credential_id, ssh_credential_scope
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_uuid,
                name,
                access_scope,
                "",
                resource_type,
                target,
                address,
                port,
                db_type,
                healthcheck_url,
                notes,
                ssh_key_name,
                ssh_username,
                resolved_key_text,
                ssh_port,
                resource_subtype,
                metadata_payload,
                ssh_credential_id,
                ssh_credential_scope,
            ),
        )
        _replace_resource_team_access(conn, new_uuid, team_names if access_scope == "team" else [])
        conn.commit()
        _ensure_package_owner_record(
            resource_uuid=new_uuid,
            fallback_user=user,
            access_scope=access_scope,
            team_names=team_names,
        )
        resource_conn = _connect_resource(user, new_uuid)
        try:
            _ensure_resource_schema(resource_conn)
        finally:
            resource_conn.close()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def update_resource(
    user,
    resource_id: int,
    name: str,
    resource_type: str,
    target: str,
    notes: str,
    address: str = '',
    port: str = '',
    db_type: str = '',
    healthcheck_url: str = '',
    ssh_key_name: str = '',
    ssh_username: str = '',
    ssh_key_text: str | None = None,
    clear_ssh_key: bool = False,
    ssh_port: str = '',
    resource_subtype: str = '',
    resource_metadata: dict[str, Any] | None = None,
    ssh_credential_id: str = '',
    ssh_credential_scope: str = '',
    access_scope: str = 'account',
    team_names: list[str] | None = None,
) -> None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        existing = conn.execute(
            "SELECT ssh_key_text FROM resources WHERE id = ?",
            (resource_id,),
        ).fetchone()
        existing_key_text = existing['ssh_key_text'] if existing else ''
        if clear_ssh_key:
            resolved_key_text = ''
        elif ssh_key_text is None or ssh_key_text == '':
            if existing_key_text and not _is_encrypted(existing_key_text):
                resolved_key_text = _encrypt_key_text(existing_key_text)
            else:
                resolved_key_text = _rotate_encrypted(existing_key_text)
        else:
            resolved_key_text = _encrypt_key_text(ssh_key_text)
        metadata_payload = json.dumps(resource_metadata or {}, separators=(",", ":"))
        conn.execute(
            """
            UPDATE resources
            SET name = ?, access_scope = ?, team_name = ?, resource_type = ?, target = ?, address = ?, port = ?, db_type = ?, healthcheck_url = ?,
                notes = ?, ssh_key_name = ?, ssh_username = ?, ssh_key_text = ?, ssh_port = ?,
                resource_subtype = ?, resource_metadata = ?, ssh_credential_id = ?, ssh_credential_scope = ?
            WHERE id = ?
            """,
            (
                name,
                access_scope,
                "",
                resource_type,
                target,
                address,
                port,
                db_type,
                healthcheck_url,
                notes,
                ssh_key_name,
                ssh_username,
                resolved_key_text,
                ssh_port,
                resource_subtype,
                metadata_payload,
                ssh_credential_id,
                ssh_credential_scope,
                resource_id,
            ),
        )
        resource_row = conn.execute(
            "SELECT resource_uuid FROM resources WHERE id = ?",
            (resource_id,),
        ).fetchone()
        resource_uuid = str(resource_row["resource_uuid"] or "").strip() if resource_row else ""
        if resource_uuid:
            _replace_resource_team_access(conn, resource_uuid, team_names if access_scope == "team" else [])
        conn.commit()
        if resource_uuid:
            transfer_resource_package(
                actor=user,
                resource_uuid=resource_uuid,
                access_scope=access_scope,
                team_names=team_names,
            )
    finally:
        conn.close()


def delete_resource(user, resource_id: int) -> None:
    row = None
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT resource_uuid FROM resources WHERE id = ?",
            (resource_id,),
        ).fetchone()
        if row and str(row["resource_uuid"] or "").strip():
            resolved_uuid = str(row["resource_uuid"]).strip()
            conn.execute(
                "DELETE FROM resource_team_access WHERE resource_uuid = ?",
                (resolved_uuid,),
            )
            conn.execute(
                "DELETE FROM api_keys WHERE key_type = 'resource' AND COALESCE(resource_uuid, '') = ?",
                (resolved_uuid,),
            )
        conn.execute("DELETE FROM resources WHERE id = ?", (resource_id,))
        conn.commit()
    finally:
        conn.close()
    if row and str(row["resource_uuid"] or "").strip():
        resolved_uuid = str(row["resource_uuid"]).strip()
        resource_dir = _resource_data_dir(user, resolved_uuid, create=False)
        shutil.rmtree(resource_dir, ignore_errors=True)
        from .models import ResourcePackageOwner

        ResourcePackageOwner.objects.filter(resource_uuid=resolved_uuid).delete()


def get_resource(user, resource_id: int) -> ResourceItem | None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT id, name, resource_type, target, notes, created_at,
                   COALESCE(resource_uuid, '') as resource_uuid,
                   COALESCE(access_scope, 'account') as access_scope,
                   COALESCE(team_name, '') as team_name,
                   COALESCE(ssh_key_name, '') as ssh_key_name,
                   COALESCE(ssh_username, '') as ssh_username,
                   COALESCE(ssh_key_text, '') as ssh_key_text,
                   COALESCE(ssh_port, '') as ssh_port,
                   COALESCE(address, '') as address,
                   COALESCE(port, '') as port,
                   COALESCE(db_type, '') as db_type,
                   COALESCE(healthcheck_url, '') as healthcheck_url,
                   COALESCE(resource_subtype, '') as resource_subtype,
                   COALESCE(resource_metadata, '') as resource_metadata,
                   COALESCE(ssh_credential_id, '') as ssh_credential_id,
                   COALESCE(ssh_credential_scope, '') as ssh_credential_scope,
                   COALESCE(last_status, '') as last_status,
                   COALESCE(last_checked_at, '') as last_checked_at,
                   COALESCE(last_error, '') as last_error
            FROM resources
            WHERE id = ?
            """,
            (resource_id,),
        ).fetchone()
        if not row:
            return None
        return ResourceItem(
            id=row['id'],
            resource_uuid=row['resource_uuid'],
            name=row['name'],
            access_scope=row['access_scope'] or 'account',
            team_names=_list_resource_team_access(conn, row['resource_uuid']),
            resource_type=row['resource_type'],
            target=_display_target(
                row['resource_type'],
                row['target'],
                row['address'] or (row['target'] if row['resource_type'] == 'vm' else row['address']),
                row['port'],
                row['healthcheck_url'],
            ),
            address=row['address'] or (row['target'] if row['resource_type'] == 'vm' else row['address']),
            port=row['port'],
            db_type=row['db_type'],
            healthcheck_url=row['healthcheck_url'],
            notes=row['notes'] or '',
            created_at=row['created_at'],
            last_status=row['last_status'],
            last_checked_at=row['last_checked_at'],
            last_error=row['last_error'],
            ssh_key_name=row['ssh_key_name'],
            ssh_username=row['ssh_username'],
            ssh_key_text=row['ssh_key_text'],
            ssh_key_present=bool(row['ssh_key_text'] or row['ssh_credential_id']),
            ssh_port=row['ssh_port'],
            resource_subtype=row['resource_subtype'],
            resource_metadata=_load_resource_metadata(row['resource_metadata']),
            ssh_credential_id=row['ssh_credential_id'],
            ssh_credential_scope=row['ssh_credential_scope'],
        )
    finally:
        conn.close()


def get_resource_by_uuid(user, resource_uuid: str) -> ResourceItem | None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT id, name, resource_type, target, notes, created_at,
                   COALESCE(resource_uuid, '') as resource_uuid,
                   COALESCE(access_scope, 'account') as access_scope,
                   COALESCE(team_name, '') as team_name,
                   COALESCE(ssh_key_name, '') as ssh_key_name,
                   COALESCE(ssh_username, '') as ssh_username,
                   COALESCE(ssh_key_text, '') as ssh_key_text,
                   COALESCE(ssh_port, '') as ssh_port,
                   COALESCE(address, '') as address,
                   COALESCE(port, '') as port,
                   COALESCE(db_type, '') as db_type,
                   COALESCE(healthcheck_url, '') as healthcheck_url,
                   COALESCE(resource_subtype, '') as resource_subtype,
                   COALESCE(resource_metadata, '') as resource_metadata,
                   COALESCE(ssh_credential_id, '') as ssh_credential_id,
                   COALESCE(ssh_credential_scope, '') as ssh_credential_scope,
                   COALESCE(last_status, '') as last_status,
                   COALESCE(last_checked_at, '') as last_checked_at,
                   COALESCE(last_error, '') as last_error
            FROM resources
            WHERE resource_uuid = ?
            """,
            (resource_uuid,),
        ).fetchone()
        if not row:
            return None
        return ResourceItem(
            id=row['id'],
            resource_uuid=row['resource_uuid'],
            name=row['name'],
            access_scope=row['access_scope'] or 'account',
            team_names=_list_resource_team_access(conn, row['resource_uuid']),
            resource_type=row['resource_type'],
            target=_display_target(
                row['resource_type'],
                row['target'],
                row['address'] or (row['target'] if row['resource_type'] == 'vm' else row['address']),
                row['port'],
                row['healthcheck_url'],
            ),
            address=row['address'] or (row['target'] if row['resource_type'] == 'vm' else row['address']),
            port=row['port'],
            db_type=row['db_type'],
            healthcheck_url=row['healthcheck_url'],
            notes=row['notes'] or '',
            created_at=row['created_at'],
            last_status=row['last_status'],
            last_checked_at=row['last_checked_at'],
            last_error=row['last_error'],
            ssh_key_name=row['ssh_key_name'],
            ssh_username=row['ssh_username'],
            ssh_key_text=row['ssh_key_text'],
            ssh_key_present=bool(row['ssh_key_text'] or row['ssh_credential_id']),
            ssh_port=row['ssh_port'],
            resource_subtype=row['resource_subtype'],
            resource_metadata=_load_resource_metadata(row['resource_metadata']),
            ssh_credential_id=row['ssh_credential_id'],
            ssh_credential_scope=row['ssh_credential_scope'],
        )
    finally:
        conn.close()


def update_resource_health(user, resource_id: int, status: str, checked_at: str, error: str = '') -> None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            """
            UPDATE resources
            SET last_status = ?, last_checked_at = ?, last_error = ?
            WHERE id = ?
            """,
            (status, checked_at, error, resource_id),
        )
        conn.commit()
    finally:
        conn.close()


def log_resource_check(
    user,
    resource_id: int,
    status: str,
    checked_at: str,
    target: str,
    error: str = '',
    resource_uuid: str = '',
    check_method: str = '',
    latency_ms: float | None = None,
    packet_loss_pct: float | None = None,
) -> None:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        resolved_resource = get_resource(user, int(resource_id))
        resolved_uuid = str(resolved_resource.resource_uuid).strip() if resolved_resource else ""
    if not resolved_uuid:
        return
    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        conn.execute(
            """
            INSERT INTO resource_checks (
                status, checked_at, target, error, check_method, latency_ms, packet_loss_pct
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                status,
                checked_at,
                target,
                error,
                check_method,
                latency_ms,
                packet_loss_pct,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_resource_checks(user, resource_uuid: str, limit: int = 50) -> list[ResourceCheckItem]:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return []
    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        rows = conn.execute(
            """
            SELECT
                id,
                status,
                checked_at,
                COALESCE(target, '') AS target,
                COALESCE(error, '') AS error,
                COALESCE(check_method, '') AS check_method,
                latency_ms,
                packet_loss_pct
            FROM resource_checks
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(max(1, limit)),),
        ).fetchall()
        return [
            ResourceCheckItem(
                id=int(row["id"]),
                resource_id=0,
                status=str(row["status"] or ""),
                checked_at=str(row["checked_at"] or ""),
                target=str(row["target"] or ""),
                error=str(row["error"] or ""),
                check_method=str(row["check_method"] or ""),
                latency_ms=float(row["latency_ms"]) if row["latency_ms"] is not None else None,
                packet_loss_pct=float(row["packet_loss_pct"]) if row["packet_loss_pct"] is not None else None,
            )
            for row in rows
        ]
    finally:
        conn.close()


def store_resource_logs(
    user,
    resource_uuid: str,
    payload: dict[str, Any],
    ip_address: str | None,
    user_agent: str | None,
) -> str:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return ""
    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        logs = payload.get("logs")
        if isinstance(logs, list):
            log_count = len(logs)
        elif logs is None:
            log_count = 0
        else:
            log_count = 1
        conn.execute(
            """
            INSERT INTO resource_logs (
                received_at,
                ip_address,
                user_agent,
                log_count,
                payload
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                payload.get("received_at") or "",
                ip_address,
                user_agent,
                int(log_count),
                json.dumps(payload),
            ),
        )
        conn.commit()
        return "resource_logs"
    finally:
        conn.close()


def list_resource_logs(user, resource_uuid: str, limit: int = 200) -> list[dict[str, Any]]:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return []
    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        rows = conn.execute(
            """
            SELECT id, received_at, payload
            FROM resource_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(max(1, limit)),),
        ).fetchall()
    finally:
        conn.close()

    parsed_logs: list[dict[str, Any]] = []
    for row in rows:
        row_payload_raw = row["payload"] or "{}"
        try:
            row_payload = json.loads(row_payload_raw)
        except (TypeError, ValueError):
            row_payload = {}
        if not isinstance(row_payload, dict):
            row_payload = {}

        envelope_received_at = str(row["received_at"] or "").strip()
        logs = row_payload.get("logs")
        if not isinstance(logs, list):
            logs = [row_payload]

        for item in logs:
            if isinstance(item, dict):
                level = str(item.get("level") or "info").strip().lower() or "info"
                message = str(item.get("message") or "").strip()
                logger = str(item.get("logger") or "").strip() or "alshival"
                timestamp = str(item.get("ts") or envelope_received_at).strip() or envelope_received_at
                raw_metadata = item.get("extra")
                if not isinstance(raw_metadata, dict):
                    raw_metadata = {}
            else:
                level = "info"
                message = str(item or "").strip()
                logger = "alshival"
                timestamp = envelope_received_at
                raw_metadata = {}

            envelope_meta = {
                "sdk": row_payload.get("sdk"),
                "sdk_version": row_payload.get("sdk_version"),
                "submitted_by_username": row_payload.get("submitted_by_username"),
                "received_at": row_payload.get("received_at"),
            }
            merged_metadata = {k: v for k, v in envelope_meta.items() if v not in (None, "", [], {})}
            merged_metadata.update(raw_metadata)

            parsed_logs.append(
                {
                    "level": level,
                    "message": message,
                    "logger": logger,
                    "timestamp": timestamp,
                    "metadata": merged_metadata,
                }
            )

    return parsed_logs


def add_ask_chat_message(user, *, conversation_id: str = "default", role: str, content: str) -> int:
    resolved_role = str(role or "").strip().lower()
    if resolved_role not in {"user", "assistant", "system", "tool"}:
        raise ValueError("role must be one of: user, assistant, system, tool")
    resolved_content = str(content or "").strip()
    if not resolved_content:
        raise ValueError("content is required")
    resolved_conversation_id = str(conversation_id or "").strip() or "default"

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO ask_chat_messages (conversation_id, role, content)
            VALUES (?, ?, ?)
            """,
            (resolved_conversation_id, resolved_role, resolved_content),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def list_ask_chat_messages(user, *, conversation_id: str = "default", limit: int = 20) -> list[dict[str, str]]:
    resolved_conversation_id = str(conversation_id or "").strip() or "default"
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT role, content, created_at
            FROM ask_chat_messages
            WHERE conversation_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (resolved_conversation_id, int(max(1, limit))),
        ).fetchall()
    finally:
        conn.close()

    ordered = list(reversed(rows))
    return [
        {
            "role": str(row["role"] or "").strip(),
            "content": str(row["content"] or "").strip(),
            "created_at": str(row["created_at"] or "").strip(),
        }
        for row in ordered
        if str(row["role"] or "").strip() in {"user", "assistant", "system", "tool"} and str(row["content"] or "").strip()
    ]


def clear_ask_chat_messages(user, *, conversation_id: str = "default") -> int:
    resolved_conversation_id = str(conversation_id or "").strip() or "default"
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            DELETE FROM ask_chat_messages
            WHERE conversation_id = ?
            """,
            (resolved_conversation_id,),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def _ensure_ask_chat_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ask_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT NOT NULL DEFAULT 'default',
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ask_chat_conv_created
        ON ask_chat_messages(conversation_id, id DESC)
        """
    )
    columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(ask_chat_messages)").fetchall()
    }
    if "message_kind" not in columns:
        conn.execute("ALTER TABLE ask_chat_messages ADD COLUMN message_kind TEXT NOT NULL DEFAULT 'chat'")
    if "tool_name" not in columns:
        conn.execute("ALTER TABLE ask_chat_messages ADD COLUMN tool_name TEXT")
    if "tool_call_id" not in columns:
        conn.execute("ALTER TABLE ask_chat_messages ADD COLUMN tool_call_id TEXT")
    if "tool_args_json" not in columns:
        conn.execute("ALTER TABLE ask_chat_messages ADD COLUMN tool_args_json TEXT")
    if "tool_result_json" not in columns:
        conn.execute("ALTER TABLE ask_chat_messages ADD COLUMN tool_result_json TEXT")
    conn.commit()


def add_ask_chat_tool_event(
    user,
    *,
    conversation_id: str = "default",
    kind: str,
    tool_name: str,
    tool_call_id: str = "",
    tool_args_json: str = "",
    tool_result_json: str = "",
    content: str = "",
) -> int:
    resolved_kind = str(kind or "").strip().lower()
    if resolved_kind not in {"tool_call", "tool_result"}:
        raise ValueError("kind must be one of: tool_call, tool_result")
    resolved_tool_name = str(tool_name or "").strip()
    if not resolved_tool_name:
        raise ValueError("tool_name is required")
    resolved_content = str(content or "").strip()
    if not resolved_content:
        resolved_content = f"[{resolved_kind}] {resolved_tool_name}"
    resolved_conversation_id = str(conversation_id or "").strip() or "default"

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        _ensure_ask_chat_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO ask_chat_messages (
                conversation_id, role, content, message_kind, tool_name, tool_call_id, tool_args_json, tool_result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_conversation_id,
                "tool",
                resolved_content,
                resolved_kind,
                resolved_tool_name,
                str(tool_call_id or "").strip(),
                str(tool_args_json or "").strip(),
                str(tool_result_json or "").strip(),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def add_ask_chat_context_event(
    user,
    *,
    event_type: str,
    summary: str,
    payload: dict[str, Any] | None = None,
    conversation_id: str = "default",
) -> int:
    resolved_event_type = str(event_type or "").strip().lower()
    if not resolved_event_type:
        raise ValueError("event_type is required")
    resolved_summary = str(summary or "").strip()
    if not resolved_summary:
        raise ValueError("summary is required")
    resolved_conversation_id = str(conversation_id or "").strip() or "default"
    payload_json = _json_dumps_compact(payload if isinstance(payload, dict) else {})

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        _ensure_ask_chat_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO ask_chat_messages (
                conversation_id,
                role,
                content,
                message_kind,
                tool_name,
                tool_call_id,
                tool_args_json,
                tool_result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_conversation_id,
                "tool",
                resolved_summary[:5000],
                "context_event",
                resolved_event_type[:120],
                "",
                "",
                payload_json,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid or 0)
    finally:
        conn.close()


def _team_chat_db_path(team: Group) -> Path:
    return _team_owner_dir(team) / "team_chat.db"


def _ensure_team_chat_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT NOT NULL DEFAULT 'default',
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            author_user_id INTEGER NOT NULL DEFAULT 0,
            author_username TEXT NOT NULL DEFAULT '',
            message_kind TEXT NOT NULL DEFAULT 'chat',
            tool_name TEXT,
            tool_call_id TEXT,
            tool_args_json TEXT,
            tool_result_json TEXT,
            attachment_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    message_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(team_chat_messages)").fetchall()
    }
    if "attachment_id" not in message_columns:
        conn.execute("ALTER TABLE team_chat_messages ADD COLUMN attachment_id INTEGER")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_team_chat_conv_created
        ON team_chat_messages(conversation_id, id DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_chat_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            content_type TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            file_blob BLOB NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_chat_notification_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            team_chat_app_enabled INTEGER NOT NULL DEFAULT 1,
            team_chat_sms_enabled INTEGER NOT NULL DEFAULT 0,
            team_chat_email_enabled INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(team_id, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_team_chat_notify_team_user
        ON team_chat_notification_settings(team_id, user_id)
        """
    )
    conn.commit()


def add_team_chat_message(
    team: Group,
    *,
    actor_user=None,
    conversation_id: str = "default",
    role: str,
    content: str,
    attachment_name: str = "",
    attachment_content_type: str = "",
    attachment_blob: bytes | None = None,
) -> int:
    resolved_role = str(role or "").strip().lower()
    if resolved_role not in {"user", "assistant", "system", "tool"}:
        raise ValueError("role must be one of: user, assistant, system, tool")
    resolved_content = str(content or "").strip()
    if not resolved_content and not attachment_blob:
        raise ValueError("content or attachment is required")
    resolved_conversation_id = str(conversation_id or "").strip() or "default"

    author_user_id = int(getattr(actor_user, "id", 0) or 0)
    author_username = str(getattr(actor_user, "username", "") or "").strip()

    conn = _connect_sqlite(_team_chat_db_path(team))
    try:
        _ensure_team_chat_schema(conn)
        attachment_id: int | None = None
        if attachment_blob:
            attachment_cursor = conn.execute(
                """
                INSERT INTO team_chat_attachments (
                    file_name, content_type, file_size, file_blob
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    str(attachment_name or "attachment").strip() or "attachment",
                    str(attachment_content_type or "application/octet-stream").strip() or "application/octet-stream",
                    len(attachment_blob),
                    attachment_blob,
                ),
            )
            attachment_id = int(attachment_cursor.lastrowid or 0) or None
        cursor = conn.execute(
            """
            INSERT INTO team_chat_messages (
                conversation_id, role, content, author_user_id, author_username, attachment_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_conversation_id,
                resolved_role,
                resolved_content,
                author_user_id,
                author_username,
                attachment_id,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def list_team_chat_messages(team: Group, *, conversation_id: str = "default", limit: int = 20) -> list[dict[str, Any]]:
    resolved_conversation_id = str(conversation_id or "").strip() or "default"
    conn = _connect_sqlite(_team_chat_db_path(team))
    try:
        _ensure_team_chat_schema(conn)
        rows = conn.execute(
            """
            SELECT
                m.role,
                m.content,
                m.created_at,
                m.author_user_id,
                m.author_username,
                m.attachment_id,
                COALESCE(a.file_name, '') AS attachment_name,
                COALESCE(a.content_type, '') AS attachment_content_type,
                COALESCE(a.file_size, 0) AS attachment_size
            FROM (
                SELECT
                    id,
                    role,
                    content,
                    created_at,
                    author_user_id,
                    author_username,
                    attachment_id
                FROM team_chat_messages
                WHERE conversation_id = ?
                ORDER BY id DESC
                LIMIT ?
            ) m
            LEFT JOIN team_chat_attachments a ON a.id = m.attachment_id
            ORDER BY m.id ASC
            """,
            (resolved_conversation_id, int(max(1, limit))),
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "role": str(row["role"] or "").strip(),
            "content": str(row["content"] or "").strip(),
            "created_at": str(row["created_at"] or "").strip(),
            "author_user_id": str(row["author_user_id"] or "0"),
            "author_username": str(row["author_username"] or "").strip(),
            "attachment_id": int(row["attachment_id"]) if row["attachment_id"] is not None else None,
            "attachment_name": str(row["attachment_name"] or "").strip(),
            "attachment_content_type": str(row["attachment_content_type"] or "").strip(),
            "attachment_size": int(row["attachment_size"] or 0),
        }
        for row in rows
        if (
            str(row["role"] or "").strip() in {"user", "assistant", "system", "tool"}
            and (
                str(row["content"] or "").strip()
                or row["attachment_id"] is not None
            )
        )
    ]


def get_team_chat_attachment(team: Group, *, attachment_id: int) -> dict[str, Any] | None:
    resolved_attachment_id = int(attachment_id or 0)
    if resolved_attachment_id <= 0:
        return None
    conn = _connect_sqlite(_team_chat_db_path(team))
    try:
        _ensure_team_chat_schema(conn)
        row = conn.execute(
            """
            SELECT id, file_name, content_type, file_size, file_blob, created_at
            FROM team_chat_attachments
            WHERE id = ?
            LIMIT 1
            """,
            (resolved_attachment_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": int(row["id"] or 0),
            "file_name": str(row["file_name"] or ""),
            "content_type": str(row["content_type"] or "application/octet-stream"),
            "file_size": int(row["file_size"] or 0),
            "file_blob": row["file_blob"] or b"",
            "created_at": str(row["created_at"] or ""),
        }
    finally:
        conn.close()


_DEFAULT_TEAM_CHAT_NOTIFICATION_SETTINGS: dict[str, bool] = {
    "team_chat_app_enabled": True,
    "team_chat_sms_enabled": False,
    "team_chat_email_enabled": False,
}

_ALERT_FILTER_PROMPT_MAX_CHARS = 16000


def get_team_chat_notification_settings(team: Group, *, user_id: int) -> dict[str, bool]:
    resolved_user_id = int(user_id or 0)
    if resolved_user_id <= 0:
        return dict(_DEFAULT_TEAM_CHAT_NOTIFICATION_SETTINGS)
    conn = _connect_sqlite(_team_chat_db_path(team))
    try:
        _ensure_team_chat_schema(conn)
        row = conn.execute(
            """
            SELECT
                COALESCE(team_chat_app_enabled, 1) AS team_chat_app_enabled,
                COALESCE(team_chat_sms_enabled, 0) AS team_chat_sms_enabled,
                COALESCE(team_chat_email_enabled, 0) AS team_chat_email_enabled
            FROM team_chat_notification_settings
            WHERE team_id = ? AND user_id = ?
            LIMIT 1
            """,
            (int(team.id), resolved_user_id),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return dict(_DEFAULT_TEAM_CHAT_NOTIFICATION_SETTINGS)
    return {
        "team_chat_app_enabled": bool(int(row["team_chat_app_enabled"] or 0)),
        "team_chat_sms_enabled": bool(int(row["team_chat_sms_enabled"] or 0)),
        "team_chat_email_enabled": bool(int(row["team_chat_email_enabled"] or 0)),
    }


def upsert_team_chat_notification_settings(
    team: Group,
    *,
    user_id: int,
    payload: dict[str, Any] | None,
) -> dict[str, bool]:
    resolved_user_id = int(user_id or 0)
    if resolved_user_id <= 0:
        return dict(_DEFAULT_TEAM_CHAT_NOTIFICATION_SETTINGS)
    source = payload if isinstance(payload, dict) else {}
    normalized = {
        "team_chat_app_enabled": bool(source.get("team_chat_app_enabled", True)),
        "team_chat_sms_enabled": bool(source.get("team_chat_sms_enabled", False)),
        "team_chat_email_enabled": bool(source.get("team_chat_email_enabled", False)),
    }
    conn = _connect_sqlite(_team_chat_db_path(team))
    try:
        _ensure_team_chat_schema(conn)
        conn.execute(
            """
            INSERT INTO team_chat_notification_settings (
                team_id,
                user_id,
                team_chat_app_enabled,
                team_chat_sms_enabled,
                team_chat_email_enabled,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(team_id, user_id) DO UPDATE SET
                team_chat_app_enabled = excluded.team_chat_app_enabled,
                team_chat_sms_enabled = excluded.team_chat_sms_enabled,
                team_chat_email_enabled = excluded.team_chat_email_enabled,
                updated_at = datetime('now')
            """,
            (
                int(team.id),
                resolved_user_id,
                1 if normalized["team_chat_app_enabled"] else 0,
                1 if normalized["team_chat_sms_enabled"] else 0,
                1 if normalized["team_chat_email_enabled"] else 0,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return normalized


_DEFAULT_CALENDAR_NOTIFICATION_SETTINGS: dict[str, bool] = {
    "calendar_events_app_enabled": True,
    "calendar_events_sms_enabled": False,
    "calendar_events_email_enabled": False,
}


def get_user_calendar_notification_settings(user) -> dict[str, Any]:
    resolved_user_id = int(getattr(user, "id", 0) or 0)
    result: dict[str, Any] = {
        "user_id": resolved_user_id,
        "updated_at": "",
    }
    result.update(_DEFAULT_CALENDAR_NOTIFICATION_SETTINGS)
    if resolved_user_id <= 0:
        return result

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT
                user_id,
                COALESCE(calendar_events_app_enabled, 1) AS calendar_events_app_enabled,
                COALESCE(calendar_events_sms_enabled, 0) AS calendar_events_sms_enabled,
                COALESCE(calendar_events_email_enabled, 0) AS calendar_events_email_enabled,
                COALESCE(updated_at, '') AS updated_at
            FROM calendar_notification_settings
            WHERE user_id = ?
            LIMIT 1
            """,
            (resolved_user_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return result

    result["calendar_events_app_enabled"] = bool(int(row["calendar_events_app_enabled"] or 0))
    result["calendar_events_sms_enabled"] = bool(int(row["calendar_events_sms_enabled"] or 0))
    result["calendar_events_email_enabled"] = bool(int(row["calendar_events_email_enabled"] or 0))
    result["updated_at"] = str(row["updated_at"] or "")
    return result


def upsert_user_calendar_notification_settings(
    user,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_user_id = int(getattr(user, "id", 0) or 0)
    if resolved_user_id <= 0:
        return get_user_calendar_notification_settings(user)

    source = payload if isinstance(payload, dict) else {}
    normalized = {
        "calendar_events_app_enabled": bool(source.get("calendar_events_app_enabled", True)),
        "calendar_events_sms_enabled": bool(source.get("calendar_events_sms_enabled", False)),
        "calendar_events_email_enabled": bool(source.get("calendar_events_email_enabled", False)),
    }
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO calendar_notification_settings (
                user_id,
                calendar_events_app_enabled,
                calendar_events_sms_enabled,
                calendar_events_email_enabled,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
                calendar_events_app_enabled = excluded.calendar_events_app_enabled,
                calendar_events_sms_enabled = excluded.calendar_events_sms_enabled,
                calendar_events_email_enabled = excluded.calendar_events_email_enabled,
                updated_at = datetime('now')
            """,
            (
                resolved_user_id,
                1 if normalized["calendar_events_app_enabled"] else 0,
                1 if normalized["calendar_events_sms_enabled"] else 0,
                1 if normalized["calendar_events_email_enabled"] else 0,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return get_user_calendar_notification_settings(user)


def _normalize_alert_filter_prompt(value: str) -> str:
    normalized = str(value or "").replace("\r", "").strip()
    if len(normalized) > _ALERT_FILTER_PROMPT_MAX_CHARS:
        normalized = normalized[:_ALERT_FILTER_PROMPT_MAX_CHARS]
    return normalized


def get_user_alert_filter_prompt(user) -> dict[str, str]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT
                COALESCE(prompt, '') AS prompt,
                COALESCE(updated_at, '') AS updated_at
            FROM alert_filter_prompt
            WHERE id = 1
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return {"prompt": "", "updated_at": ""}
        return {
            "prompt": str(row["prompt"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
    finally:
        conn.close()


def update_user_alert_filter_prompt(
    user,
    *,
    prompt: str = "",
    mode: str = "replace",
) -> dict[str, str]:
    resolved_mode = str(mode or "").strip().lower() or "replace"
    if resolved_mode not in {"replace", "append", "clear"}:
        raise ValueError("mode must be one of: replace, append, clear")

    incoming_prompt = _normalize_alert_filter_prompt(prompt)
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        current_row = conn.execute(
            """
            SELECT COALESCE(prompt, '') AS prompt
            FROM alert_filter_prompt
            WHERE id = 1
            LIMIT 1
            """
        ).fetchone()
        current_prompt = str(current_row["prompt"] or "") if current_row is not None else ""

        if resolved_mode == "clear":
            next_prompt = ""
        elif resolved_mode == "append":
            if current_prompt and incoming_prompt:
                next_prompt = f"{current_prompt}\n{incoming_prompt}"
            else:
                next_prompt = current_prompt or incoming_prompt
        else:
            next_prompt = incoming_prompt

        next_prompt = _normalize_alert_filter_prompt(next_prompt)
        conn.execute(
            """
            INSERT INTO alert_filter_prompt (id, prompt, updated_at)
            VALUES (1, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                prompt = excluded.prompt,
                updated_at = datetime('now')
            """,
            (next_prompt,),
        )
        conn.commit()
        row = conn.execute(
            """
            SELECT
                COALESCE(prompt, '') AS prompt,
                COALESCE(updated_at, '') AS updated_at
            FROM alert_filter_prompt
            WHERE id = 1
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return {"prompt": "", "updated_at": ""}
        return {
            "prompt": str(row["prompt"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
    finally:
        conn.close()


def add_team_chat_tool_event(
    team: Group,
    *,
    actor_user=None,
    conversation_id: str = "default",
    kind: str,
    tool_name: str,
    tool_call_id: str = "",
    tool_args_json: str = "",
    tool_result_json: str = "",
    content: str = "",
) -> int:
    resolved_kind = str(kind or "").strip().lower()
    if resolved_kind not in {"tool_call", "tool_result"}:
        raise ValueError("kind must be one of: tool_call, tool_result")
    resolved_tool_name = str(tool_name or "").strip()
    if not resolved_tool_name:
        raise ValueError("tool_name is required")
    resolved_content = str(content or "").strip()
    if not resolved_content:
        resolved_content = f"[{resolved_kind}] {resolved_tool_name}"
    resolved_conversation_id = str(conversation_id or "").strip() or "default"

    author_user_id = int(getattr(actor_user, "id", 0) or 0)
    author_username = str(getattr(actor_user, "username", "") or "").strip()

    conn = _connect_sqlite(_team_chat_db_path(team))
    try:
        _ensure_team_chat_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO team_chat_messages (
                conversation_id, role, content, author_user_id, author_username, message_kind, tool_name, tool_call_id, tool_args_json, tool_result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_conversation_id,
                "tool",
                resolved_content,
                author_user_id,
                author_username,
                resolved_kind,
                resolved_tool_name,
                str(tool_call_id or "").strip(),
                str(tool_args_json or "").strip(),
                str(tool_result_json or "").strip(),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def list_resource_notes(user, resource_uuid: str, limit: int = 200) -> list[ResourceNoteItem]:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return []
    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        rows = conn.execute(
            """
            SELECT
                t.id,
                COALESCE(t.body, '') AS body,
                COALESCE(t.author_user_id, 0) AS author_user_id,
                COALESCE(t.author_username, '') AS author_username,
                COALESCE(t.created_at, '') AS created_at,
                t.attachment_id,
                COALESCE(a.file_name, '') AS attachment_name,
                COALESCE(a.content_type, '') AS attachment_content_type,
                COALESCE(a.file_size, 0) AS attachment_size
            FROM (
                SELECT id, body, author_user_id, author_username, attachment_id, created_at
                FROM resource_notes
                ORDER BY id DESC
                LIMIT ?
            ) t
            LEFT JOIN resource_note_attachments a ON a.id = t.attachment_id
            ORDER BY t.id ASC
            """,
            (int(max(1, limit)),),
        ).fetchall()
        return [
            ResourceNoteItem(
                id=int(row["id"]),
                resource_uuid=resolved_uuid,
                body=str(row["body"] or ""),
                author_user_id=int(row["author_user_id"] or 0),
                author_username=str(row["author_username"] or ""),
                created_at=str(row["created_at"] or ""),
                attachment_id=int(row["attachment_id"]) if row["attachment_id"] is not None else None,
                attachment_name=str(row["attachment_name"] or ""),
                attachment_content_type=str(row["attachment_content_type"] or ""),
                attachment_size=int(row["attachment_size"] or 0),
            )
            for row in rows
        ]
    finally:
        conn.close()


_RESOURCE_ALERT_SETTINGS_DEFAULTS = {
    "health_alerts_app_enabled": True,
    "health_alerts_sms_enabled": False,
    "health_alerts_email_enabled": False,
    "cloud_log_errors_app_enabled": True,
    "cloud_log_errors_sms_enabled": False,
    "cloud_log_errors_email_enabled": False,
}


def _normalize_resource_alert_settings_payload(payload: dict[str, Any] | None) -> dict[str, int]:
    source = payload or {}
    normalized: dict[str, int] = {}
    for key, default in _RESOURCE_ALERT_SETTINGS_DEFAULTS.items():
        value = source.get(key, default)
        normalized[key] = 1 if bool(value) else 0
    return normalized


def get_resource_alert_settings(user, resource_uuid: str, target_user_id: int) -> dict[str, Any]:
    resolved_uuid = str(resource_uuid or "").strip()
    resolved_user_id = int(target_user_id or 0)
    result: dict[str, Any] = {
        "user_id": resolved_user_id,
        "updated_at": "",
    }
    result.update(_RESOURCE_ALERT_SETTINGS_DEFAULTS)

    if not resolved_uuid or resolved_user_id <= 0:
        return result

    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        row = conn.execute(
            """
            SELECT
                user_id,
                health_alerts_app_enabled,
                health_alerts_sms_enabled,
                health_alerts_email_enabled,
                cloud_log_errors_app_enabled,
                cloud_log_errors_sms_enabled,
                cloud_log_errors_email_enabled,
                COALESCE(updated_at, '') AS updated_at
            FROM resource_alert_settings
            WHERE user_id = ?
            LIMIT 1
            """,
            (resolved_user_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return result

    for key in _RESOURCE_ALERT_SETTINGS_DEFAULTS:
        result[key] = bool(int(row[key] or 0))
    result["updated_at"] = str(row["updated_at"] or "")
    return result


def upsert_resource_alert_settings(
    user,
    resource_uuid: str,
    target_user_id: int,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_uuid = str(resource_uuid or "").strip()
    resolved_user_id = int(target_user_id or 0)
    if not resolved_uuid or resolved_user_id <= 0:
        return get_resource_alert_settings(user, resolved_uuid, resolved_user_id)

    normalized = _normalize_resource_alert_settings_payload(payload)
    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        conn.execute(
            """
            INSERT INTO resource_alert_settings (
                user_id,
                health_alerts_app_enabled,
                health_alerts_sms_enabled,
                health_alerts_email_enabled,
                cloud_log_errors_app_enabled,
                cloud_log_errors_sms_enabled,
                cloud_log_errors_email_enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                health_alerts_app_enabled = excluded.health_alerts_app_enabled,
                health_alerts_sms_enabled = excluded.health_alerts_sms_enabled,
                health_alerts_email_enabled = excluded.health_alerts_email_enabled,
                cloud_log_errors_app_enabled = excluded.cloud_log_errors_app_enabled,
                cloud_log_errors_sms_enabled = excluded.cloud_log_errors_sms_enabled,
                cloud_log_errors_email_enabled = excluded.cloud_log_errors_email_enabled,
                updated_at = datetime('now')
            """,
            (
                resolved_user_id,
                normalized["health_alerts_app_enabled"],
                normalized["health_alerts_sms_enabled"],
                normalized["health_alerts_email_enabled"],
                normalized["cloud_log_errors_app_enabled"],
                normalized["cloud_log_errors_sms_enabled"],
                normalized["cloud_log_errors_email_enabled"],
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return get_resource_alert_settings(user, resolved_uuid, resolved_user_id)


def add_user_notification(
    user,
    *,
    kind: str,
    title: str,
    body: str,
    resource_uuid: str = "",
    level: str = "info",
    channel: str = "app",
    metadata: dict[str, Any] | None = None,
) -> int:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        payload = metadata or {}
        cursor = conn.execute(
            """
            INSERT INTO notifications (
                kind,
                title,
                body,
                resource_uuid,
                level,
                channel,
                metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(kind or "").strip() or "event",
                str(title or "").strip()[:255],
                str(body or "").strip()[:5000],
                str(resource_uuid or "").strip(),
                str(level or "").strip().lower() or "info",
                str(channel or "").strip().lower() or "app",
                json.dumps(payload),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid or 0)
    finally:
        conn.close()


def list_user_notifications(user, *, limit: int = 20) -> dict[str, Any]:
    resolved_limit = max(1, min(int(limit or 20), 100))
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT
                id,
                kind,
                title,
                body,
                COALESCE(resource_uuid, '') AS resource_uuid,
                COALESCE(level, 'info') AS level,
                COALESCE(channel, 'app') AS channel,
                COALESCE(metadata, '{}') AS metadata,
                COALESCE(is_read, 0) AS is_read,
                COALESCE(created_at, '') AS created_at
            FROM notifications
            ORDER BY id DESC
            LIMIT ?
            """,
            (resolved_limit,),
        ).fetchall()
        unread_row = conn.execute(
            """
            SELECT COUNT(*) AS unread_count
            FROM notifications
            WHERE COALESCE(is_read, 0) = 0
            """
        ).fetchone()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            metadata = json.loads(str(row["metadata"] or "{}"))
        except (TypeError, ValueError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        items.append(
            {
                "id": int(row["id"] or 0),
                "kind": str(row["kind"] or "").strip() or "event",
                "title": str(row["title"] or "").strip(),
                "body": str(row["body"] or "").strip(),
                "resource_uuid": str(row["resource_uuid"] or "").strip(),
                "level": str(row["level"] or "info").strip().lower() or "info",
                "channel": str(row["channel"] or "app").strip().lower() or "app",
                "metadata": metadata,
                "is_read": bool(int(row["is_read"] or 0)),
                "created_at": str(row["created_at"] or "").strip(),
            }
        )

    unread_count = int((unread_row["unread_count"] if unread_row else 0) or 0)
    return {"unread_count": unread_count, "items": items}


def mark_all_user_notifications_read(user) -> int:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            UPDATE notifications
            SET is_read = 1
            WHERE COALESCE(is_read, 0) = 0
            """
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def clear_user_notifications(user) -> int:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute("DELETE FROM notifications")
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def get_user_asana_task_cache(user, *, cache_key: str = "overview") -> dict[str, Any] | None:
    resolved_cache_key = str(cache_key or "").strip() or "overview"
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT
                COALESCE(payload_json, '{}') AS payload_json,
                COALESCE(fetched_at_epoch, 0) AS fetched_at_epoch
            FROM asana_task_cache
            WHERE cache_key = ?
            LIMIT 1
            """,
            (resolved_cache_key,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    try:
        payload = json.loads(str(row["payload_json"] or "{}"))
    except (TypeError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    try:
        fetched_at_epoch = int(row["fetched_at_epoch"] or 0)
    except (TypeError, ValueError):
        fetched_at_epoch = 0
    return {
        "cache_key": resolved_cache_key,
        "fetched_at_epoch": fetched_at_epoch,
        "payload": payload,
    }


def set_user_asana_task_cache(
    user,
    *,
    payload: dict[str, Any],
    cache_key: str = "overview",
    fetched_at_epoch: int | None = None,
) -> None:
    resolved_cache_key = str(cache_key or "").strip() or "overview"
    resolved_payload = payload if isinstance(payload, dict) else {}
    resolved_fetched_at_epoch = int(fetched_at_epoch if fetched_at_epoch is not None else 0)
    if resolved_fetched_at_epoch <= 0:
        resolved_fetched_at_epoch = 0
    payload_json = json.dumps(resolved_payload, separators=(",", ":"))

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO asana_task_cache (
                cache_key,
                payload_json,
                fetched_at_epoch,
                updated_at
            ) VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(cache_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                fetched_at_epoch = excluded.fetched_at_epoch,
                updated_at = datetime('now')
            """,
            (
                resolved_cache_key,
                payload_json,
                resolved_fetched_at_epoch,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _normalize_resource_uuid_list(values: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        candidate = str(raw or "").strip().lower()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def _normalize_agenda_item_payload(item: dict[str, Any] | None) -> dict[str, Any]:
    source = str((item or {}).get("source") or "").strip().lower()
    item_id = str((item or {}).get("item_id") or (item or {}).get("id") or "").strip()
    source_item_id = str(
        (item or {}).get("source_item_id")
        or (item or {}).get("task_gid")
        or (item or {}).get("event_id")
        or ""
    ).strip()
    title = str((item or {}).get("title") or "").strip()
    due_date = str((item or {}).get("date") or "").strip()
    due_time = str((item or {}).get("time") or "").strip()
    due_at = str((item or {}).get("due_at") or "").strip()
    item_url = str((item or {}).get("url") or "").strip()
    item_meta = str((item or {}).get("meta") or "").strip()
    is_completed = bool((item or {}).get("done") or (item or {}).get("is_completed"))
    try:
        payload_json = json.dumps(item or {}, separators=(",", ":"))
    except (TypeError, ValueError):
        payload_json = "{}"
    return {
        "item_id": item_id,
        "source": source,
        "source_item_id": source_item_id,
        "title": title,
        "due_date": due_date,
        "due_time": due_time,
        "due_at": due_at,
        "item_url": item_url,
        "item_meta": item_meta,
        "is_completed": is_completed,
        "payload_json": payload_json,
    }


def _upsert_resource_agenda_task(
    user,
    *,
    resource_uuid: str,
    agenda_item: dict[str, Any],
    assigned_by_user_id: int = 0,
    assigned_by_username: str = "",
) -> None:
    resolved_resource_uuid = str(resource_uuid or "").strip().lower()
    normalized_item = _normalize_agenda_item_payload(agenda_item)
    resolved_item_id = str(normalized_item["item_id"] or "").strip()
    if not resolved_resource_uuid or not resolved_item_id:
        return
    conn = _connect_resource(user, resolved_resource_uuid)
    try:
        _ensure_resource_schema(conn)
        conn.execute(
            """
            INSERT INTO resource_agenda_tasks (
                item_id,
                source,
                source_item_id,
                title,
                due_date,
                due_time,
                due_at,
                item_url,
                item_meta,
                is_completed,
                payload_json,
                assigned_by_user_id,
                assigned_by_username,
                assigned_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(item_id) DO UPDATE SET
                source = excluded.source,
                source_item_id = excluded.source_item_id,
                title = excluded.title,
                due_date = excluded.due_date,
                due_time = excluded.due_time,
                due_at = excluded.due_at,
                item_url = excluded.item_url,
                item_meta = excluded.item_meta,
                is_completed = excluded.is_completed,
                payload_json = excluded.payload_json,
                assigned_by_user_id = excluded.assigned_by_user_id,
                assigned_by_username = excluded.assigned_by_username,
                updated_at = datetime('now')
            """,
            (
                resolved_item_id,
                str(normalized_item["source"] or "").strip(),
                str(normalized_item["source_item_id"] or "").strip(),
                str(normalized_item["title"] or "").strip(),
                str(normalized_item["due_date"] or "").strip(),
                str(normalized_item["due_time"] or "").strip(),
                str(normalized_item["due_at"] or "").strip(),
                str(normalized_item["item_url"] or "").strip(),
                str(normalized_item["item_meta"] or "").strip(),
                1 if bool(normalized_item["is_completed"]) else 0,
                str(normalized_item["payload_json"] or "{}"),
                int(assigned_by_user_id or 0),
                str(assigned_by_username or "").strip(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _delete_resource_agenda_task(user, *, resource_uuid: str, item_id: str) -> None:
    resolved_resource_uuid = str(resource_uuid or "").strip().lower()
    resolved_item_id = str(item_id or "").strip()
    if not resolved_resource_uuid or not resolved_item_id:
        return
    conn = _connect_resource(user, resolved_resource_uuid)
    try:
        _ensure_resource_schema(conn)
        conn.execute(
            "DELETE FROM resource_agenda_tasks WHERE item_id = ?",
            (resolved_item_id,),
        )
        conn.commit()
    finally:
        conn.close()


def list_user_asana_board_resource_mappings(user) -> dict[str, list[str]]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT
                COALESCE(board_gid, '') AS board_gid,
                COALESCE(resource_uuid, '') AS resource_uuid
            FROM asana_board_resource_map
            ORDER BY COALESCE(board_gid, '') ASC, COALESCE(resource_uuid, '') ASC
            """
        ).fetchall()
    finally:
        conn.close()

    mappings: dict[str, list[str]] = {}
    for row in rows:
        board_gid = str(row["board_gid"] or "").strip()
        resource_uuid = str(row["resource_uuid"] or "").strip().lower()
        if not board_gid or not resource_uuid:
            continue
        if board_gid not in mappings:
            mappings[board_gid] = []
        if resource_uuid not in mappings[board_gid]:
            mappings[board_gid].append(resource_uuid)
    return mappings


def list_user_asana_task_resource_mappings(user) -> dict[str, list[str]]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT
                COALESCE(task_gid, '') AS task_gid,
                COALESCE(resource_uuid, '') AS resource_uuid
            FROM asana_task_resource_map
            ORDER BY COALESCE(task_gid, '') ASC, COALESCE(resource_uuid, '') ASC
            """
        ).fetchall()
    finally:
        conn.close()

    mappings: dict[str, list[str]] = {}
    for row in rows:
        task_gid = str(row["task_gid"] or "").strip()
        resource_uuid = str(row["resource_uuid"] or "").strip().lower()
        if not task_gid or not resource_uuid:
            continue
        if task_gid not in mappings:
            mappings[task_gid] = []
        if resource_uuid not in mappings[task_gid]:
            mappings[task_gid].append(resource_uuid)
    return mappings


def set_user_asana_board_resource_mapping(
    user,
    *,
    board_gid: str,
    resource_uuids: list[str] | tuple[str, ...] | set[str] | None,
) -> list[str]:
    resolved_board_gid = str(board_gid or "").strip()
    if not resolved_board_gid:
        return []
    normalized_resource_uuids = _normalize_resource_uuid_list(resource_uuids)
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            "DELETE FROM asana_board_resource_map WHERE board_gid = ?",
            (resolved_board_gid,),
        )
        if normalized_resource_uuids:
            conn.executemany(
                """
                INSERT INTO asana_board_resource_map (
                    board_gid,
                    resource_uuid,
                    created_at,
                    updated_at
                ) VALUES (?, ?, datetime('now'), datetime('now'))
                """,
                [
                    (resolved_board_gid, resource_uuid)
                    for resource_uuid in normalized_resource_uuids
                ],
            )
        conn.commit()
    finally:
        conn.close()
    return normalized_resource_uuids


def set_user_asana_task_resource_mapping(
    user,
    *,
    task_gid: str,
    resource_uuids: list[str] | tuple[str, ...] | set[str] | None,
) -> list[str]:
    resolved_task_gid = str(task_gid or "").strip()
    if not resolved_task_gid:
        return []
    normalized_resource_uuids = _normalize_resource_uuid_list(resource_uuids)
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            "DELETE FROM asana_task_resource_map WHERE task_gid = ?",
            (resolved_task_gid,),
        )
        if normalized_resource_uuids:
            conn.executemany(
                """
                INSERT INTO asana_task_resource_map (
                    task_gid,
                    resource_uuid,
                    created_at,
                    updated_at
                ) VALUES (?, ?, datetime('now'), datetime('now'))
                """,
                [
                    (resolved_task_gid, resource_uuid)
                    for resource_uuid in normalized_resource_uuids
                ],
            )
        conn.commit()
    finally:
        conn.close()
    return normalized_resource_uuids


def list_user_agenda_item_resource_mappings(user) -> dict[str, list[str]]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT
                COALESCE(item_id, '') AS item_id,
                COALESCE(resource_uuid, '') AS resource_uuid
            FROM agenda_item_resource_map
            ORDER BY COALESCE(item_id, '') ASC, COALESCE(resource_uuid, '') ASC
            """
        ).fetchall()
    finally:
        conn.close()

    mappings: dict[str, list[str]] = {}
    for row in rows:
        item_id = str(row["item_id"] or "").strip()
        resource_uuid = str(row["resource_uuid"] or "").strip().lower()
        if not item_id or not resource_uuid:
            continue
        if item_id not in mappings:
            mappings[item_id] = []
        if resource_uuid not in mappings[item_id]:
            mappings[item_id].append(resource_uuid)
    return mappings


def set_user_agenda_item_resource_mapping(
    user,
    *,
    item: dict[str, Any] | None,
    resource_uuids: list[str] | tuple[str, ...] | set[str] | None,
) -> list[str]:
    normalized_item = _normalize_agenda_item_payload(item)
    resolved_item_id = str(normalized_item["item_id"] or "").strip()
    if not resolved_item_id:
        return []
    normalized_resource_uuids = _normalize_resource_uuid_list(resource_uuids)

    conn = _connect(user)
    previous_resource_uuids: list[str] = []
    try:
        _ensure_schema(conn)
        previous_rows = conn.execute(
            """
            SELECT COALESCE(resource_uuid, '') AS resource_uuid
            FROM agenda_item_resource_map
            WHERE item_id = ?
            """,
            (resolved_item_id,),
        ).fetchall()
        previous_resource_uuids = _normalize_resource_uuid_list(
            [str(row["resource_uuid"] or "").strip().lower() for row in previous_rows]
        )

        conn.execute(
            "DELETE FROM agenda_item_resource_map WHERE item_id = ?",
            (resolved_item_id,),
        )
        if normalized_resource_uuids:
            conn.executemany(
                """
                INSERT INTO agenda_item_resource_map (
                    item_id,
                    source,
                    source_item_id,
                    resource_uuid,
                    title,
                    due_date,
                    due_time,
                    due_at,
                    item_url,
                    item_meta,
                    is_completed,
                    payload_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """,
                [
                    (
                        resolved_item_id,
                        str(normalized_item["source"] or "").strip(),
                        str(normalized_item["source_item_id"] or "").strip(),
                        resource_uuid,
                        str(normalized_item["title"] or "").strip(),
                        str(normalized_item["due_date"] or "").strip(),
                        str(normalized_item["due_time"] or "").strip(),
                        str(normalized_item["due_at"] or "").strip(),
                        str(normalized_item["item_url"] or "").strip(),
                        str(normalized_item["item_meta"] or "").strip(),
                        1 if bool(normalized_item["is_completed"]) else 0,
                        str(normalized_item["payload_json"] or "{}"),
                    )
                    for resource_uuid in normalized_resource_uuids
                ],
            )
        conn.commit()
    finally:
        conn.close()

    next_set = set(normalized_resource_uuids)
    previous_set = set(previous_resource_uuids)
    removed_resource_uuids = sorted(previous_set - next_set)
    added_or_retained_uuids = sorted(next_set)

    for resource_uuid in removed_resource_uuids:
        _delete_resource_agenda_task(user, resource_uuid=resource_uuid, item_id=resolved_item_id)
    for resource_uuid in added_or_retained_uuids:
        _upsert_resource_agenda_task(
            user,
            resource_uuid=resource_uuid,
            agenda_item=normalized_item,
            assigned_by_user_id=int(getattr(user, "id", 0) or 0),
            assigned_by_username=str(getattr(user, "username", "") or "").strip(),
        )
    return normalized_resource_uuids


def list_resource_agenda_tasks(user, resource_uuid: str, limit: int = 200) -> list[dict[str, Any]]:
    resolved_uuid = str(resource_uuid or "").strip().lower()
    if not resolved_uuid:
        return []
    conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(conn)
        rows = conn.execute(
            """
            SELECT
                id,
                COALESCE(item_id, '') AS item_id,
                COALESCE(source, '') AS source,
                COALESCE(source_item_id, '') AS source_item_id,
                COALESCE(title, '') AS title,
                COALESCE(due_date, '') AS due_date,
                COALESCE(due_time, '') AS due_time,
                COALESCE(due_at, '') AS due_at,
                COALESCE(item_url, '') AS item_url,
                COALESCE(item_meta, '') AS item_meta,
                COALESCE(is_completed, 0) AS is_completed,
                COALESCE(payload_json, '{}') AS payload_json,
                COALESCE(assigned_by_user_id, 0) AS assigned_by_user_id,
                COALESCE(assigned_by_username, '') AS assigned_by_username,
                COALESCE(assigned_at, '') AS assigned_at,
                COALESCE(updated_at, '') AS updated_at
            FROM resource_agenda_tasks
            ORDER BY
                CASE WHEN COALESCE(is_completed, 0) = 1 THEN 1 ELSE 0 END ASC,
                COALESCE(due_date, '') ASC,
                COALESCE(due_time, '') ASC,
                id DESC
            LIMIT ?
            """,
            (int(max(1, limit)),),
        ).fetchall()
    finally:
        conn.close()

    task_rows: list[dict[str, Any]] = []
    for row in rows:
        payload = _json_loads_or(row["payload_json"], default={})
        if not isinstance(payload, dict):
            payload = {}
        task_rows.append(
            {
                "id": int(row["id"] or 0),
                "item_id": str(row["item_id"] or "").strip(),
                "source": str(row["source"] or "").strip(),
                "source_item_id": str(row["source_item_id"] or "").strip(),
                "title": str(row["title"] or "").strip(),
                "due_date": str(row["due_date"] or "").strip(),
                "due_time": str(row["due_time"] or "").strip(),
                "due_at": str(row["due_at"] or "").strip(),
                "item_url": str(row["item_url"] or "").strip(),
                "item_meta": str(row["item_meta"] or "").strip(),
                "is_completed": bool(int(row["is_completed"] or 0)),
                "payload": payload,
                "assigned_by_user_id": int(row["assigned_by_user_id"] or 0),
                "assigned_by_username": str(row["assigned_by_username"] or "").strip(),
                "assigned_at": str(row["assigned_at"] or "").strip(),
                "updated_at": str(row["updated_at"] or "").strip(),
            }
        )
    return task_rows


def replace_user_calendar_event_cache(
    user,
    *,
    provider: str,
    events: list[dict[str, Any]],
    fetched_at_epoch: int | None = None,
    status: str = "ok",
    message: str = "",
) -> int:
    resolved_provider = str(provider or "").strip().lower()
    if not resolved_provider:
        return 0
    resolved_fetched_at_epoch = int(fetched_at_epoch if fetched_at_epoch is not None else 0)
    if resolved_fetched_at_epoch < 0:
        resolved_fetched_at_epoch = 0
    resolved_status = str(status or "").strip().lower() or "ok"
    resolved_message = str(message or "").strip()

    normalized_rows: list[tuple[str, str, str, str, int, str, str, str]] = []
    for raw in events if isinstance(events, list) else []:
        if not isinstance(raw, dict):
            continue
        event_id = str(raw.get("event_id") or raw.get("id") or "").strip()
        if not event_id:
            continue
        title = str(raw.get("title") or "").strip()
        due_date = str(raw.get("due_date") or raw.get("date") or "").strip()
        due_time = str(raw.get("due_time") or raw.get("time") or "").strip()
        completed = bool(raw.get("is_completed") or raw.get("completed") or raw.get("done"))
        status_value = str(raw.get("status") or "").strip().lower() or ("completed" if completed else "open")
        source_url = str(raw.get("source_url") or raw.get("url") or "").strip()
        try:
            payload_json = json.dumps(raw, separators=(",", ":"))
        except (TypeError, ValueError):
            payload_json = "{}"

        normalized_rows.append(
            (
                event_id,
                title,
                due_date,
                due_time,
                1 if completed else 0,
                status_value,
                source_url,
                payload_json,
            )
        )

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute("DELETE FROM calendar_event_cache WHERE provider = ?", (resolved_provider,))
        if normalized_rows:
            conn.executemany(
                """
                INSERT INTO calendar_event_cache (
                    provider,
                    event_id,
                    title,
                    due_date,
                    due_time,
                    is_completed,
                    status,
                    source_url,
                    payload_json,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                [
                    (
                        resolved_provider,
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                    )
                    for row in normalized_rows
                ],
            )
        conn.execute(
            """
            INSERT INTO calendar_sync_state (
                provider,
                fetched_at_epoch,
                item_count,
                status,
                message,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(provider) DO UPDATE SET
                fetched_at_epoch = excluded.fetched_at_epoch,
                item_count = excluded.item_count,
                status = excluded.status,
                message = excluded.message,
                updated_at = datetime('now')
            """,
            (
                resolved_provider,
                resolved_fetched_at_epoch,
                len(normalized_rows),
                resolved_status,
                resolved_message,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return len(normalized_rows)


def list_user_calendar_event_cache(
    user,
    *,
    provider: str = "",
    limit: int = 500,
    include_completed: bool = True,
) -> list[dict[str, Any]]:
    resolved_provider = str(provider or "").strip().lower()
    resolved_limit = max(1, min(int(limit or 500), 5000))
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        params: list[Any] = []
        where_parts: list[str] = []
        if resolved_provider:
            where_parts.append("provider = ?")
            params.append(resolved_provider)
        if not include_completed:
            where_parts.append("COALESCE(is_completed, 0) = 0")

        where_sql = ""
        if where_parts:
            where_sql = "WHERE " + " AND ".join(where_parts)
        rows = conn.execute(
            f"""
            SELECT
                COALESCE(provider, '') AS provider,
                COALESCE(event_id, '') AS event_id,
                COALESCE(title, '') AS title,
                COALESCE(due_date, '') AS due_date,
                COALESCE(due_time, '') AS due_time,
                COALESCE(is_completed, 0) AS is_completed,
                COALESCE(status, '') AS status,
                COALESCE(source_url, '') AS source_url,
                COALESCE(payload_json, '{{}}') AS payload_json,
                COALESCE(updated_at, '') AS updated_at
            FROM calendar_event_cache
            {where_sql}
            ORDER BY COALESCE(due_date, '') ASC, COALESCE(due_time, '') ASC, COALESCE(title, '') ASC
            LIMIT ?
            """,
            (*params, resolved_limit),
        ).fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        items.append(
            {
                "provider": str(row["provider"] or "").strip(),
                "event_id": str(row["event_id"] or "").strip(),
                "title": str(row["title"] or "").strip(),
                "due_date": str(row["due_date"] or "").strip(),
                "due_time": str(row["due_time"] or "").strip(),
                "is_completed": bool(int(row["is_completed"] or 0)),
                "status": str(row["status"] or "").strip(),
                "source_url": str(row["source_url"] or "").strip(),
                "payload": payload,
                "updated_at": str(row["updated_at"] or "").strip(),
            }
        )
    return items


def get_user_calendar_sync_state(user, *, provider: str) -> dict[str, Any] | None:
    resolved_provider = str(provider or "").strip().lower()
    if not resolved_provider:
        return None
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT
                COALESCE(provider, '') AS provider,
                COALESCE(fetched_at_epoch, 0) AS fetched_at_epoch,
                COALESCE(item_count, 0) AS item_count,
                COALESCE(status, '') AS status,
                COALESCE(message, '') AS message,
                COALESCE(updated_at, '') AS updated_at
            FROM calendar_sync_state
            WHERE provider = ?
            LIMIT 1
            """,
            (resolved_provider,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    try:
        fetched_at_epoch = int(row["fetched_at_epoch"] or 0)
    except (TypeError, ValueError):
        fetched_at_epoch = 0
    try:
        item_count = int(row["item_count"] or 0)
    except (TypeError, ValueError):
        item_count = 0
    return {
        "provider": str(row["provider"] or "").strip().lower(),
        "fetched_at_epoch": fetched_at_epoch,
        "item_count": item_count,
        "status": str(row["status"] or "").strip().lower(),
        "message": str(row["message"] or "").strip(),
        "updated_at": str(row["updated_at"] or "").strip(),
    }


def set_user_calendar_sync_state(
    user,
    *,
    provider: str,
    fetched_at_epoch: int,
    item_count: int,
    status: str,
    message: str = "",
) -> None:
    resolved_provider = str(provider or "").strip().lower()
    if not resolved_provider:
        return
    resolved_status = str(status or "").strip().lower() or "ok"
    resolved_message = str(message or "").strip()
    resolved_epoch = int(fetched_at_epoch or 0)
    resolved_item_count = max(0, int(item_count or 0))

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO calendar_sync_state (
                provider,
                fetched_at_epoch,
                item_count,
                status,
                message,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(provider) DO UPDATE SET
                fetched_at_epoch = excluded.fetched_at_epoch,
                item_count = excluded.item_count,
                status = excluded.status,
                message = excluded.message,
                updated_at = datetime('now')
            """,
            (
                resolved_provider,
                resolved_epoch,
                resolved_item_count,
                resolved_status,
                resolved_message,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_user_calendar_event_completion(
    user,
    *,
    provider: str,
    event_id: str,
    is_completed: bool,
    status: str = "",
) -> bool:
    resolved_provider = str(provider or "").strip().lower()
    resolved_event_id = str(event_id or "").strip()
    if not resolved_provider or not resolved_event_id:
        return False
    resolved_status = str(status or "").strip().lower() or ("completed" if is_completed else "open")

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            UPDATE calendar_event_cache
            SET
                is_completed = ?,
                status = ?,
                updated_at = datetime('now')
            WHERE provider = ? AND event_id = ?
            """,
            (
                1 if is_completed else 0,
                resolved_status,
                resolved_provider,
                resolved_event_id,
            ),
        )
        conn.commit()
        return int(cursor.rowcount or 0) > 0
    finally:
        conn.close()


def upsert_user_outlook_mail_cache(
    user,
    *,
    messages: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> int:
    normalized_rows: list[tuple[Any, ...]] = []
    for raw in messages or []:
        if not isinstance(raw, dict):
            continue
        message_id = str(raw.get("message_id") or raw.get("id") or "").strip()
        if not message_id:
            continue
        folder = str(raw.get("folder") or "inbox").strip().lower() or "inbox"
        internet_message_id = str(raw.get("internet_message_id") or "").strip()
        conversation_id = str(raw.get("conversation_id") or "").strip()
        subject = str(raw.get("subject") or "").strip()
        sender_email = str(raw.get("sender_email") or "").strip().lower()
        sender_name = str(raw.get("sender_name") or "").strip()
        received_at = str(raw.get("received_at") or "").strip()
        sent_at = str(raw.get("sent_at") or "").strip()
        body_preview = str(raw.get("body_preview") or "").strip()
        body_text = str(raw.get("body_text") or "").strip()
        web_link = str(raw.get("web_link") or "").strip()
        is_read = 1 if bool(raw.get("is_read")) else 0
        has_attachments = 1 if bool(raw.get("has_attachments")) else 0

        to_recipients = raw.get("to_recipients")
        cc_recipients = raw.get("cc_recipients")
        if not isinstance(to_recipients, list):
            to_recipients = []
        if not isinstance(cc_recipients, list):
            cc_recipients = []
        to_recipients_json = json.dumps(
            [str(item or "").strip().lower() for item in to_recipients if str(item or "").strip()],
            separators=(",", ":"),
        )
        cc_recipients_json = json.dumps(
            [str(item or "").strip().lower() for item in cc_recipients if str(item or "").strip()],
            separators=(",", ":"),
        )
        raw_payload = raw.get("raw_payload") if isinstance(raw.get("raw_payload"), dict) else raw
        try:
            raw_payload_json = json.dumps(raw_payload, separators=(",", ":"))
        except (TypeError, ValueError):
            raw_payload_json = "{}"

        normalized_rows.append(
            (
                message_id,
                folder,
                internet_message_id,
                conversation_id,
                subject,
                sender_email,
                sender_name,
                to_recipients_json,
                cc_recipients_json,
                received_at,
                sent_at,
                body_preview,
                body_text,
                web_link,
                is_read,
                has_attachments,
                raw_payload_json,
            )
        )

    if not normalized_rows:
        return 0

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.executemany(
            """
            INSERT INTO outlook_mail_cache (
                message_id,
                folder,
                internet_message_id,
                conversation_id,
                subject,
                sender_email,
                sender_name,
                to_recipients_json,
                cc_recipients_json,
                received_at,
                sent_at,
                body_preview,
                body_text,
                web_link,
                is_read,
                has_attachments,
                raw_payload_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(message_id) DO UPDATE SET
                folder = excluded.folder,
                internet_message_id = excluded.internet_message_id,
                conversation_id = excluded.conversation_id,
                subject = excluded.subject,
                sender_email = excluded.sender_email,
                sender_name = excluded.sender_name,
                to_recipients_json = excluded.to_recipients_json,
                cc_recipients_json = excluded.cc_recipients_json,
                received_at = excluded.received_at,
                sent_at = excluded.sent_at,
                body_preview = excluded.body_preview,
                body_text = excluded.body_text,
                web_link = excluded.web_link,
                is_read = excluded.is_read,
                has_attachments = excluded.has_attachments,
                raw_payload_json = excluded.raw_payload_json,
                updated_at = datetime('now')
            """,
            normalized_rows,
        )
        conn.commit()
    finally:
        conn.close()
    return len(normalized_rows)


def list_user_outlook_mail_cache(
    user,
    *,
    query: str = "",
    limit: int = 40,
    folder: str = "",
    include_body: bool = False,
) -> list[dict[str, Any]]:
    resolved_query = str(query or "").strip().lower()
    resolved_folder = str(folder or "").strip().lower()
    resolved_limit = max(1, min(int(limit or 40), 500))

    where_parts: list[str] = []
    params: list[Any] = []
    if resolved_folder and resolved_folder != "all":
        where_parts.append("folder = ?")
        params.append(resolved_folder)
    if resolved_query:
        searchable_parts = [
            "lower(COALESCE(subject, '')) LIKE ?",
            "lower(COALESCE(sender_email, '')) LIKE ?",
            "lower(COALESCE(sender_name, '')) LIKE ?",
            "lower(COALESCE(body_preview, '')) LIKE ?",
            "lower(COALESCE(conversation_id, '')) LIKE ?",
        ]
        if include_body:
            searchable_parts.append("lower(COALESCE(body_text, '')) LIKE ?")
        where_parts.append(f"({' OR '.join(searchable_parts)})")
        like_value = f"%{resolved_query}%"
        params.extend([like_value for _ in searchable_parts])

    where_sql = ""
    if where_parts:
        where_sql = "WHERE " + " AND ".join(where_parts)

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                COALESCE(message_id, '') AS message_id,
                COALESCE(folder, 'inbox') AS folder,
                COALESCE(internet_message_id, '') AS internet_message_id,
                COALESCE(conversation_id, '') AS conversation_id,
                COALESCE(subject, '') AS subject,
                COALESCE(sender_email, '') AS sender_email,
                COALESCE(sender_name, '') AS sender_name,
                COALESCE(to_recipients_json, '[]') AS to_recipients_json,
                COALESCE(cc_recipients_json, '[]') AS cc_recipients_json,
                COALESCE(received_at, '') AS received_at,
                COALESCE(sent_at, '') AS sent_at,
                COALESCE(body_preview, '') AS body_preview,
                COALESCE(body_text, '') AS body_text,
                COALESCE(web_link, '') AS web_link,
                COALESCE(is_read, 0) AS is_read,
                COALESCE(has_attachments, 0) AS has_attachments,
                COALESCE(raw_payload_json, '{{}}') AS raw_payload_json,
                COALESCE(updated_at, '') AS updated_at
            FROM outlook_mail_cache
            {where_sql}
            ORDER BY
                CASE WHEN COALESCE(received_at, '') = '' THEN 1 ELSE 0 END ASC,
                COALESCE(received_at, '') DESC,
                COALESCE(updated_at, '') DESC
            LIMIT ?
            """,
            (*params, resolved_limit),
        ).fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            to_recipients = json.loads(str(row["to_recipients_json"] or "[]"))
        except (TypeError, ValueError):
            to_recipients = []
        try:
            cc_recipients = json.loads(str(row["cc_recipients_json"] or "[]"))
        except (TypeError, ValueError):
            cc_recipients = []
        if not isinstance(to_recipients, list):
            to_recipients = []
        if not isinstance(cc_recipients, list):
            cc_recipients = []
        try:
            raw_payload = json.loads(str(row["raw_payload_json"] or "{}"))
        except (TypeError, ValueError):
            raw_payload = {}
        if not isinstance(raw_payload, dict):
            raw_payload = {}

        items.append(
            {
                "message_id": str(row["message_id"] or "").strip(),
                "folder": str(row["folder"] or "inbox").strip().lower() or "inbox",
                "internet_message_id": str(row["internet_message_id"] or "").strip(),
                "conversation_id": str(row["conversation_id"] or "").strip(),
                "subject": str(row["subject"] or "").strip(),
                "sender_email": str(row["sender_email"] or "").strip().lower(),
                "sender_name": str(row["sender_name"] or "").strip(),
                "to_recipients": [str(item or "").strip().lower() for item in to_recipients if str(item or "").strip()],
                "cc_recipients": [str(item or "").strip().lower() for item in cc_recipients if str(item or "").strip()],
                "received_at": str(row["received_at"] or "").strip(),
                "sent_at": str(row["sent_at"] or "").strip(),
                "body_preview": str(row["body_preview"] or "").strip(),
                "body_text": str(row["body_text"] or "").strip(),
                "web_link": str(row["web_link"] or "").strip(),
                "is_read": bool(int(row["is_read"] or 0)),
                "has_attachments": bool(int(row["has_attachments"] or 0)),
                "raw_payload": raw_payload,
                "updated_at": str(row["updated_at"] or "").strip(),
            }
        )
    return items


def get_user_outlook_mail_cache_message(
    user,
    *,
    message_id: str,
) -> dict[str, Any] | None:
    resolved_message_id = str(message_id or "").strip()
    if not resolved_message_id:
        return None
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT
                COALESCE(message_id, '') AS message_id,
                COALESCE(folder, 'inbox') AS folder,
                COALESCE(internet_message_id, '') AS internet_message_id,
                COALESCE(conversation_id, '') AS conversation_id,
                COALESCE(subject, '') AS subject,
                COALESCE(sender_email, '') AS sender_email,
                COALESCE(sender_name, '') AS sender_name,
                COALESCE(to_recipients_json, '[]') AS to_recipients_json,
                COALESCE(cc_recipients_json, '[]') AS cc_recipients_json,
                COALESCE(received_at, '') AS received_at,
                COALESCE(sent_at, '') AS sent_at,
                COALESCE(body_preview, '') AS body_preview,
                COALESCE(body_text, '') AS body_text,
                COALESCE(web_link, '') AS web_link,
                COALESCE(is_read, 0) AS is_read,
                COALESCE(has_attachments, 0) AS has_attachments,
                COALESCE(raw_payload_json, '{}') AS raw_payload_json,
                COALESCE(updated_at, '') AS updated_at
            FROM outlook_mail_cache
            WHERE message_id = ?
            LIMIT 1
            """,
            (resolved_message_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None

    try:
        to_recipients = json.loads(str(row["to_recipients_json"] or "[]"))
    except (TypeError, ValueError):
        to_recipients = []
    try:
        cc_recipients = json.loads(str(row["cc_recipients_json"] or "[]"))
    except (TypeError, ValueError):
        cc_recipients = []
    try:
        raw_payload = json.loads(str(row["raw_payload_json"] or "{}"))
    except (TypeError, ValueError):
        raw_payload = {}
    if not isinstance(to_recipients, list):
        to_recipients = []
    if not isinstance(cc_recipients, list):
        cc_recipients = []
    if not isinstance(raw_payload, dict):
        raw_payload = {}

    return {
        "message_id": str(row["message_id"] or "").strip(),
        "folder": str(row["folder"] or "inbox").strip().lower() or "inbox",
        "internet_message_id": str(row["internet_message_id"] or "").strip(),
        "conversation_id": str(row["conversation_id"] or "").strip(),
        "subject": str(row["subject"] or "").strip(),
        "sender_email": str(row["sender_email"] or "").strip().lower(),
        "sender_name": str(row["sender_name"] or "").strip(),
        "to_recipients": [str(item or "").strip().lower() for item in to_recipients if str(item or "").strip()],
        "cc_recipients": [str(item or "").strip().lower() for item in cc_recipients if str(item or "").strip()],
        "received_at": str(row["received_at"] or "").strip(),
        "sent_at": str(row["sent_at"] or "").strip(),
        "body_preview": str(row["body_preview"] or "").strip(),
        "body_text": str(row["body_text"] or "").strip(),
        "web_link": str(row["web_link"] or "").strip(),
        "is_read": bool(int(row["is_read"] or 0)),
        "has_attachments": bool(int(row["has_attachments"] or 0)),
        "raw_payload": raw_payload,
        "updated_at": str(row["updated_at"] or "").strip(),
    }


def _normalize_reminder_action(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"", "notify", "notify_user", "notify_collaborators"}:
        return REMINDER_DEFAULT_ACTION
    raise ValueError("action must be one of: notify_user")


def create_reminder(
    user,
    *,
    title: str,
    remind_at: str,
    message: str | None = None,
    recipients: list[str] | tuple[str, ...] | None = None,
    action: str = REMINDER_DEFAULT_ACTION,
    channels: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    created_by_user_id: int | None = None,
    created_by_username: str = "",
) -> dict[str, Any]:
    resolved_title = str(title or "").strip()
    if not resolved_title:
        raise ValueError("title is required")

    remind_dt = _parse_iso_datetime(remind_at, field_name="remind_at")
    action_value = _normalize_reminder_action(action)
    recipient_list = _normalize_reminder_recipients(recipients)
    channel_map = _normalize_reminder_channels(channels)
    metadata_map = metadata if isinstance(metadata, dict) else {}
    now = _now_iso()

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO reminders (
                title,
                message,
                recipients_json,
                remind_at,
                status,
                action,
                channels_json,
                metadata_json,
                created_by_user_id,
                created_by_username,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_title,
                str(message or "").strip() or None,
                _json_dumps_compact(recipient_list),
                remind_dt.isoformat(),
                REMINDER_STATUS_SCHEDULED,
                action_value,
                _json_dumps_compact(channel_map),
                _json_dumps_compact(metadata_map),
                int(created_by_user_id) if created_by_user_id is not None else None,
                str(created_by_username or "").strip(),
                now,
                now,
            ),
        )
        reminder_id = int(cursor.lastrowid or 0)
        row = conn.execute(
            """
            SELECT
                id,
                COALESCE(title, '') AS title,
                COALESCE(message, '') AS message,
                COALESCE(recipients_json, '[]') AS recipients_json,
                COALESCE(remind_at, '') AS remind_at,
                COALESCE(status, 'scheduled') AS status,
                COALESCE(action, 'notify_user') AS action,
                COALESCE(channels_json, '{}') AS channels_json,
                COALESCE(metadata_json, '{}') AS metadata_json,
                COALESCE(last_error, '') AS last_error,
                created_by_user_id,
                COALESCE(created_by_username, '') AS created_by_username,
                COALESCE(created_at, '') AS created_at,
                COALESCE(updated_at, '') AS updated_at,
                COALESCE(sent_at, '') AS sent_at
            FROM reminders
            WHERE id = ?
            LIMIT 1
            """,
            (reminder_id,),
        ).fetchone()
        conn.commit()
    finally:
        conn.close()

    if row is None:
        raise RuntimeError("Failed to create reminder.")
    return _row_to_reminder(row)


def get_reminder(user, reminder_id: int) -> dict[str, Any] | None:
    try:
        resolved_id = int(reminder_id)
    except Exception:
        return None
    if resolved_id <= 0:
        return None

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT
                id,
                COALESCE(title, '') AS title,
                COALESCE(message, '') AS message,
                COALESCE(recipients_json, '[]') AS recipients_json,
                COALESCE(remind_at, '') AS remind_at,
                COALESCE(status, 'scheduled') AS status,
                COALESCE(action, 'notify_user') AS action,
                COALESCE(channels_json, '{}') AS channels_json,
                COALESCE(metadata_json, '{}') AS metadata_json,
                COALESCE(last_error, '') AS last_error,
                created_by_user_id,
                COALESCE(created_by_username, '') AS created_by_username,
                COALESCE(created_at, '') AS created_at,
                COALESCE(updated_at, '') AS updated_at,
                COALESCE(sent_at, '') AS sent_at
            FROM reminders
            WHERE id = ?
            LIMIT 1
            """,
            (resolved_id,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    return _row_to_reminder(row)


def update_reminder(
    user,
    reminder_id: int,
    *,
    title: str | None = None,
    remind_at: str | None = None,
    message: str | None = None,
    recipients: list[str] | tuple[str, ...] | None = None,
    action: str | None = None,
    channels: dict[str, Any] | None = None,
    status: str | None = None,
    metadata: dict[str, Any] | None = None,
    last_error: str | None = None,
) -> dict[str, Any]:
    try:
        resolved_id = int(reminder_id)
    except Exception as exc:
        raise ValueError("reminder_id must be positive") from exc
    if resolved_id <= 0:
        raise ValueError("reminder_id must be positive")

    updates: dict[str, Any] = {}
    if title is not None:
        title_value = str(title or "").strip()
        if not title_value:
            raise ValueError("title cannot be empty")
        updates["title"] = title_value
    if remind_at is not None:
        updates["remind_at"] = _parse_iso_datetime(remind_at, field_name="remind_at").isoformat()
    if message is not None:
        updates["message"] = str(message or "").strip() or None
    if recipients is not None:
        updates["recipients_json"] = _json_dumps_compact(_normalize_reminder_recipients(recipients))
    if action is not None:
        updates["action"] = _normalize_reminder_action(action)
    if channels is not None:
        updates["channels_json"] = _json_dumps_compact(_normalize_reminder_channels(channels))
    if status is not None:
        updates["status"] = _normalize_reminder_status(status)
    if metadata is not None:
        updates["metadata_json"] = _json_dumps_compact(metadata if isinstance(metadata, dict) else {})
    if last_error is not None:
        updates["last_error"] = str(last_error or "").strip()
    if not updates:
        raise ValueError("No updates provided")

    updates["updated_at"] = _now_iso()
    if updates.get("status") == REMINDER_STATUS_SENT:
        updates["sent_at"] = updates["updated_at"]

    set_clause = ", ".join(f"{field} = ?" for field in updates.keys())
    values = list(updates.values()) + [resolved_id]

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            f"UPDATE reminders SET {set_clause} WHERE id = ?",
            values,
        )
        if int(cursor.rowcount or 0) <= 0:
            raise ValueError(f"Reminder {resolved_id} not found")
        row = conn.execute(
            """
            SELECT
                id,
                COALESCE(title, '') AS title,
                COALESCE(message, '') AS message,
                COALESCE(recipients_json, '[]') AS recipients_json,
                COALESCE(remind_at, '') AS remind_at,
                COALESCE(status, 'scheduled') AS status,
                COALESCE(action, 'notify_user') AS action,
                COALESCE(channels_json, '{}') AS channels_json,
                COALESCE(metadata_json, '{}') AS metadata_json,
                COALESCE(last_error, '') AS last_error,
                created_by_user_id,
                COALESCE(created_by_username, '') AS created_by_username,
                COALESCE(created_at, '') AS created_at,
                COALESCE(updated_at, '') AS updated_at,
                COALESCE(sent_at, '') AS sent_at
            FROM reminders
            WHERE id = ?
            LIMIT 1
            """,
            (resolved_id,),
        ).fetchone()
        conn.commit()
    finally:
        conn.close()

    if row is None:
        raise ValueError(f"Reminder {resolved_id} not found")
    return _row_to_reminder(row)


def delete_reminder(user, reminder_id: int, *, hard_delete: bool = False) -> dict[str, Any]:
    try:
        resolved_id = int(reminder_id)
    except Exception as exc:
        raise ValueError("reminder_id must be positive") from exc
    if resolved_id <= 0:
        raise ValueError("reminder_id must be positive")

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT
                id,
                COALESCE(title, '') AS title,
                COALESCE(message, '') AS message,
                COALESCE(recipients_json, '[]') AS recipients_json,
                COALESCE(remind_at, '') AS remind_at,
                COALESCE(status, 'scheduled') AS status,
                COALESCE(action, 'notify_user') AS action,
                COALESCE(channels_json, '{}') AS channels_json,
                COALESCE(metadata_json, '{}') AS metadata_json,
                COALESCE(last_error, '') AS last_error,
                created_by_user_id,
                COALESCE(created_by_username, '') AS created_by_username,
                COALESCE(created_at, '') AS created_at,
                COALESCE(updated_at, '') AS updated_at,
                COALESCE(sent_at, '') AS sent_at
            FROM reminders
            WHERE id = ?
            LIMIT 1
            """,
            (resolved_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Reminder {resolved_id} not found")
        reminder = _row_to_reminder(row)

        if hard_delete:
            conn.execute("DELETE FROM reminders WHERE id = ?", (resolved_id,))
        else:
            now = _now_iso()
            conn.execute(
                """
                UPDATE reminders
                SET status = ?, updated_at = ?
                WHERE id = ?
                """,
                (REMINDER_STATUS_CANCELED, now, resolved_id),
            )
            row = conn.execute(
                """
                SELECT
                    id,
                    COALESCE(title, '') AS title,
                    COALESCE(message, '') AS message,
                    COALESCE(recipients_json, '[]') AS recipients_json,
                    COALESCE(remind_at, '') AS remind_at,
                    COALESCE(status, 'scheduled') AS status,
                    COALESCE(action, 'notify_user') AS action,
                    COALESCE(channels_json, '{}') AS channels_json,
                    COALESCE(metadata_json, '{}') AS metadata_json,
                    COALESCE(last_error, '') AS last_error,
                    created_by_user_id,
                    COALESCE(created_by_username, '') AS created_by_username,
                    COALESCE(created_at, '') AS created_at,
                    COALESCE(updated_at, '') AS updated_at,
                    COALESCE(sent_at, '') AS sent_at
                FROM reminders
                WHERE id = ?
                LIMIT 1
                """,
                (resolved_id,),
            ).fetchone()
            if row is not None:
                reminder = _row_to_reminder(row)
        conn.commit()
        return reminder
    finally:
        conn.close()


def list_due_reminders(
    user,
    *,
    now_dt: datetime | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    now_utc = (now_dt or datetime.now(timezone.utc)).astimezone(timezone.utc)
    now_iso = now_utc.isoformat()
    try:
        resolved_limit = max(1, int(limit or 200))
    except Exception:
        resolved_limit = 200

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT
                id,
                COALESCE(title, '') AS title,
                COALESCE(message, '') AS message,
                COALESCE(recipients_json, '[]') AS recipients_json,
                COALESCE(remind_at, '') AS remind_at,
                COALESCE(status, 'scheduled') AS status,
                COALESCE(action, 'notify_user') AS action,
                COALESCE(channels_json, '{}') AS channels_json,
                COALESCE(metadata_json, '{}') AS metadata_json,
                COALESCE(last_error, '') AS last_error,
                created_by_user_id,
                COALESCE(created_by_username, '') AS created_by_username,
                COALESCE(created_at, '') AS created_at,
                COALESCE(updated_at, '') AS updated_at,
                COALESCE(sent_at, '') AS sent_at
            FROM reminders
            WHERE status = ? AND remind_at <= ?
            ORDER BY remind_at ASC, id ASC
            LIMIT ?
            """,
            (REMINDER_STATUS_SCHEDULED, now_iso, resolved_limit),
        ).fetchall()
    finally:
        conn.close()

    return [_row_to_reminder(row) for row in rows]


def list_reminders(
    user,
    *,
    statuses: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    try:
        resolved_limit = max(1, int(limit or 100))
    except Exception:
        resolved_limit = 100

    cleaned_statuses: list[str] = []
    for raw_status in statuses or []:
        try:
            cleaned_statuses.append(_normalize_reminder_status(raw_status))
        except ValueError:
            continue

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        if cleaned_statuses:
            placeholders = ", ".join("?" for _ in cleaned_statuses)
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    COALESCE(title, '') AS title,
                    COALESCE(message, '') AS message,
                    COALESCE(recipients_json, '[]') AS recipients_json,
                    COALESCE(remind_at, '') AS remind_at,
                    COALESCE(status, 'scheduled') AS status,
                    COALESCE(action, 'notify_user') AS action,
                    COALESCE(channels_json, '{{}}') AS channels_json,
                    COALESCE(metadata_json, '{{}}') AS metadata_json,
                    COALESCE(last_error, '') AS last_error,
                    created_by_user_id,
                    COALESCE(created_by_username, '') AS created_by_username,
                    COALESCE(created_at, '') AS created_at,
                    COALESCE(updated_at, '') AS updated_at,
                    COALESCE(sent_at, '') AS sent_at
                FROM reminders
                WHERE status IN ({placeholders})
                ORDER BY remind_at DESC, id DESC
                LIMIT ?
                """,
                [*cleaned_statuses, resolved_limit],
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    id,
                    COALESCE(title, '') AS title,
                    COALESCE(message, '') AS message,
                    COALESCE(recipients_json, '[]') AS recipients_json,
                    COALESCE(remind_at, '') AS remind_at,
                    COALESCE(status, 'scheduled') AS status,
                    COALESCE(action, 'notify_user') AS action,
                    COALESCE(channels_json, '{}') AS channels_json,
                    COALESCE(metadata_json, '{}') AS metadata_json,
                    COALESCE(last_error, '') AS last_error,
                    created_by_user_id,
                    COALESCE(created_by_username, '') AS created_by_username,
                    COALESCE(created_at, '') AS created_at,
                    COALESCE(updated_at, '') AS updated_at,
                    COALESCE(sent_at, '') AS sent_at
                FROM reminders
                ORDER BY remind_at DESC, id DESC
                LIMIT ?
                """,
                (resolved_limit,),
            ).fetchall()
    finally:
        conn.close()

    return [_row_to_reminder(row) for row in rows]


def add_resource_note(
    user,
    resource_uuid: str,
    body: str,
    author_user_id: int,
    author_username: str,
    attachment_name: str = "",
    attachment_content_type: str = "",
    attachment_blob: bytes | None = None,
) -> int:
    resolved_resource_uuid = str(resource_uuid or "").strip()
    conn = _connect_resource(user, resolved_resource_uuid)
    try:
        _ensure_resource_schema(conn)
        resolved_body = str(body or "").strip()
        resolved_author_username = str(author_username or "").strip()
        attachment_id: int | None = None
        if attachment_blob:
            attachment_cursor = conn.execute(
                """
                INSERT INTO resource_note_attachments (
                    file_name, content_type, file_size, file_blob
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    str(attachment_name or "attachment").strip() or "attachment",
                    str(attachment_content_type or "application/octet-stream").strip() or "application/octet-stream",
                    len(attachment_blob),
                    attachment_blob,
                ),
            )
            attachment_id = int(attachment_cursor.lastrowid)

        cursor = conn.execute(
            """
            INSERT INTO resource_notes (
                body, author_user_id, author_username, attachment_id
            ) VALUES (?, ?, ?, ?)
            """,
            (
                resolved_body,
                int(author_user_id or 0),
                resolved_author_username,
                attachment_id,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def get_resource_note_attachment(user, resource_uuid: str, attachment_id: int) -> dict[str, Any] | None:
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return None
    resource_conn = _connect_resource(user, resolved_uuid)
    try:
        _ensure_resource_schema(resource_conn)
        row = resource_conn.execute(
            """
            SELECT id, file_name, content_type, file_size, file_blob, created_at
            FROM resource_note_attachments
            WHERE id = ?
            """,
            (int(attachment_id),),
        ).fetchone()
        if not row:
            return None
        return {
            "id": int(row["id"]),
            "resource_uuid": resolved_uuid,
            "file_name": str(row["file_name"] or ""),
            "content_type": str(row["content_type"] or "application/octet-stream"),
            "file_size": int(row["file_size"] or 0),
            "file_blob": row["file_blob"] or b"",
            "created_at": str(row["created_at"] or ""),
        }
    finally:
        resource_conn.close()


def create_account_api_key(user, name: str) -> tuple[int, str]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        raw_key = generate_api_key("account")
        cursor = conn.execute(
            """
            INSERT INTO api_keys (
                key_type, name, resource_uuid, key_prefix, key_hash, created_at, updated_at, is_active
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'), 1)
            """,
            (
                "account",
                (name or "Account API Key").strip() or "Account API Key",
                "",
                key_prefix(raw_key),
                hash_api_key(raw_key),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid), raw_key
    finally:
        conn.close()


def rotate_internal_account_api_key(user, name: str = "Internal Worker API Key") -> int:
    """
    Rotate an internal account API key for a user.

    Existing active account keys with the same name are deactivated, then a
    fresh account key is created. The raw key is intentionally discarded.
    """
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        resolved_name = (name or "Internal Worker API Key").strip() or "Internal Worker API Key"
        conn.execute(
            """
            UPDATE api_keys
            SET is_active = 0, updated_at = datetime('now')
            WHERE key_type = 'account'
              AND name = ?
              AND COALESCE(is_active, 1) = 1
            """,
            (resolved_name,),
        )
        raw_key = generate_api_key("account")
        cursor = conn.execute(
            """
            INSERT INTO api_keys (
                key_type, name, resource_uuid, key_prefix, key_hash, created_at, updated_at, is_active
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'), 1)
            """,
            (
                "account",
                resolved_name,
                "",
                key_prefix(raw_key),
                hash_api_key(raw_key),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def create_resource_api_key(user, name: str, resource_uuid: str) -> tuple[int, str]:
    resolved_resource_uuid = (resource_uuid or "").strip()
    if not resolved_resource_uuid:
        raise ValueError("resource_uuid is required")
    conn = _connect_resource(user, resolved_resource_uuid)
    try:
        _ensure_resource_schema(conn)
        raw_key = generate_api_key("resource")
        cursor = conn.execute(
            """
            INSERT INTO resource_api_keys (
                name, key_prefix, key_hash, created_at, updated_at, is_active
            ) VALUES (?, ?, ?, datetime('now'), datetime('now'), 1)
            """,
            (
                (name or "Resource API Key").strip() or "Resource API Key",
                key_prefix(raw_key),
                hash_api_key(raw_key),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid), raw_key
    finally:
        conn.close()


def list_resource_api_keys(user, resource_uuid: str) -> list[UserAPIKeyItem]:
    resolved_resource_uuid = str(resource_uuid or "").strip()
    if not resolved_resource_uuid:
        return []
    conn = _connect_resource(user, resolved_resource_uuid)
    try:
        _ensure_resource_schema(conn)
        rows = conn.execute(
            """
            SELECT id, name, key_prefix, created_at
            FROM resource_api_keys
            WHERE COALESCE(is_active, 1) = 1
            ORDER BY id DESC
            """,
        ).fetchall()
        return [
            UserAPIKeyItem(
                id=int(row["id"]),
                key_type="resource",
                name=str(row["name"] or "").strip() or "Resource API Key",
                resource_uuid=resolved_resource_uuid,
                key_prefix=key_preview(str(row["key_prefix"] or "").strip()),
                created_at=str(row["created_at"] or ""),
            )
            for row in rows
        ]
    finally:
        conn.close()


def revoke_resource_api_key(user, key_id: int, resource_uuid: str) -> None:
    resolved_resource_uuid = str(resource_uuid or "").strip()
    if not resolved_resource_uuid:
        return
    conn = _connect_resource(user, resolved_resource_uuid)
    try:
        _ensure_resource_schema(conn)
        conn.execute(
            """
            UPDATE resource_api_keys
            SET is_active = 0, updated_at = datetime('now')
            WHERE id = ?
            """,
            (int(key_id),),
        )
        conn.commit()
    finally:
        conn.close()


def resolve_api_key_scope(user, raw_api_key: str, resource_uuid: str) -> str:
    resolved_key = str(raw_api_key or "").strip()
    resolved_resource_uuid = str(resource_uuid or "").strip()
    if not resolved_key:
        return ""
    key_hash_value = hash_api_key(resolved_key)
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT key_type
            FROM api_keys
            WHERE key_hash = ?
              AND COALESCE(is_active, 1) = 1
              AND key_type = 'account'
            ORDER BY id DESC
            LIMIT 1
            """,
            (key_hash_value,),
        ).fetchone()
        if row and str(row["key_type"] or "").strip().lower() == "account":
            return "account"
    finally:
        conn.close()
    if not resolved_resource_uuid:
        return ""
    resource_conn = _connect_resource(user, resolved_resource_uuid)
    try:
        _ensure_resource_schema(resource_conn)
        resource_row = resource_conn.execute(
            """
            SELECT id
            FROM resource_api_keys
            WHERE key_hash = ?
              AND COALESCE(is_active, 1) = 1
            ORDER BY id DESC
            LIMIT 1
            """,
            (key_hash_value,),
        ).fetchone()
        if resource_row:
            return "resource"
        return ""
    finally:
        resource_conn.close()


def list_user_api_keys(user, key_type: str | None = None) -> list[UserAPIKeyItem]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        where = "WHERE is_active = 1"
        params: tuple[Any, ...] = ()
        if key_type:
            where += " AND key_type = ?"
            params = (str(key_type).strip().lower(),)
        rows = conn.execute(
            f"""
            SELECT id, key_type, name, COALESCE(resource_uuid, '') AS resource_uuid, key_prefix, created_at
            FROM api_keys
            {where}
            ORDER BY id DESC
            """,
            params,
        ).fetchall()
        return [
            UserAPIKeyItem(
                id=int(row["id"]),
                key_type=str(row["key_type"] or "").strip() or "account",
                name=str(row["name"] or "").strip() or "API Key",
                resource_uuid=str(row["resource_uuid"] or "").strip(),
                key_prefix=key_preview(str(row["key_prefix"] or "").strip()),
                created_at=str(row["created_at"] or ""),
            )
            for row in rows
        ]
    finally:
        conn.close()


def list_ssh_credentials(user) -> List[SSHCredentialItem]:
    items: list[SSHCredentialItem] = []

    # Account-scoped credentials live in the per-user member DB.
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, name, COALESCE(scope, 'account') as scope, COALESCE(team_name, '') as team_name,
                   encrypted_private_key, created_at
            FROM ssh_credentials
            WHERE COALESCE(is_active, 1) = 1
            ORDER BY id DESC
            """
        ).fetchall()
        for row in rows:
            key_text = row["encrypted_private_key"] or ""
            if key_text and not _is_encrypted(key_text):
                conn.execute(
                    "UPDATE ssh_credentials SET encrypted_private_key = ?, updated_at = datetime('now') WHERE id = ?",
                    (_encrypt_key_text(key_text), row["id"]),
                )
        conn.commit()
        for row in rows:
            resolved_scope = str(row["scope"] or "account").strip().lower()
            team_names = _list_ssh_team_access(conn, row["id"]) if resolved_scope == "team" else []
            items.append(
                SSHCredentialItem(
                    id=f"account:{int(row['id'])}",
                    name=row["name"],
                    scope=resolved_scope if resolved_scope in {"account", "team"} else "account",
                    team_names=team_names,
                    created_at=row["created_at"],
                )
            )
    finally:
        conn.close()

    # Team-scoped credentials live in each team root under TEAM_DATA_ROOT.
    teams = list(user.groups.order_by("name"))
    for team in teams:
        team_conn = _connect_sqlite(_team_ssh_db_path(team))
        try:
            _ensure_team_ssh_schema(team_conn)
            rows = team_conn.execute(
                """
                SELECT id, name, encrypted_private_key, created_at
                FROM ssh_credentials
                WHERE COALESCE(is_active, 1) = 1
                ORDER BY id DESC
                """
            ).fetchall()
            for row in rows:
                key_text = row["encrypted_private_key"] or ""
                if key_text and not _is_encrypted(key_text):
                    team_conn.execute(
                        "UPDATE ssh_credentials SET encrypted_private_key = ?, updated_at = datetime('now') WHERE id = ?",
                        (_encrypt_key_text(key_text), row["id"]),
                    )
            team_conn.commit()
            for row in rows:
                items.append(
                    SSHCredentialItem(
                        id=f"team:{int(team.id)}:{int(row['id'])}",
                        name=row["name"],
                        scope="team",
                        team_names=[str(team.name)],
                        created_at=str(row["created_at"] or ""),
                    )
                )
        finally:
            team_conn.close()

    return items


def add_ssh_credential(
    user,
    name: str,
    scope: str,
    team_names: list[str] | None,
    private_key_text: str,
) -> str:
    resolved_scope = scope if scope in {"account", "team"} else "account"
    encrypted_key = _encrypt_key_text(private_key_text)

    if resolved_scope == "team":
        resolved_teams = list(
            Group.objects.filter(name__in=list(team_names or []))
            .order_by("name")
        )
        first_credential_ref = ""
        for team in resolved_teams:
            team_conn = _connect_sqlite(_team_ssh_db_path(team))
            try:
                _ensure_team_ssh_schema(team_conn)
                cursor = team_conn.execute(
                    """
                    INSERT INTO ssh_credentials (
                        name, encrypted_private_key, created_by_user_id, updated_at
                    ) VALUES (?, ?, ?, datetime('now'))
                    """,
                    (name, encrypted_key, int(getattr(user, "id", 0) or 0)),
                )
                team_conn.commit()
                credential_ref = f"team:{int(team.id)}:{int(cursor.lastrowid)}"
                if not first_credential_ref:
                    first_credential_ref = credential_ref
            finally:
                team_conn.close()
        return first_credential_ref

    conn = _connect(user)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO ssh_credentials (
                name, scope, team_name, encrypted_private_key, updated_at
            ) VALUES (?, ?, ?, ?, datetime('now'))
            """,
            (name, "account", "", encrypted_key),
        )
        conn.commit()
        return f"account:{int(cursor.lastrowid)}"
    finally:
        conn.close()


def delete_ssh_credential(user, credential_id: str | int) -> None:
    resolved_scope, team_id, local_id = _parse_ssh_credential_ref(credential_id)
    if resolved_scope == "team" and team_id > 0 and local_id > 0:
        team = user.groups.filter(id=team_id).first()
        if not team:
            return
        team_conn = _connect_sqlite(_team_ssh_db_path(team))
        try:
            _ensure_team_ssh_schema(team_conn)
            team_conn.execute(
                "UPDATE ssh_credentials SET is_active = 0, updated_at = datetime('now') WHERE id = ?",
                (local_id,),
            )
            team_conn.commit()
            return
        finally:
            team_conn.close()

    if local_id <= 0:
        return
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            "DELETE FROM ssh_credential_team_access WHERE credential_id = ?",
            (local_id,),
        )
        conn.execute(
            "UPDATE ssh_credentials SET is_active = 0, updated_at = datetime('now') WHERE id = ?",
            (local_id,),
        )
        conn.commit()
    finally:
        conn.close()


def get_ssh_credential_private_key(user, credential_id: str | int) -> str | None:
    resolved_scope, team_id, local_id = _parse_ssh_credential_ref(credential_id)
    if resolved_scope == "team" and team_id > 0 and local_id > 0:
        team = user.groups.filter(id=team_id).first()
        if not team:
            return None
        team_conn = _connect_sqlite(_team_ssh_db_path(team))
        try:
            _ensure_team_ssh_schema(team_conn)
            row = team_conn.execute(
                """
                SELECT encrypted_private_key
                FROM ssh_credentials
                WHERE id = ? AND COALESCE(is_active, 1) = 1
                """,
                (local_id,),
            ).fetchone()
            if not row:
                return None
            decrypted = _decrypt_key_text(row["encrypted_private_key"] or "")
            if not decrypted:
                return None
            return decrypted
        finally:
            team_conn.close()

    if local_id <= 0:
        return None
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT encrypted_private_key
            FROM ssh_credentials
            WHERE id = ? AND COALESCE(is_active, 1) = 1
            """,
            (local_id,),
        ).fetchone()
        if not row:
            return None
        decrypted = _decrypt_key_text(row['encrypted_private_key'] or '')
        if not decrypted:
            return None
        return decrypted
    finally:
        conn.close()
