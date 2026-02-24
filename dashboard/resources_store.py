import base64
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List

from django.conf import settings
from django.utils.text import slugify
from cryptography.fernet import Fernet, InvalidToken


@dataclass
class ResourceItem:
    id: int
    name: str
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
    id: int
    name: str
    scope: str
    team_name: str
    created_at: str


_SSH_KEY_PREFIX = "enc:"


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


def _user_db_path(user) -> Path:
    base_dir = Path(getattr(settings, 'USER_DATA_ROOT', Path(settings.BASE_DIR) / 'user_data'))
    base_dir.mkdir(parents=True, exist_ok=True)
    username = user.get_username() or f"user-{user.pk}"
    safe_username = slugify(username) or f"user-{user.pk}"
    user_dir = base_dir / f"{safe_username}-{user.pk}"
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir / 'member.db'


def _connect(user) -> sqlite3.Connection:
    db_path = _user_db_path(user)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
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
            error TEXT
        )
        """
    )
    existing_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(resources)").fetchall()
    }
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
                name=row['name'],
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
) -> None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        resolved_key_text = _encrypt_key_text(ssh_key_text) if ssh_key_text else ''
        metadata_payload = json.dumps(resource_metadata or {}, separators=(",", ":"))
        conn.execute(
            """
            INSERT INTO resources (
                name, resource_type, target, address, port, db_type, healthcheck_url,
                notes, ssh_key_name, ssh_username, ssh_key_text, ssh_port,
                resource_subtype, resource_metadata, ssh_credential_id, ssh_credential_scope
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
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
        conn.commit()
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
            SET name = ?, resource_type = ?, target = ?, address = ?, port = ?, db_type = ?, healthcheck_url = ?,
                notes = ?, ssh_key_name = ?, ssh_username = ?, ssh_key_text = ?, ssh_port = ?,
                resource_subtype = ?, resource_metadata = ?, ssh_credential_id = ?, ssh_credential_scope = ?
            WHERE id = ?
            """,
            (
                name,
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
        conn.commit()
    finally:
        conn.close()


def delete_resource(user, resource_id: int) -> None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute("DELETE FROM resources WHERE id = ?", (resource_id,))
        conn.commit()
    finally:
        conn.close()


def get_resource(user, resource_id: int) -> ResourceItem | None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT id, name, resource_type, target, notes, created_at,
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
            name=row['name'],
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
) -> None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO resource_checks (resource_id, status, checked_at, target, error)
            VALUES (?, ?, ?, ?, ?)
            """,
            (resource_id, status, checked_at, target, error),
        )
        conn.commit()
    finally:
        conn.close()


def list_ssh_credentials(user) -> List[SSHCredentialItem]:
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
            key_text = row['encrypted_private_key'] or ''
            if key_text and not _is_encrypted(key_text):
                conn.execute(
                    "UPDATE ssh_credentials SET encrypted_private_key = ?, updated_at = datetime('now') WHERE id = ?",
                    (_encrypt_key_text(key_text), row['id']),
                )
        conn.commit()
        return [
            SSHCredentialItem(
                id=row['id'],
                name=row['name'],
                scope=row['scope'] or 'account',
                team_name=row['team_name'] or '',
                created_at=row['created_at'],
            )
            for row in rows
        ]
    finally:
        conn.close()


def add_ssh_credential(
    user,
    name: str,
    scope: str,
    team_name: str,
    private_key_text: str,
) -> int:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        resolved_scope = scope if scope in {'account', 'team'} else 'account'
        encrypted_key = _encrypt_key_text(private_key_text)
        cursor = conn.execute(
            """
            INSERT INTO ssh_credentials (
                name, scope, team_name, encrypted_private_key, updated_at
            ) VALUES (?, ?, ?, ?, datetime('now'))
            """,
            (name, resolved_scope, team_name, encrypted_key),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def delete_ssh_credential(user, credential_id: int) -> None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            "UPDATE ssh_credentials SET is_active = 0, updated_at = datetime('now') WHERE id = ?",
            (credential_id,),
        )
        conn.commit()
    finally:
        conn.close()


def get_ssh_credential_private_key(user, credential_id: int) -> str | None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT encrypted_private_key
            FROM ssh_credentials
            WHERE id = ? AND COALESCE(is_active, 1) = 1
            """,
            (credential_id,),
        ).fetchone()
        if not row:
            return None
        decrypted = _decrypt_key_text(row['encrypted_private_key'] or '')
        if not decrypted:
            return None
        return decrypted
    finally:
        conn.close()
