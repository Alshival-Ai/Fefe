import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List

from django.conf import settings
from django.utils.text import slugify


@dataclass
class WatchlistItem:
    id: int
    name: str
    resource_type: str
    target: str
    notes: str
    created_at: str


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
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            resource_type TEXT NOT NULL,
            target TEXT NOT NULL,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def list_watchlist(user) -> List[WatchlistItem]:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT id, name, resource_type, target, notes, created_at FROM watchlist ORDER BY id DESC"
        ).fetchall()
        return [
            WatchlistItem(
                id=row['id'],
                name=row['name'],
                resource_type=row['resource_type'],
                target=row['target'],
                notes=row['notes'] or '',
                created_at=row['created_at'],
            )
            for row in rows
        ]
    finally:
        conn.close()


def add_watchlist_item(user, name: str, resource_type: str, target: str, notes: str) -> None:
    conn = _connect(user)
    try:
        _ensure_schema(conn)
        conn.execute(
            "INSERT INTO watchlist (name, resource_type, target, notes) VALUES (?, ?, ?, ?)",
            (name, resource_type, target, notes),
        )
        conn.commit()
    finally:
        conn.close()
