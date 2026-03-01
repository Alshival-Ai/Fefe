from __future__ import annotations

import json
import os
import hashlib
import re
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from django.conf import settings
from django.contrib.auth import get_user_model

from .models import ResourcePackageOwner, ResourceTeamShare, WikiPage
from .resources_store import (
    _user_knowledge_db_path,
    _user_knowledge_db_path_for_owner_root,
    get_resource_knowledge_db_path,
    get_resource_owner_context,
    list_resource_agenda_tasks,
    list_resource_notes,
)

_RESOURCE_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
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


def _connect_sqlite(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _owner_collection_name(owner_scope: str) -> str:
    scope = str(owner_scope or "").strip().lower()
    if scope == "global":
        return "global_resources"
    if scope == "team":
        return "team_resources"
    return "user_resources"


def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return "{}"


def _stable_json_hash(value: Any) -> str:
    payload = _safe_json_dumps(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _owner_sqlite_db_path(owner_root: Path, owner_scope: str) -> Path:
    scope = str(owner_scope or "").strip().lower()
    if scope == "global":
        return owner_root / "global.db"
    if scope == "team":
        return owner_root / "team.db"
    root_name = str(owner_root.name or "").strip()
    if root_name == ".alshival":
        member_db_path = owner_root / "member.db"
        home_dir = owner_root.parent
        legacy_owner_root = home_dir.parent if str(home_dir.name or "").strip() == "home" else None
    else:
        member_db_path = owner_root / "home" / ".alshival" / "member.db"
        legacy_owner_root = owner_root
    if legacy_owner_root is not None:
        legacy_path = legacy_owner_root / "member.db"
        if legacy_path.exists() and not member_db_path.exists():
            try:
                member_db_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(legacy_path), str(member_db_path))
            except Exception:
                pass
    return member_db_path


def _ensure_owner_snapshot_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_health_snapshots (
            resource_uuid TEXT PRIMARY KEY,
            collection_name TEXT NOT NULL,
            name TEXT NOT NULL,
            owner_scope TEXT NOT NULL,
            owner_user_id INTEGER NOT NULL DEFAULT 0,
            owner_team_id INTEGER NOT NULL DEFAULT 0,
            status TEXT,
            checked_at TEXT,
            check_method TEXT,
            latency_ms REAL,
            packet_loss_pct REAL,
            error TEXT,
            target TEXT,
            address TEXT,
            port TEXT,
            healthcheck_url TEXT,
            resource_type TEXT,
            ssh_configured INTEGER NOT NULL DEFAULT 0,
            resource_metadata TEXT NOT NULL DEFAULT '{}',
            document_json TEXT NOT NULL DEFAULT '{}',
            document_text TEXT NOT NULL DEFAULT '',
            context_hash TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        """
    )
    columns = {
        str(row["name"] or "").strip().lower()
        for row in conn.execute("PRAGMA table_info(resource_health_snapshots)").fetchall()
    }
    if "context_hash" not in columns:
        conn.execute(
            "ALTER TABLE resource_health_snapshots ADD COLUMN context_hash TEXT NOT NULL DEFAULT ''"
        )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_res_health_snap_updated
        ON resource_health_snapshots(updated_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_res_health_snap_collection
        ON resource_health_snapshots(collection_name, updated_at)
        """
    )
    conn.commit()


def _ensure_chroma_path(path: Path) -> Path:
    if path.exists() and path.is_file():
        backup = path.with_name(f"{path.name}.sqlite_legacy")
        if not backup.exists():
            path.rename(backup)
        else:
            path.unlink(missing_ok=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _get_chroma_collection(knowledge_path: Path):
    _ensure_runtime_cache_dirs()
    try:
        import chromadb
    except Exception:
        return None
    resolved_path = _ensure_chroma_path(knowledge_path)
    client = chromadb.PersistentClient(path=str(resolved_path))
    return client.get_or_create_collection(name="resources")


def _build_chroma_metadata(
    *,
    resource_uuid: str,
    owner_scope: str,
    owner_user_id: int,
    owner_team_id: int,
    resource,
    status: str,
    checked_at: str,
    check_method: str,
    latency_ms: float | None,
    packet_loss_pct: float | None,
    ssh_configured: bool,
    document_json: str,
    context_hash: str,
) -> dict[str, Any]:
    return {
        "resource_uuid": resource_uuid,
        "collection_name": "resources",
        "owner_scope": owner_scope,
        "owner_user_id": owner_user_id,
        "owner_team_id": owner_team_id,
        "status": str(status or "").strip(),
        "check_method": str(check_method or "").strip(),
        "checked_at": str(checked_at or "").strip(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "name": str(getattr(resource, "name", "") or "").strip(),
        "resource_type": str(getattr(resource, "resource_type", "") or "").strip(),
        "target": str(getattr(resource, "target", "") or "").strip(),
        "ssh_configured": bool(ssh_configured),
        "resource_document_json": document_json,
        "resource_context_hash": str(context_hash or "").strip(),
    }


def _flatten_collection_ids(raw_ids: object) -> list[str]:
    if isinstance(raw_ids, list) and raw_ids and isinstance(raw_ids[0], list):
        raw_ids = raw_ids[0]
    if not isinstance(raw_ids, list):
        return []
    return [str(item or "").strip() for item in raw_ids if str(item or "").strip()]


def _resource_uuid_from_collection_record_id(record_id: str) -> str:
    resolved_id = str(record_id or "").strip()
    if not resolved_id:
        return ""
    if _RESOURCE_UUID_RE.fullmatch(resolved_id):
        return resolved_id.lower()
    prefix = str(resolved_id.split(":", 1)[0] or "").strip()
    if _RESOURCE_UUID_RE.fullmatch(prefix):
        return prefix.lower()
    return ""


def _resource_wiki_page_record_id(resource_uuid: str, page_id: object) -> str:
    resolved_uuid = str(resource_uuid or "").strip().lower()
    try:
        resolved_page_id = int(page_id or 0)
    except Exception:
        resolved_page_id = 0
    if not resolved_uuid or resolved_page_id <= 0:
        return ""
    return f"{resolved_uuid}:wiki_page:{resolved_page_id}"


def _resource_wiki_page_documents(
    *,
    resource_uuid: str,
    resource_name: str,
    owner_scope: str,
    owner_user_id: int,
    owner_team_id: int,
    context_hash: str,
    page_payloads: list[dict[str, Any]],
) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    ids: list[str] = []
    documents: list[str] = []
    metadatas: list[dict[str, Any]] = []
    for page in page_payloads:
        if not isinstance(page, dict):
            continue
        page_id = int(page.get("id") or 0)
        record_id = _resource_wiki_page_record_id(resource_uuid, page_id)
        if not record_id:
            continue
        page_title = str(page.get("title") or "").strip()
        page_path = str(page.get("path") or "").strip()
        page_body = str(page.get("body_markdown") or "").strip()
        page_updated_at = str(page.get("updated_at") or "").strip()
        page_document = "\n".join(
            part
            for part in [
                f"Resource: {resource_name}" if resource_name else "",
                f"Wiki Path: {page_path}" if page_path else "",
                f"Wiki Title: {page_title}" if page_title else "",
                page_body,
            ]
            if part
        ).strip()
        if not page_document:
            continue
        ids.append(record_id)
        documents.append(page_document)
        metadatas.append(
            {
                "source": "resource_wiki_page",
                "collection_name": "resources",
                "resource_uuid": str(resource_uuid or "").strip(),
                "owner_scope": str(owner_scope or "").strip(),
                "owner_user_id": int(owner_user_id or 0),
                "owner_team_id": int(owner_team_id or 0),
                "name": resource_name,
                "wiki_page_id": page_id,
                "title": page_title,
                "path": page_path,
                "is_draft": bool(page.get("is_draft", False)),
                "updated_at": page_updated_at,
                "resource_context_hash": str(context_hash or "").strip(),
            }
        )
    return ids, documents, metadatas


def _upsert_resource_records_to_collection(
    *,
    collection,
    resource_uuid: str,
    resource_name: str,
    document_text: str,
    resource_metadata: dict[str, Any],
    owner_scope: str,
    owner_user_id: int,
    owner_team_id: int,
    context_hash: str,
    page_payloads: list[dict[str, Any]],
) -> None:
    if collection is None:
        return

    normalized_resource_uuid = str(resource_uuid or "").strip().lower()
    if not normalized_resource_uuid:
        return

    wiki_ids, wiki_documents, wiki_metadatas = _resource_wiki_page_documents(
        resource_uuid=normalized_resource_uuid,
        resource_name=resource_name,
        owner_scope=owner_scope,
        owner_user_id=owner_user_id,
        owner_team_id=owner_team_id,
        context_hash=context_hash,
        page_payloads=page_payloads,
    )

    upsert_ids = [normalized_resource_uuid] + list(wiki_ids)
    upsert_docs = [document_text or resource_name or normalized_resource_uuid] + list(wiki_documents)
    upsert_metas = [dict(resource_metadata or {})] + list(wiki_metadatas)
    collection.upsert(ids=upsert_ids, documents=upsert_docs, metadatas=upsert_metas)

    expected_wiki_ids = set(wiki_ids)
    try:
        existing_rows = collection.get(
            where={"resource_uuid": normalized_resource_uuid},
            include=[],
        )
    except Exception:
        existing_rows = {}
    existing_ids = _flatten_collection_ids(existing_rows.get("ids") if isinstance(existing_rows, dict) else [])
    stale_wiki_ids = [
        item_id
        for item_id in existing_ids
        if str(item_id or "").startswith(f"{normalized_resource_uuid}:wiki_page:")
        and item_id not in expected_wiki_ids
    ]
    if stale_wiki_ids:
        try:
            collection.delete(ids=stale_wiki_ids)
        except Exception:
            pass


def _build_document(resource, owner_context: dict[str, Any], check_payload: dict[str, Any]) -> tuple[dict[str, Any], str, bool]:
    resource_uuid = str(getattr(resource, "resource_uuid", "") or "").strip()
    notes_rows = list_resource_notes(owner_context.get("owner_user"), resource_uuid, limit=200)
    notes_payload: list[dict[str, Any]] = []
    for row in notes_rows:
        notes_payload.append(
            {
                "id": int(getattr(row, "id", 0) or 0),
                "body": str(getattr(row, "body", "") or ""),
                "author_user_id": int(getattr(row, "author_user_id", 0) or 0),
                "author_username": str(getattr(row, "author_username", "") or ""),
                "created_at": str(getattr(row, "created_at", "") or ""),
                "attachment_id": int(getattr(row, "attachment_id", 0) or 0) if getattr(row, "attachment_id", None) else None,
                "attachment_name": str(getattr(row, "attachment_name", "") or ""),
                "attachment_content_type": str(getattr(row, "attachment_content_type", "") or ""),
                "attachment_size": int(getattr(row, "attachment_size", 0) or 0),
            }
            )

    agenda_tasks_rows = list_resource_agenda_tasks(owner_context.get("owner_user"), resource_uuid, limit=300)
    agenda_tasks_payload: list[dict[str, Any]] = []
    for row in agenda_tasks_rows:
        if not isinstance(row, dict):
            continue
        agenda_tasks_payload.append(
            {
                "item_id": str(row.get("item_id") or "").strip(),
                "source": str(row.get("source") or "").strip(),
                "source_item_id": str(row.get("source_item_id") or "").strip(),
                "title": str(row.get("title") or "").strip(),
                "due_date": str(row.get("due_date") or "").strip(),
                "due_time": str(row.get("due_time") or "").strip(),
                "due_at": str(row.get("due_at") or "").strip(),
                "item_meta": str(row.get("item_meta") or "").strip(),
                "is_completed": bool(row.get("is_completed")),
                "assigned_by_user_id": int(row.get("assigned_by_user_id") or 0),
                "assigned_by_username": str(row.get("assigned_by_username") or "").strip(),
                "assigned_at": str(row.get("assigned_at") or "").strip(),
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
        )

    ssh_username = str(getattr(resource, "ssh_username", "") or "").strip()
    ssh_key_name = str(getattr(resource, "ssh_key_name", "") or "").strip()
    ssh_credential_id = str(getattr(resource, "ssh_credential_id", "") or "").strip()
    ssh_configured = bool(
        ssh_username and (bool(getattr(resource, "ssh_key_present", False)) or ssh_key_name or ssh_credential_id)
    )

    wiki_pages_payload: list[dict[str, Any]] = []
    if resource_uuid:
        wiki_rows = list(
            WikiPage.objects.filter(
                scope=WikiPage.SCOPE_RESOURCE,
                resource_uuid=resource_uuid,
            )
            .prefetch_related("team_access")
            .order_by("-updated_at", "path")[:100]
        )
        for page in wiki_rows:
            team_names = sorted(
                [
                    str(team.name or "").strip()
                    for team in page.team_access.all()
                    if str(team.name or "").strip()
                ]
            )
            wiki_pages_payload.append(
                {
                    "id": int(getattr(page, "id", 0) or 0),
                    "path": str(getattr(page, "path", "") or "").strip(),
                    "title": str(getattr(page, "title", "") or "").strip(),
                    "is_draft": bool(getattr(page, "is_draft", False)),
                    "team_names": team_names,
                    "updated_at": str(getattr(page, "updated_at", "") or ""),
                    "body_markdown": str(getattr(page, "body_markdown", "") or ""),
                }
            )

    document: dict[str, Any] = {
        "resource": {
            "id": int(getattr(resource, "id", 0) or 0),
            "resource_uuid": resource_uuid,
            "name": str(getattr(resource, "name", "") or "").strip() or resource_uuid,
            "access_scope": str(getattr(resource, "access_scope", "") or "account").strip() or "account",
            "team_names": list(getattr(resource, "team_names", []) or []),
            "resource_type": str(getattr(resource, "resource_type", "") or "unknown").strip() or "unknown",
            "target": str(getattr(resource, "target", "") or "").strip(),
            "address": str(getattr(resource, "address", "") or "").strip(),
            "port": str(getattr(resource, "port", "") or "").strip(),
            "db_type": str(getattr(resource, "db_type", "") or "").strip(),
            "healthcheck_url": str(getattr(resource, "healthcheck_url", "") or "").strip(),
            "notes": str(getattr(resource, "notes", "") or ""),
            "created_at": str(getattr(resource, "created_at", "") or ""),
            "last_status": str(getattr(resource, "last_status", "") or ""),
            "last_checked_at": str(getattr(resource, "last_checked_at", "") or ""),
            "last_error": str(getattr(resource, "last_error", "") or ""),
            "ssh_key_name": ssh_key_name,
            "ssh_username": ssh_username,
            "ssh_key_present": bool(getattr(resource, "ssh_key_present", False)),
            "ssh_port": str(getattr(resource, "ssh_port", "") or ""),
            "ssh_credential_id": ssh_credential_id,
            "ssh_credential_scope": str(getattr(resource, "ssh_credential_scope", "") or "").strip(),
            "ssh_configured": ssh_configured,
            "resource_subtype": str(getattr(resource, "resource_subtype", "") or "").strip(),
            "resource_metadata": getattr(resource, "resource_metadata", {}) or {},
        },
        "owner_context": {
            "owner_scope": str(owner_context.get("owner_scope") or "user"),
            "owner_user_id": int(owner_context.get("owner_user_id") or 0),
            "owner_team_id": int(owner_context.get("owner_team_id") or 0),
        },
        "notes_thread": notes_payload,
        "assigned_agenda_tasks": agenda_tasks_payload,
        "resource_wiki_pages": wiki_pages_payload,
        "latest_health": {
            "status": str(check_payload.get("status") or ""),
            "checked_at": str(check_payload.get("checked_at") or ""),
            "error": str(check_payload.get("error") or ""),
            "check_method": str(check_payload.get("check_method") or ""),
        },
    }

    doc_text_parts = [
        str(document["resource"]["name"] or ""),
        str(document["resource"]["resource_type"] or ""),
        str(document["resource"]["target"] or ""),
        str(document["resource"]["address"] or ""),
        str(document["resource"]["db_type"] or ""),
        str(document["resource"]["notes"] or ""),
        str(document["latest_health"]["status"] or ""),
        str(document["latest_health"]["error"] or ""),
    ]
    for note in notes_payload:
        note_body = str(note.get("body") or "").strip()
        if note_body:
            doc_text_parts.append(note_body)
    for task in agenda_tasks_payload:
        task_title = str(task.get("title") or "").strip()
        task_meta = str(task.get("item_meta") or "").strip()
        task_status = "completed" if bool(task.get("is_completed")) else "open"
        task_due = " ".join(
            [
                str(task.get("due_date") or "").strip(),
                str(task.get("due_time") or "").strip(),
            ]
        ).strip()
        task_source = str(task.get("source") or "").strip()
        summary = " | ".join(
            part for part in [task_title, task_meta, task_source, task_status, task_due] if part
        ).strip()
        if summary:
            doc_text_parts.append(summary)
    for page in wiki_pages_payload[:30]:
        page_title = str(page.get("title") or "").strip()
        page_path = str(page.get("path") or "").strip()
        page_body = str(page.get("body_markdown") or "").strip()
        if page_title:
            doc_text_parts.append(page_title)
        if page_path:
            doc_text_parts.append(page_path)
        if page_body:
            doc_text_parts.append(page_body[:2000])
    document_text = " | ".join(part for part in doc_text_parts if str(part).strip())
    return document, document_text, ssh_configured


def _resource_context_hash_from_document(document: dict[str, Any]) -> str:
    payload = dict(document or {})
    resource_block = payload.get("resource")
    if isinstance(resource_block, dict):
        normalized_resource = dict(resource_block)
        normalized_resource.pop("last_checked_at", None)
        payload["resource"] = normalized_resource

    latest_health = payload.get("latest_health")
    if isinstance(latest_health, dict):
        normalized_latest = dict(latest_health)
        normalized_latest.pop("checked_at", None)
        normalized_latest.pop("latency_ms", None)
        normalized_latest.pop("packet_loss_pct", None)
        payload["latest_health"] = normalized_latest

    return _stable_json_hash(payload)


def _current_snapshot_context_hash(*, owner_root: Path, owner_scope: str, resource_uuid: str) -> str:
    db_path = _owner_sqlite_db_path(owner_root, owner_scope)
    if not db_path.exists():
        return ""
    conn = _connect_sqlite(db_path)
    try:
        _ensure_owner_snapshot_schema(conn)
        row = conn.execute(
            "SELECT context_hash FROM resource_health_snapshots WHERE resource_uuid = ?",
            (str(resource_uuid or "").strip(),),
        ).fetchone()
        if row is None:
            return ""
        return str(row["context_hash"] or "").strip()
    except Exception:
        return ""
    finally:
        conn.close()


def _upsert_owner_snapshot(
    *,
    owner_root: Path,
    owner_scope: str,
    owner_user_id: int,
    owner_team_id: int,
    resource,
    collection_name: str,
    status: str,
    checked_at: str,
    error: str,
    check_method: str,
    latency_ms: float | None,
    packet_loss_pct: float | None,
    document_json: str,
    document_text: str,
    context_hash: str,
    ssh_configured: bool,
) -> None:
    db_path = _owner_sqlite_db_path(owner_root, owner_scope)
    conn = _connect_sqlite(db_path)
    try:
        _ensure_owner_snapshot_schema(conn)
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO resource_health_snapshots (
                resource_uuid, collection_name, name, owner_scope, owner_user_id, owner_team_id,
                status, checked_at, check_method, latency_ms, packet_loss_pct, error,
                target, address, port, healthcheck_url, resource_type, ssh_configured,
                resource_metadata, document_json, document_text, context_hash, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(resource_uuid) DO UPDATE SET
                collection_name=excluded.collection_name,
                name=excluded.name,
                owner_scope=excluded.owner_scope,
                owner_user_id=excluded.owner_user_id,
                owner_team_id=excluded.owner_team_id,
                status=excluded.status,
                checked_at=excluded.checked_at,
                check_method=excluded.check_method,
                latency_ms=excluded.latency_ms,
                packet_loss_pct=excluded.packet_loss_pct,
                error=excluded.error,
                target=excluded.target,
                address=excluded.address,
                port=excluded.port,
                healthcheck_url=excluded.healthcheck_url,
                resource_type=excluded.resource_type,
                ssh_configured=excluded.ssh_configured,
                resource_metadata=excluded.resource_metadata,
                document_json=excluded.document_json,
                document_text=excluded.document_text,
                context_hash=excluded.context_hash,
                updated_at=excluded.updated_at
            """,
            (
                str(getattr(resource, "resource_uuid", "") or "").strip(),
                collection_name,
                str(getattr(resource, "name", "") or "").strip() or str(getattr(resource, "resource_uuid", "") or ""),
                owner_scope,
                int(owner_user_id or 0),
                int(owner_team_id or 0),
                str(status or "").strip(),
                str(checked_at or "").strip(),
                str(check_method or "").strip(),
                latency_ms,
                packet_loss_pct,
                str(error or "").strip(),
                str(getattr(resource, "target", "") or "").strip(),
                str(getattr(resource, "address", "") or "").strip(),
                str(getattr(resource, "port", "") or "").strip(),
                str(getattr(resource, "healthcheck_url", "") or "").strip(),
                str(getattr(resource, "resource_type", "") or "").strip(),
                1 if ssh_configured else 0,
                _safe_json_dumps(getattr(resource, "resource_metadata", {}) or {}),
                document_json,
                document_text,
                str(context_hash or "").strip(),
                now_iso,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_resource_health_knowledge(
    *,
    user,
    resource,
    status: str,
    checked_at: str,
    error: str,
    check_method: str,
    latency_ms: float | None,
    packet_loss_pct: float | None,
) -> None:
    resource_uuid = str(getattr(resource, "resource_uuid", "") or "").strip()
    if not resource_uuid:
        return

    knowledge_db_path = get_resource_knowledge_db_path(user, resource_uuid)
    owner_context: dict[str, Any] = get_resource_owner_context(user, resource_uuid)
    owner_scope = str(owner_context.get("owner_scope") or "user")
    collection_name = _owner_collection_name(owner_scope)
    owner_user_id = int(owner_context.get("owner_user_id") or 0)
    owner_team_id = int(owner_context.get("owner_team_id") or 0)
    owner_root = Path(owner_context.get("owner_root") or knowledge_db_path.parent)
    owner_user = user
    if owner_user_id > 0:
        try:
            from django.contrib.auth import get_user_model

            User = get_user_model()
            resolved_user = User.objects.filter(id=owner_user_id).first()
            if resolved_user is not None:
                owner_user = resolved_user
        except Exception:
            pass

    document, document_text, ssh_configured = _build_document(
        resource,
        {
            "owner_scope": owner_scope,
            "owner_user_id": owner_user_id,
            "owner_team_id": owner_team_id,
            "owner_user": owner_user,
        },
        {
            "status": status,
            "checked_at": checked_at,
            "error": error,
            "check_method": check_method,
            "latency_ms": latency_ms,
            "packet_loss_pct": packet_loss_pct,
        },
    )
    document_json = _safe_json_dumps(document)
    context_hash = _resource_context_hash_from_document(document)
    existing_context_hash = _current_snapshot_context_hash(
        owner_root=owner_root,
        owner_scope=owner_scope,
        resource_uuid=resource_uuid,
    )
    skip_chroma_upsert = bool(existing_context_hash and existing_context_hash == context_hash)

    metadata = _build_chroma_metadata(
        resource_uuid=resource_uuid,
        owner_scope=owner_scope,
        owner_user_id=owner_user_id,
        owner_team_id=owner_team_id,
        resource=resource,
        status=status,
        checked_at=checked_at,
        check_method=check_method,
        latency_ms=latency_ms,
        packet_loss_pct=packet_loss_pct,
        ssh_configured=ssh_configured,
        document_json=document_json,
        context_hash=context_hash,
    )
    if not skip_chroma_upsert:
        resource_name = str(getattr(resource, "name", "") or resource_uuid).strip() or resource_uuid
        wiki_payloads = document.get("resource_wiki_pages")
        if not isinstance(wiki_payloads, list):
            wiki_payloads = []

        # Always keep a resource-scoped KB copy under the resource package directory.
        resource_dir = Path(owner_context.get("resource_dir") or owner_root / "resources" / resource_uuid)
        resource_dir.mkdir(parents=True, exist_ok=True)
        resource_kb_path = resource_dir / "knowledge.db"
        resource_collection = _get_chroma_collection(resource_kb_path)
        if resource_collection is not None:
            _upsert_resource_records_to_collection(
                collection=resource_collection,
                resource_uuid=resource_uuid,
                resource_name=resource_name,
                document_text=document_text,
                resource_metadata=metadata,
                owner_scope=owner_scope,
                owner_user_id=owner_user_id,
                owner_team_id=owner_team_id,
                context_hash=context_hash,
                page_payloads=wiki_payloads,
            )

        team_ids_to_sync: set[int] = set()
        if owner_scope == "team" and owner_team_id > 0:
            team_ids_to_sync.add(int(owner_team_id))
        if owner_user_id > 0:
            shared_team_ids = (
                ResourceTeamShare.objects.filter(owner_id=owner_user_id, resource_uuid=resource_uuid)
                .values_list("team_id", flat=True)
            )
            for team_id in shared_team_ids:
                resolved_team_id = int(team_id or 0)
                if resolved_team_id > 0:
                    team_ids_to_sync.add(resolved_team_id)

        if team_ids_to_sync:
            User = get_user_model()
            team_members = list(
                User.objects.filter(is_active=True, groups__id__in=team_ids_to_sync)
                .distinct()
                .order_by("id")
            )
            for member in team_members:
                member_kb_path = _user_knowledge_db_path(member)
                member_collection = _get_chroma_collection(member_kb_path)
                if member_collection is None:
                    continue
                _upsert_resource_records_to_collection(
                    collection=member_collection,
                    resource_uuid=resource_uuid,
                    resource_name=resource_name,
                    document_text=document_text,
                    resource_metadata=metadata,
                    owner_scope=owner_scope,
                    owner_user_id=owner_user_id,
                    owner_team_id=owner_team_id,
                    context_hash=context_hash,
                    page_payloads=wiki_payloads,
                )

        if owner_scope != "team":
            collection = _get_chroma_collection(knowledge_db_path)
            if collection is not None:
                _upsert_resource_records_to_collection(
                    collection=collection,
                    resource_uuid=resource_uuid,
                    resource_name=resource_name,
                    document_text=document_text,
                    resource_metadata=metadata,
                    owner_scope=owner_scope,
                    owner_user_id=owner_user_id,
                    owner_team_id=owner_team_id,
                    context_hash=context_hash,
                    page_payloads=wiki_payloads,
                )

    _upsert_owner_snapshot(
        owner_root=owner_root,
        owner_scope=owner_scope,
        owner_user_id=owner_user_id,
        owner_team_id=owner_team_id,
        resource=resource,
        collection_name=collection_name,
        status=status,
        checked_at=checked_at,
        error=error,
        check_method=check_method,
        latency_ms=latency_ms,
        packet_loss_pct=packet_loss_pct,
        document_json=document_json,
        document_text=document_text,
        context_hash=context_hash,
        ssh_configured=ssh_configured,
    )


def _iter_owner_roots() -> list[tuple[str, Path]]:
    roots: list[tuple[str, Path]] = []
    user_root = Path(getattr(settings, "USER_DATA_ROOT", Path(settings.BASE_DIR) / "var" / "user_data"))
    team_root = Path(getattr(settings, "TEAM_DATA_ROOT", Path(settings.BASE_DIR) / "var" / "team_data"))
    global_root = Path(getattr(settings, "GLOBAL_DATA_ROOT", Path(settings.BASE_DIR) / "var" / "global_data"))

    if user_root.exists():
        for entry in user_root.iterdir():
            if not entry.is_dir():
                continue
            home_app_root = entry / "home" / ".alshival"
            if home_app_root.exists() and home_app_root.is_dir():
                roots.append(("user", home_app_root))
            else:
                roots.append(("user", entry))
    if team_root.exists():
        for entry in team_root.iterdir():
            if entry.is_dir():
                roots.append(("team", entry))
    if global_root.exists():
        roots.append(("global", global_root))
    return roots


def _user_id_from_owner_root(owner_root: Path) -> int:
    slug = str(owner_root.name or "").strip()
    if slug == ".alshival":
        home_dir = owner_root.parent
        user_dir = home_dir.parent if str(home_dir.name or "").strip() == "home" else Path("")
        slug = str(user_dir.name or "").strip()
    if "-" not in slug:
        return 0
    suffix = slug.rsplit("-", 1)[-1]
    try:
        return int(suffix)
    except Exception:
        return 0


def _existing_resource_uuids(owner_root: Path, owner_scope: str) -> set[str]:
    values: set[str] = set()
    resources_root = owner_root / "resources"
    if not resources_root.exists():
        resources_root_entries = []
    else:
        resources_root_entries = list(resources_root.iterdir())

    for entry in resources_root_entries:
        if not entry.is_dir():
            continue
        resource_uuid = str(entry.name or "").strip()
        if resource_uuid:
            values.add(resource_uuid)

    scope = str(owner_scope or "").strip().lower()
    if scope == "user":
        user_id = _user_id_from_owner_root(owner_root)
        if user_id > 0:
            User = get_user_model()
            user = User.objects.filter(id=user_id, is_active=True).first()
            if user is not None:
                team_ids = list(user.groups.values_list("id", flat=True))
                if team_ids:
                    for resource_uuid in (
                        ResourcePackageOwner.objects.filter(
                            owner_scope=ResourcePackageOwner.OWNER_SCOPE_TEAM,
                            owner_team_id__in=team_ids,
                        )
                        .exclude(resource_uuid__isnull=True)
                        .exclude(resource_uuid="")
                        .values_list("resource_uuid", flat=True)
                    ):
                        value = str(resource_uuid or "").strip()
                        if value:
                            values.add(value)
                    for resource_uuid in (
                        ResourceTeamShare.objects.filter(team_id__in=team_ids)
                        .exclude(resource_uuid__isnull=True)
                        .exclude(resource_uuid="")
                        .values_list("resource_uuid", flat=True)
                    ):
                        value = str(resource_uuid or "").strip()
                        if value:
                            values.add(value)
    return values


def cleanup_stale_knowledge_records() -> dict[str, int]:
    scanned = 0
    removed_knowledge = 0
    removed_snapshots = 0

    for owner_scope, owner_root in _iter_owner_roots():
        scanned += 1
        existing = _existing_resource_uuids(owner_root, owner_scope)

        scope = str(owner_scope or "").strip().lower()
        knowledge_path = (
            _user_knowledge_db_path_for_owner_root(owner_root)
            if scope == "user"
            else owner_root / "knowledge.db"
        )
        if scope == "team":
            # Alpha behavior duplicates team-owned resource knowledge into member KBs.
            # Team knowledge.db is no longer an active source and can be pruned.
            if knowledge_path.exists():
                try:
                    shutil.rmtree(knowledge_path)
                    removed_knowledge += 1
                except Exception:
                    pass
        else:
            collection = None
            if knowledge_path.exists():
                collection = _get_chroma_collection(knowledge_path)
            if collection is not None:
                try:
                    rows = collection.get(include=[])
                    ids = _flatten_collection_ids(rows.get("ids") if isinstance(rows, dict) else [])
                except Exception:
                    ids = []
                stale_ids: list[str] = []
                for item in ids:
                    base_resource_uuid = _resource_uuid_from_collection_record_id(item)
                    if not base_resource_uuid:
                        continue
                    if base_resource_uuid not in existing:
                        stale_ids.append(item)
                if stale_ids:
                    try:
                        collection.delete(ids=stale_ids)
                        removed_knowledge += len(stale_ids)
                    except Exception:
                        pass

        snapshot_db_path = _owner_sqlite_db_path(owner_root, owner_scope)
        if snapshot_db_path.exists():
            conn = _connect_sqlite(snapshot_db_path)
            try:
                _ensure_owner_snapshot_schema(conn)
                rows = conn.execute("SELECT resource_uuid FROM resource_health_snapshots").fetchall()
                stale = []
                for row in rows:
                    resource_uuid = str(row["resource_uuid"] or "").strip()
                    if resource_uuid and resource_uuid not in existing:
                        stale.append(resource_uuid)
                for resource_uuid in stale:
                    conn.execute(
                        "DELETE FROM resource_health_snapshots WHERE resource_uuid = ?",
                        (resource_uuid,),
                    )
                conn.commit()
                removed_snapshots += len(stale)
            finally:
                conn.close()

    return {
        "scanned": int(scanned),
        "removed_knowledge": int(removed_knowledge),
        "removed_snapshots": int(removed_snapshots),
    }
