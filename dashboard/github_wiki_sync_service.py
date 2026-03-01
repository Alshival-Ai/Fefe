from __future__ import annotations

import base64
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Iterable

import requests
from allauth.socialaccount.models import SocialAccount, SocialToken
from django.db import transaction
from django.db.utils import OperationalError, ProgrammingError
from django.utils.text import slugify

from .models import WikiPage
from .resources_store import get_resource_owner_context
from .wiki_markdown import render_markdown_fallback

_GITHUB_API_BASE_URL = "https://api.github.com"
_GITHUB_API_TIMEOUT_SECONDS = 20
_WIKI_SCOPE_RESOURCE = WikiPage.SCOPE_RESOURCE
_GITHUB_REPO_PART_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _normalize_resource_uuid(raw_value: str) -> str:
    return str(raw_value or "").strip().lower()


def _normalize_github_repository_full_name(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    lowered = raw.lower()
    if lowered.startswith("https://"):
        raw = raw[8:]
    elif lowered.startswith("http://"):
        raw = raw[7:]

    lowered = raw.lower()
    if lowered.startswith("github.com/"):
        raw = raw[len("github.com/") :]

    raw = raw.strip().strip("/")
    if raw.endswith(".git"):
        raw = raw[:-4]

    pieces = [part.strip() for part in raw.split("/") if part.strip()]
    if len(pieces) < 2:
        return ""

    owner = pieces[0]
    repo = pieces[1]
    if not _GITHUB_REPO_PART_RE.fullmatch(owner):
        return ""
    if not _GITHUB_REPO_PART_RE.fullmatch(repo):
        return ""
    return f"{owner}/{repo}"


def _normalize_resource_github_repositories(raw_value: object) -> list[str]:
    values: list[object]
    if raw_value is None:
        values = []
    elif isinstance(raw_value, (list, tuple, set)):
        values = list(raw_value)
    else:
        values = [raw_value]

    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        for candidate in re.split(r"[\n,]", str(value or "")):
            full_name = _normalize_github_repository_full_name(candidate)
            if not full_name:
                continue
            dedupe_key = full_name.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            normalized.append(full_name)
    return normalized


def resource_github_repository_names(resource) -> list[str]:
    metadata = getattr(resource, "resource_metadata", None)
    if not isinstance(metadata, dict):
        return []
    return _normalize_resource_github_repositories(metadata.get("github_repositories"))


def _normalize_wiki_path(raw_path: str, raw_title: str = "") -> str:
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


def _wiki_filename_for_page_path(path: str) -> str:
    normalized = _normalize_wiki_path(path, "")
    if not normalized:
        return ""
    return f"{normalized}.md"


def _page_path_for_wiki_filename(filename: str) -> str:
    raw = str(filename or "").replace("\\", "/").strip().strip("/")
    if not raw:
        return ""
    if raw.lower().endswith(".md"):
        raw = raw[:-3]
    return _normalize_wiki_path(raw, raw)


def _github_access_token_for_user(user) -> tuple[str, str]:
    try:
        account = (
            SocialAccount.objects.filter(user=user, provider="github")
            .order_by("id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        return "", "database_unavailable"
    except Exception:
        return "", "unable_to_load_github_account"

    if account is None:
        return "", "github_not_connected"

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
    if not access_token:
        return "", "missing_github_oauth_token"
    return access_token, ""


def _resolve_access_token_from_users(token_users: Iterable[object]) -> tuple[object | None, str, str]:
    seen: set[int] = set()
    for user in token_users:
        if user is None or not bool(getattr(user, "is_active", False)):
            continue
        user_id = int(getattr(user, "id", 0) or 0)
        if user_id <= 0 or user_id in seen:
            continue
        seen.add(user_id)
        access_token, token_error = _github_access_token_for_user(user)
        if access_token:
            return user, access_token, ""
        if token_error not in {"github_not_connected", "missing_github_oauth_token"}:
            return user, "", token_error
    return None, "", "missing_github_token"


def _github_request_json(
    *,
    method: str,
    path: str,
    access_token: str,
    params: dict[str, Any] | None = None,
) -> tuple[Any, int, str]:
    url = f"{_GITHUB_API_BASE_URL}{path}"
    try:
        response = requests.request(
            method.upper(),
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            params=params or {},
            timeout=_GITHUB_API_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        return None, 0, f"github_request_failed:{exc}"

    try:
        body = response.json() if response.content else {}
    except Exception:
        body = {}

    status_code = int(response.status_code)
    if status_code >= 400:
        detail = ""
        if isinstance(body, dict):
            detail = str(body.get("message") or "").strip()
        if not detail:
            detail = str(response.text or "").strip()[:240] or f"status_{status_code}"
        return body, status_code, f"github_http_{status_code}:{detail}"
    return body, status_code, ""


def _github_repo_context(*, repository_full_name: str, access_token: str) -> tuple[dict[str, str], str]:
    source_payload, _source_status, source_error = _github_request_json(
        method="GET",
        path=f"/repos/{repository_full_name}",
        access_token=access_token,
    )
    if source_error:
        return {}, f"{repository_full_name}:source_repo_error:{source_error}"
    if not isinstance(source_payload, dict):
        return {}, f"{repository_full_name}:source_repo_invalid_payload"

    if not bool(source_payload.get("has_wiki", True)):
        return {}, f"{repository_full_name}:wiki_disabled"

    return {
        "source_repo": repository_full_name,
        "source_default_branch": str(source_payload.get("default_branch") or "").strip() or "main",
        "private": "1" if bool(source_payload.get("private", False)) else "0",
        "wiki_enabled": "1",
    }, ""


def _wiki_remote_url(repository_full_name: str) -> str:
    return f"https://github.com/{repository_full_name}.wiki.git"


def _wiki_worktree_dir(*, actor, resource_uuid: str, repository_full_name: str) -> Path:
    owner_context = get_resource_owner_context(actor, resource_uuid)
    resource_dir = Path(owner_context.get("resource_dir") or "")
    if not resource_dir:
        owner_root = Path(owner_context.get("owner_root") or ".")
        resource_dir = owner_root / "resources" / (resource_uuid or "unknown-resource")
    safe_repo = re.sub(r"[^a-zA-Z0-9_.-]+", "_", repository_full_name.strip().lower())
    return resource_dir / ".wiki-sync" / safe_repo


def _git_binary_available() -> bool:
    return shutil.which("git") is not None


def _git_extraheader(access_token: str) -> str:
    token = str(access_token or "").strip()
    credential = base64.b64encode(f"oauth2:{token}".encode("utf-8")).decode("ascii")
    return f"Authorization: Basic {credential}"


def _sanitize_git_detail(detail: str, *, max_length: int = 240) -> str:
    cleaned = str(detail or "").strip()
    if not cleaned:
        return ""
    cleaned = re.sub(
        r"(Authorization:\s*Basic\s+)[A-Za-z0-9+/=]+",
        r"\1***",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"(https://)([^/@\s]+)@github\.com/",
        r"\1***@github.com/",
        cleaned,
        flags=re.IGNORECASE,
    )
    if len(cleaned) > max_length:
        return cleaned[:max_length]
    return cleaned


def _classify_git_error(detail: str) -> str:
    lowered = str(detail or "").lower()
    if not lowered:
        return ""
    if "sso" in lowered:
        return "github_sso_required"
    if "authentication failed" in lowered or "access denied" in lowered:
        return "github_auth_failed"
    if "could not read username" in lowered:
        return "github_auth_failed"
    if "repository" in lowered and "not found" in lowered:
        return "wiki_repo_not_found"
    if "not found" in lowered and ".wiki.git" in lowered:
        return "wiki_repo_not_found"
    if "write access to repository not granted" in lowered:
        return "github_write_denied"
    if "permission denied" in lowered and "github.com" in lowered:
        return "github_auth_failed"
    return ""


def _run_git(
    *,
    args: list[str],
    cwd: Path | None = None,
    access_token: str = "",
    timeout: int = 90,
) -> tuple[int, str, str]:
    command = ["git"]
    if access_token:
        command.extend(["-c", f"http.https://github.com/.extraheader={_git_extraheader(access_token)}"])
    command.extend(args)
    environment = dict(os.environ)
    environment.setdefault("GIT_TERMINAL_PROMPT", "0")
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd) if cwd is not None else None,
            capture_output=True,
            text=True,
            env=environment,
            timeout=max(10, int(timeout or 90)),
            check=False,
        )
    except Exception as exc:
        return 1, "", str(exc)
    return int(completed.returncode), str(completed.stdout or ""), str(completed.stderr or "")


def _current_git_branch(worktree: Path) -> str:
    code, stdout, _stderr = _run_git(args=["rev-parse", "--abbrev-ref", "HEAD"], cwd=worktree)
    if code != 0:
        return ""
    branch = str(stdout or "").strip()
    if branch and branch != "HEAD":
        return branch

    code, stdout, _stderr = _run_git(args=["symbolic-ref", "refs/remotes/origin/HEAD"], cwd=worktree)
    if code == 0:
        value = str(stdout or "").strip()
        if value.startswith("refs/remotes/origin/"):
            candidate = value.rsplit("/", 1)[-1]
            if candidate and candidate != "HEAD":
                return candidate
    for candidate in ("master", "main"):
        code, _stdout, _stderr = _run_git(args=["show-ref", "--verify", f"refs/remotes/origin/{candidate}"], cwd=worktree)
        if code == 0:
            return candidate
    return ""


def _ensure_wiki_git_worktree(
    *,
    actor,
    resource_uuid: str,
    repository_full_name: str,
    access_token: str,
) -> tuple[Path | None, str, str]:
    if not _git_binary_available():
        return None, "", "git_unavailable"

    worktree = _wiki_worktree_dir(actor=actor, resource_uuid=resource_uuid, repository_full_name=repository_full_name)
    remote_url = _wiki_remote_url(repository_full_name)
    worktree.parent.mkdir(parents=True, exist_ok=True)

    if not (worktree / ".git").exists():
        if worktree.exists():
            try:
                shutil.rmtree(worktree)
            except Exception:
                return None, "", "wiki_worktree_cleanup_failed"
        code, stdout, stderr = _run_git(
            args=["clone", "--quiet", remote_url, str(worktree)],
            access_token=access_token,
            timeout=120,
        )
        if code != 0:
            detail = _sanitize_git_detail(stderr or stdout)
            classified = _classify_git_error(detail)
            if classified:
                return None, "", classified
            return None, "", f"git_clone_failed:{detail or 'unknown'}"

    set_url_code, set_url_stdout, set_url_stderr = _run_git(
        args=["remote", "set-url", "origin", remote_url],
        cwd=worktree,
    )
    if set_url_code != 0:
        detail = _sanitize_git_detail(set_url_stderr or set_url_stdout)
        return None, "", f"git_remote_set_url_failed:{detail or 'unknown'}"

    fetch_code, fetch_stdout, fetch_stderr = _run_git(
        args=["fetch", "--prune", "origin"],
        cwd=worktree,
        access_token=access_token,
    )
    if fetch_code != 0:
        detail = _sanitize_git_detail(fetch_stderr or fetch_stdout)
        classified = _classify_git_error(detail)
        if classified:
            return None, "", classified
        return None, "", f"git_fetch_failed:{detail or 'unknown'}"

    branch = _current_git_branch(worktree)
    if branch:
        checkout_code, checkout_stdout, checkout_stderr = _run_git(
            args=["checkout", "-q", branch],
            cwd=worktree,
        )
        if checkout_code != 0:
            detail = _sanitize_git_detail(checkout_stderr or checkout_stdout)
            return None, "", f"git_checkout_failed:{detail or branch}"

        pull_code, pull_stdout, pull_stderr = _run_git(
            args=["pull", "--ff-only", "origin", branch],
            cwd=worktree,
            access_token=access_token,
        )
        if pull_code != 0:
            detail = _sanitize_git_detail(pull_stderr or pull_stdout)
            classified = _classify_git_error(detail)
            if classified:
                return None, "", classified
            return None, "", f"git_pull_failed:{detail or branch}"
    else:
        pull_code, pull_stdout, pull_stderr = _run_git(
            args=["pull", "--ff-only"],
            cwd=worktree,
            access_token=access_token,
        )
        if pull_code != 0:
            detail = _sanitize_git_detail(pull_stderr or pull_stdout)
            # A wiki remote can be intentionally empty; keep local tree available.
            if "no such ref was fetched" not in str(detail).lower():
                classified = _classify_git_error(detail)
                if classified:
                    return None, "", classified
                return None, "", f"git_pull_failed:{detail or 'unknown'}"
        branch = _current_git_branch(worktree) or "master"

    return worktree, branch, ""


def _list_wiki_markdown_files(worktree: Path) -> list[str]:
    files: list[str] = []
    for path in worktree.rglob("*.md"):
        if ".git" in path.parts:
            continue
        rel = path.relative_to(worktree).as_posix().strip()
        if not rel:
            continue
        basename = rel.rsplit("/", 1)[-1]
        if basename.startswith("_"):
            continue
        files.append(rel)
    files.sort()
    return files


def _pull_remote_wiki_into_local(
    *,
    actor,
    resource_uuid: str,
    resource_name: str,
    repository_full_name: str,
    access_token: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "remote_files": 0,
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "draft_skipped": 0,
        "errors": 0,
        "error": "",
    }

    worktree, _branch, sync_error = _ensure_wiki_git_worktree(
        actor=actor,
        resource_uuid=resource_uuid,
        repository_full_name=repository_full_name,
        access_token=access_token,
    )
    if worktree is None:
        result["errors"] += 1
        result["error"] = sync_error or "wiki_git_unavailable"
        return result

    files = _list_wiki_markdown_files(worktree)
    existing_pages = {
        str(item.path or "").strip().lower(): item
        for item in WikiPage.objects.filter(
            scope=_WIKI_SCOPE_RESOURCE,
            resource_uuid=resource_uuid,
        )
    }

    for file_path in files:
        result["remote_files"] += 1
        normalized_path = _page_path_for_wiki_filename(file_path)
        if not normalized_path:
            continue

        absolute_file = worktree / file_path
        try:
            body_markdown = absolute_file.read_text(encoding="utf-8")
        except Exception:
            result["errors"] += 1
            if not result.get("error"):
                result["error"] = f"read_failed:{file_path}"
            continue

        if not str(body_markdown or "").strip():
            continue
        title = _extract_wiki_title_from_markdown(body_markdown)
        if not title:
            fallback_title = normalized_path.rsplit("/", 1)[-1].replace("-", " ").strip()
            title = fallback_title.title() if fallback_title else "Untitled"

        existing = existing_pages.get(normalized_path.lower())
        if existing is None:
            try:
                with transaction.atomic():
                    created = WikiPage.objects.create(
                        scope=_WIKI_SCOPE_RESOURCE,
                        resource_uuid=resource_uuid,
                        resource_name=resource_name,
                        path=normalized_path,
                        title=title,
                        is_draft=False,
                        body_markdown=body_markdown,
                        body_html_fallback=render_markdown_fallback(body_markdown),
                        created_by=actor,
                        updated_by=actor,
                    )
                existing_pages[normalized_path.lower()] = created
                result["created"] += 1
            except Exception:
                result["errors"] += 1
                if not result.get("error"):
                    result["error"] = f"create_failed:{normalized_path}"
            continue

        if bool(existing.is_draft):
            result["draft_skipped"] += 1
            continue

        needs_update = bool(
            str(existing.title or "").strip() != title
            or str(existing.body_markdown or "") != body_markdown
            or str(existing.resource_name or "").strip() != resource_name
        )
        if not needs_update:
            result["unchanged"] += 1
            continue

        try:
            with transaction.atomic():
                existing.resource_name = resource_name
                existing.title = title
                existing.body_markdown = body_markdown
                existing.body_html_fallback = render_markdown_fallback(body_markdown)
                existing.updated_by = actor
                existing.save(
                    update_fields=[
                        "resource_name",
                        "title",
                        "body_markdown",
                        "body_html_fallback",
                        "updated_by",
                        "updated_at",
                    ]
                )
            result["updated"] += 1
        except Exception:
            result["errors"] += 1
            if not result.get("error"):
                result["error"] = f"update_failed:{normalized_path}"

    return result


def _push_local_wiki_to_remote(
    *,
    actor,
    resource_uuid: str,
    resource_name: str,
    repository_full_name: str,
    access_token: str,
    changed_page_ids: Iterable[int] | None,
    deleted_paths: Iterable[str] | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "upserted": 0,
        "deleted": 0,
        "draft_skipped": 0,
        "missing_pages": 0,
        "errors": 0,
        "error": "",
    }

    normalized_deleted_paths: list[str] = []
    seen_deleted: set[str] = set()
    for raw_path in deleted_paths or []:
        normalized_path = _normalize_wiki_path(str(raw_path or ""), "")
        if not normalized_path:
            continue
        dedupe_key = normalized_path.lower()
        if dedupe_key in seen_deleted:
            continue
        seen_deleted.add(dedupe_key)
        normalized_deleted_paths.append(normalized_path)

    page_rows: list[WikiPage] = []
    if changed_page_ids is None:
        page_rows = list(
            WikiPage.objects.filter(
                scope=_WIKI_SCOPE_RESOURCE,
                resource_uuid=resource_uuid,
                is_draft=False,
            ).order_by("path")
        )
    else:
        resolved_ids = sorted({int(page_id) for page_id in changed_page_ids if int(page_id or 0) > 0})
        if resolved_ids:
            page_rows = list(
                WikiPage.objects.filter(
                    scope=_WIKI_SCOPE_RESOURCE,
                    resource_uuid=resource_uuid,
                    id__in=resolved_ids,
                )
            )
        expected = len(set(resolved_ids))
        if expected > len(page_rows):
            result["missing_pages"] += expected - len(page_rows)

    if not page_rows and not normalized_deleted_paths:
        return result

    worktree, branch, sync_error = _ensure_wiki_git_worktree(
        actor=actor,
        resource_uuid=resource_uuid,
        repository_full_name=repository_full_name,
        access_token=access_token,
    )
    if worktree is None:
        result["errors"] += 1
        result["error"] = sync_error or "wiki_git_unavailable"
        return result

    files_to_stage: list[str] = []
    for page in page_rows:
        page_path = _normalize_wiki_path(str(page.path or ""), "")
        if not page_path:
            continue
        if bool(page.is_draft):
            result["draft_skipped"] += 1
            if page_path.lower() not in seen_deleted:
                normalized_deleted_paths.append(page_path)
                seen_deleted.add(page_path.lower())
            continue

        wiki_filename = _wiki_filename_for_page_path(page_path)
        if not wiki_filename:
            continue
        absolute_file = worktree / wiki_filename
        absolute_file.parent.mkdir(parents=True, exist_ok=True)

        markdown = str(page.body_markdown or "")
        existing_markdown = ""
        if absolute_file.exists():
            try:
                existing_markdown = absolute_file.read_text(encoding="utf-8")
            except Exception:
                existing_markdown = ""
        if existing_markdown != markdown:
            try:
                absolute_file.write_text(markdown, encoding="utf-8")
            except Exception:
                result["errors"] += 1
                if not result.get("error"):
                    result["error"] = f"write_failed:{wiki_filename}"
                continue
            result["upserted"] += 1
            files_to_stage.append(wiki_filename)

    for page_path in normalized_deleted_paths:
        wiki_filename = _wiki_filename_for_page_path(page_path)
        if not wiki_filename:
            continue
        absolute_file = worktree / wiki_filename
        if absolute_file.exists():
            try:
                absolute_file.unlink()
            except Exception:
                result["errors"] += 1
                if not result.get("error"):
                    result["error"] = f"delete_failed:{wiki_filename}"
                continue
            result["deleted"] += 1
            files_to_stage.append(wiki_filename)

    if not files_to_stage:
        return result

    unique_stage = sorted({path for path in files_to_stage if path})
    add_code, _stdout, add_stderr = _run_git(args=["add", "--", *unique_stage], cwd=worktree)
    if add_code != 0:
        result["errors"] += 1
        result["error"] = f"git_add_failed:{(add_stderr or '').strip()[:240]}"
        return result

    status_code, status_stdout, status_stderr = _run_git(args=["status", "--porcelain"], cwd=worktree)
    if status_code != 0:
        result["errors"] += 1
        result["error"] = f"git_status_failed:{(status_stderr or '').strip()[:240]}"
        return result
    if not str(status_stdout or "").strip():
        return result

    actor_username = str(getattr(actor, "username", "") or "").strip() or "alshival"
    actor_email = str(getattr(actor, "email", "") or "").strip() or f"{actor_username}@alshival.local"
    commit_message = f"Sync resource wiki {resource_name} ({resource_uuid})"
    commit_code, commit_stdout, commit_stderr = _run_git(
        args=[
            "-c",
            f"user.name={actor_username}",
            "-c",
            f"user.email={actor_email}",
            "commit",
            "-m",
            commit_message,
        ],
        cwd=worktree,
    )
    commit_output = f"{commit_stdout}\n{commit_stderr}".lower()
    if commit_code != 0 and "nothing to commit" not in commit_output:
        result["errors"] += 1
        result["error"] = f"git_commit_failed:{(commit_stderr or commit_stdout).strip()[:240]}"
        return result

    push_target = branch or _current_git_branch(worktree) or "master"
    push_code, _push_stdout, push_stderr = _run_git(
        args=["push", "origin", f"HEAD:{push_target}"],
        cwd=worktree,
        access_token=access_token,
        timeout=150,
    )
    if push_code != 0:
        result["errors"] += 1
        push_detail = _sanitize_git_detail(push_stderr or "")
        classified = _classify_git_error(push_detail)
        result["error"] = classified or f"git_push_failed:{push_detail or 'unknown'}"
        return result

    return result


def sync_resource_wiki_with_github(
    *,
    actor,
    resource,
    token_users: Iterable[object] | None = None,
    pull_remote: bool = True,
    push_changes: bool = False,
    changed_page_ids: Iterable[int] | None = None,
    deleted_paths: Iterable[str] | None = None,
) -> dict[str, Any]:
    resource_uuid = _normalize_resource_uuid(getattr(resource, "resource_uuid", ""))
    resource_name = str(getattr(resource, "name", "") or "").strip() or resource_uuid
    repositories = resource_github_repository_names(resource)

    result: dict[str, Any] = {
        "ok": False,
        "code": "",
        "resource_uuid": resource_uuid,
        "resource_name": resource_name,
        "repository": "",
        "pull": {
            "remote_files": 0,
            "created": 0,
            "updated": 0,
            "unchanged": 0,
            "draft_skipped": 0,
            "errors": 0,
            "error": "",
        },
        "push": {
            "upserted": 0,
            "deleted": 0,
            "draft_skipped": 0,
            "missing_pages": 0,
            "errors": 0,
            "error": "",
        },
        "errors": [],
    }

    if not resource_uuid:
        result["code"] = "missing_resource_uuid"
        return result

    if not repositories:
        result["code"] = "missing_github_repositories"
        return result

    primary_repository = repositories[0]
    result["repository"] = primary_repository

    sync_users: list[object] = []
    if actor is not None:
        sync_users.append(actor)
    for item in token_users or []:
        if item is not None:
            sync_users.append(item)

    token_user, access_token, token_error = _resolve_access_token_from_users(sync_users)
    if not access_token:
        result["code"] = token_error or "missing_github_token"
        return result

    repo_context, repo_error = _github_repo_context(
        repository_full_name=primary_repository,
        access_token=access_token,
    )
    if repo_error:
        result["code"] = "github_repo_unavailable"
        result["errors"].append(repo_error)
        return result

    resolved_actor = actor if actor is not None else token_user
    if resolved_actor is None:
        result["code"] = "missing_actor"
        return result

    if pull_remote:
        pull_result = _pull_remote_wiki_into_local(
            actor=resolved_actor,
            resource_uuid=resource_uuid,
            resource_name=resource_name,
            repository_full_name=primary_repository,
            access_token=access_token,
        )
        result["pull"] = pull_result

    if push_changes:
        push_result = _push_local_wiki_to_remote(
            actor=resolved_actor,
            resource_uuid=resource_uuid,
            resource_name=resource_name,
            repository_full_name=primary_repository,
            access_token=access_token,
            changed_page_ids=changed_page_ids,
            deleted_paths=deleted_paths,
        )
        result["push"] = push_result

    pull_errors = int(result["pull"].get("errors", 0))
    push_errors = int(result["push"].get("errors", 0))
    if pull_errors or push_errors:
        result["code"] = "partial_error"
        pull_error_reason = str(result["pull"].get("error") or "").strip()
        push_error_reason = str(result["push"].get("error") or "").strip()
        if pull_errors:
            if pull_error_reason:
                result["errors"].append(f"pull:{pull_error_reason}")
            result["errors"].append(f"pull_errors:{pull_errors}")
        if push_errors:
            if push_error_reason:
                result["errors"].append(f"push:{push_error_reason}")
            result["errors"].append(f"push_errors:{push_errors}")
        result["ok"] = True
        return result

    result["ok"] = True
    result["code"] = "ok"
    return result
