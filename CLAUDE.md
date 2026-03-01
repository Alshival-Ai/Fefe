# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Multi-Developer Team Project

**This is a collaborative project.** Multiple developers and AI agents push to a single `main` branch on GitHub. If you are an AI agent picking up this file:

- **Always `git pull` before starting new work** — another developer may have landed changes since your last context.
- **Always read the file you are about to edit** — do not assume your in-memory knowledge matches the current file on disk.
- **Do not force-push to `main`.** Commit directly only for small isolated fixes; use a branch + PR for anything non-trivial.
- **Docker users: always pass `--build`** when bringing up the stack after a `git pull` so the container image is rebuilt with the latest code changes.
- The `./var` directory holds all runtime data (SQLite DBs, user files, knowledge base). It is bind-mounted and **not tracked in git**. Never run `docker volume prune` or `docker system prune --volumes` — this will destroy user data.

## Project Overview

**Alshival** is a Django-based infrastructure/resource management app with a built-in MCP server and optional Vite/React frontend. The repo root is `Fefe/` but the brand name is always `Alshival` — do not reintroduce the legacy `Fefe` name in user-facing text, defaults, or UI labels.

## Commands

### Local Development

```bash
# Setup
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver        # opens at /setup/ on fresh DB

# Django checks and migrations
python manage.py check
python manage.py makemigrations
python manage.py migrate

# Optional Vite frontend
cd frontend && npm install && npm run dev    # dev server on :5173
cd frontend && npm run build               # build to dashboard/static/frontend/
```

### Tests

```bash
python manage.py test dashboard.tests                          # all tests
python manage.py test dashboard.tests.test_reminders          # single module
```

### Background Workers (run separately)

```bash
python manage.py run_resource_health_worker
python manage.py run_reminder_worker --interval-seconds 60
python manage.py run_support_inbox_worker --interval-seconds 60
python manage.py run_global_api_key_worker
python manage.py run_user_api_key_worker
python manage.py sync_user_records_kb      # backfill user records into ChromaDB
python manage.py refresh_calendar_cache --provider asana
```

### MCP Server

```bash
uvicorn mcp.app:app --host 0.0.0.0 --port 8080
```

### Docker

```bash
docker compose up --build                                                   # production-style
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build  # fast dev loop (bind-mount + uvicorn --reload)
docker compose -f docker-compose.yml -f docker-compose-http.yml up --build # + nginx HTTP on :80
docker compose -f docker-compose.yml -f docker-compose-https.yml up --build # + nginx HTTPS on :443
./tools/prod.sh                                                             # HTTPS helper (auto-fetches certbot cert)
```

## Architecture

### Backend

- **Django project**: `alshival/` (settings, root URLs, middleware, ASGI)
- **Django app**: `dashboard/` (all app logic: models, views, URLs, static, templates, management commands)
- **Auth**: `django-allauth` — login at `/accounts/login/`, Microsoft and GitHub OAuth supported
- **Global login gate**: `alshival/middleware.py` — all routes require login
- **User folder creation**: `dashboard/signals.py` — triggered on login

### Key Modules in `dashboard/`

| File | Purpose |
|---|---|
| `views.py` | All HTTP view handlers (large file) |
| `urls.py` | App URL patterns (user, team, resource, wiki, calendar, search, alert routes) |
| `models.py` | Django ORM models (`ResourceRouteAlias`, `ResourcePackageOwner`, etc.) |
| `resources_store.py` | Per-user/team/global resource CRUD and package transfers |
| `health.py` | Resource health check logic and transition cloud log emission |
| `reminder_service.py` | Reminder dispatch (APP/SMS/EMAIL) via worker |
| `calendar_sync_service.py` | Calendar cache refresh orchestration (Asana first, extensible) |
| `knowledge_store.py` | ChromaDB knowledge base operations |
| `global_api_key_store.py` | Global API key management |
| `request_auth.py` | MCP/API key auth resolution |
| `web_terminal.py` | Ask Alshival shell session logic |
| `wiki_markdown.py` | Workspace and resource wiki markdown handling |

### Data Storage

- **Main SQLite DB**: `var/db.sqlite3`
- **Per-user SQLite** (`member.db`): `var/user_data/<slug-username>-<id>/member.db`
  - Contains: `ask_chat_messages`, `reminders`, `calendar_event_cache`, `calendar_sync_state`, `asana_task_cache`, `asana_board/task_resource_map`, per-user alert filter prompt
- **User home**: `var/user_data/<slug-username>-<id>/home/` (Ask Alshival shell CWD)
- **Resource packages**: `var/user_data/<user>/resources/<uuid>/` (user), `var/team_data/<team>/resources/<uuid>/` (team), `var/global_data/resources/<uuid>/` (global)
  - Each package contains `resource.db` (health data, alert settings, logs, API keys)
- **Knowledge base** (ChromaDB): `var/user_data/<user>/home/.alshival/knowledge.db`, `var/global_data/knowledge.db`
- **Do not run `docker volume prune` or `docker system prune --volumes`** — app data lives in bind-mounted `./var`

### MCP Server (`mcp/app.py`)

FastAPI + MCP SDK server. Auth via `x-api-key` header (global/account/resource keys) or `Authorization: Bearer`. Identity resolved from `x-user-username`/`x-user-email`/`x-user-phone` headers. Twilio signature fallback for SMS callers. Tools: `search_kb`, `resource_kb`, `resource_health_check`, `resource_logs`, `resource_ssh_exec`, `alert_filter_prompt`, `search_users`, `directory`, `sms`, `calendar_context`, `asana_calendar`, `asana_get_subtasks`, `asana_list_sections`, `asana_move_task_to_section`, `asana_update_assignee`, `asana_list_workspace_members`, `asana_get_dependencies`, `asana_add_dependency`, `asana_remove_dependency`, `asana_get_project_status`, `asana_get_attachments`.

### Frontend

- **Primary UI**: Django templates in `dashboard/templates/`
  - Base: `base.html` (document title, favicon, loader logo, topbar script inclusion)
  - Layout: `vertical.html`
  - Partials: `partials/` (sidenav, topbar, notifications)
- **Static**: `dashboard/static/css/app.css` (all custom styles), `dashboard/static/js/` (per-page scripts)
- **Vite/React** (optional): `frontend/` → builds to `dashboard/static/frontend/`. Template tags: `dashboard/templatetags/vite.py`

### Key Routes

- `/` — Overview (home)
- `/resources/` — Resource monitor and watchlist
- `/u/<username>/resources/<uuid>/` — User-scoped resource detail
- `/team/<team_name>/resources/<uuid>/` — Team-scoped resource detail (canonical redirect from old aliases)
- `/wiki/` — Workspace wiki
- `/search/kb/` — Topbar KB search suggestions API
- `/calendar/cache/refresh/` — Calendar cache refresh endpoint
- `/accounts/login/` — Login
- `/setup/` — First-run setup (creates admin user + connector config)

## Architectural Rules

### Agent-Gated Alert Delivery
Every outbound alert (app notification, SMS, email) must be processed by the alert-filter agent before dispatch. Channel toggles are a hard gate; the agent only decides within enabled channels. New alert paths must call the agent gate before sending.

### Alert/Reminder Context Logging
Every outbound alert/reminder attempt (sent or failed) must be logged into the recipient user's `member.db` context stream as `ask_chat_messages` rows with `message_kind='context_event'`. Include channel, target, reminder/alert id, and outcome.

### Resource Route Canonicalization
Resources have one current canonical route alias (`ResourceRouteAlias`). Old aliases are preserved and redirect to the current canonical URL. Both `/u/<username>/resources/<uuid>/` and `/team/<team_name>/resources/<uuid>/` patterns are supported.

### Knowledge Base Deduplication
Public workspace wiki pages indexed into global ChromaDB are excluded from DB fallback wiki search to avoid duplicate results (enforced in `views.py::_tool_search_kb_for_actor` and `mcp/app.py`).

### Branding
- Always `Alshival` (capital A). Never reintroduce `Fefe` in user-facing text.
- Wordmark asset: `dashboard/static/img/branding/alshival-logo-276x186.png`
- Icon/favicon: `dashboard/static/img/branding/alshival-logo-256x256.png`
