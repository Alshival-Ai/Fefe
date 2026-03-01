# Alshival App Technical Details

## Multi-Developer Team Notice

This codebase is actively developed by multiple humans and AI agents pushing to a single `main` branch on GitHub. If you are an AI agent reading this file:

- **Pull before you edit.** Run `git pull` before starting any task.
- **Read files before editing them.** Do not rely on context from a prior session — files may have changed.
- **Check `AGENTS/DEV.md`** for the current development workflow and recent implementation notes.
- **Docker users:** always pass `--build` after `git pull` to rebuild the container image with the latest changes.
- **Never** run `docker volume prune` or `docker system prune --volumes` — all app data lives in the bind-mounted `./var` directory.

## Rule #1: Agent-Processed Outbound Alerts
- Every outbound alert delivery decision must be processed by an agent before sending.
- Applies to all alert channels: app notifications, SMS, and email.
- Channel toggles remain a hard gate; the agent only decides within enabled channels.
- New alert paths must call the alert-filter agent gate before dispatch.

## Rule #2: Outbound Alert/Reminder Context Logging
- Every outbound alert/reminder attempt (send or failure) must be logged into the user's `member.db` context stream.
- Context events are stored in `ask_chat_messages` as `message_kind='context_event'`.
- Include channel, target, reminder/alert id, and outcome (sent/failed + error details when available).
- This applies to app/SMS/email delivery paths and reminder lifecycle events (created/updated/deleted/dispatched).

## Stack Overview
- Backend: Django (project `alshival`, app `dashboard`)
- Auth: django-allauth (login at `/accounts/login/`)
- Database: SQLite (default Django DB: `var/db.sqlite3`)
- User data storage: per-user SQLite database (`member.db`) under `var/user_data/<slug-username>-<id>/`
- Frontend (optional): Vite + React embedded into Django templates

## Backend Details
- Project settings: `alshival/settings.py`
- URLs: `alshival/urls.py`
- App: `dashboard/`
- Login required globally via `alshival/middleware.py`
- User folder creation on login via `dashboard/signals.py`
- Watchlist storage: `dashboard/watchlist.py`

## Frontend Details
- Base templates: `dashboard/templates/base.html` and `dashboard/templates/vertical.html`
- Sidebar/topbar partials in `dashboard/templates/partials/`
- Matrix background effect scoped to sidebar
- Vite React app (optional) in `frontend/`
  - Vite dev server: `http://localhost:5173`
  - Build outputs to `dashboard/static/frontend/`
  - Django template tags for Vite: `dashboard/templatetags/vite.py`

## Setup and Installation
1. Create venv: `python3.13 -m venv .venv`
2. Activate venv: `source .venv/bin/activate`
3. Install deps: `pip install -r requirements.txt`
4. Configure `.env` keys:
   - `HOST`
   - `PORT`
   - `APP_BASE_URL`
   - `ALLOWED_HOSTS`
   - `CSRF_TRUSTED_ORIGINS`
   - `ALSHIVAL_SSH_KEY_MASTER_KEYS`
5. Run migrations: `python manage.py migrate`
6. Start server: `python manage.py runserver`
7. First-run setup route: `/setup/`

## Running
- Django: `python manage.py runserver`
- React (dev): `cd frontend && npm install && npm run dev`
- React (build): `cd frontend && npm run build`
- Reminder worker: `python manage.py run_reminder_worker --interval-seconds 60`
- Support inbox worker: `python manage.py run_support_inbox_worker --interval-seconds 60`
- GitHub wiki sync worker: `python manage.py run_github_wiki_sync_worker --interval-seconds 3600`

## Routine Docker Build Cleanup (Safe)
- Goal: reclaim Docker build/cache/image garbage without deleting app user data in `var/`.
- Run from project root (`/home/data-team/Fefe`) and keep the stack running.
- Recommended cadence: weekly, or any time `docker system df` shows large build cache.

1. Check current usage:
   - `docker system df`
   - `du -sh var`
2. Prune build cache (primary cleanup):
   - `docker builder prune -af`
3. Prune unused Docker objects that do not affect mounted app data:
   - `docker container prune -f`
   - `docker image prune -af`
   - `docker network prune -f`
4. Verify results:
   - `docker system df`
   - `du -sh var`
   - `docker ps`

### Data Safety Rules
- Do not run `docker volume prune` on this host.
- Do not run `docker system prune --volumes`.
- App data is stored under bind-mounted `./var` and must be preserved.

## Key Routes
- `/` Overview
- `/resources/` Resource monitor and watchlist
- `/accounts/login/` Login (Allauth)
- `/setup/` Two-step initial setup flow
- `/accounts/microsoft/login/` Microsoft OAuth login (allauth)
- `/accounts/github/login/` GitHub OAuth login (allauth)
- `/u/<username>/resources/<uuid>/` User-scoped resource detail route
- `/team/<team_name>/resources/<uuid>/` Team-scoped resource detail route
- `/u/<username>/resources/<uuid>/wiki/sync/` Manual resource wiki sync (POST)
- `/team/<team_name>/resources/<uuid>/wiki/sync/` Manual team resource wiki sync (POST)

## Initial Setup Flow
- Setup UI is implemented in `dashboard/templates/pages/setup_welcome.html`.
- Step 1 creates the initial admin user:
  - Required: `admin_username`, `admin_password`, `admin_password_confirm`
  - No admin email field in current flow
  - Username must match GitHub-style constraints (validated client + server side)
- Step 2 handles optional connector configuration:
  - OpenAI: API key input
  - Microsoft Entra: tenant/client/secret + test sign-in CTA
  - GitHub OAuth: client/secret + test sign-in CTA
  - Google/Anthropic: placeholder tiles only (coming soon)
- Setup backend logic is in `dashboard/views.py::setup_welcome`.

## Connector Persistence
- OAuth provider settings are persisted via `allauth.socialaccount.models.SocialApp`.
- Social apps are created/updated and attached to current `Site` (`SITE_ID`).
- Microsoft:
  - Provider id: `microsoft`
  - Saved settings include `tenant` in `SocialApp.settings`
  - Test flow action: `setup_action=test_microsoft` redirects to Microsoft login
- GitHub:
  - Provider id: `github`
  - Saved settings include sign-in scopes in `SocialApp.settings` (`read:user`, `user:email`, `read:org`, `repo`)
  - `repo` scope is required for GitHub wiki read/write sync operations.
  - Test flow action: `setup_action=test_github` redirects to GitHub login

## Domain and URL Behavior
- `APP_BASE_URL` is used to generate connector callback guidance during setup.
- Callback hints shown in UI:
  - Microsoft: `/accounts/microsoft/login/callback/`
  - GitHub: `/accounts/github/login/callback/`
- `APP_BASE_URL` also auto-augments:
  - `ALLOWED_HOSTS` (hostname)
  - `CSRF_TRUSTED_ORIGINS` (scheme + host origin)

## Branding Guidelines
- Brand name: always use `Alshival` (capital A, lowercase remainder).
- Wordmark style: the stylized `Alshival` wordmark (provided by logo assets) is the primary brand mark.
- Preferred logo asset for UI wordmark placements: `dashboard/static/img/branding/alshival-logo-276x186.png`.
- Icon/square logo asset for compact contexts (favicon, small chips): `dashboard/static/img/branding/alshival-logo-256x256.png`.
- Large/source logo asset: `dashboard/static/img/branding/alshival-logo-1536x1024.png`.
- Do not reintroduce legacy `Fefe` naming in user-facing text, defaults, email templates, or UI labels.
- Do not use text-only fallback initials (`F`/`A`) where the logo image is already used in templates.
- Current primary brand touchpoints in Django templates:
  - `dashboard/templates/base.html` (document title, favicon, loader logo)
  - `dashboard/templates/partials/topbar.html` (topbar logo + wordmark)
  - `dashboard/templates/account/login.html` (auth panel wordmark)
  - `dashboard/templates/pages/setup_welcome.html` (setup panel wordmark)

## UI Guidelines
- Gradient jiggle button pattern (used for setup `Next` and `Complete setup`) should reuse:
  - HTML classes: `setup-next-btn`, `setup-next-btn__label`
  - Jiggle state class: `is-jiggling`
  - Keyframe: `chatbot-jiggle`
- Canonical implementation locations:
  - Button markup + jiggle scheduler JS: `dashboard/templates/pages/setup_welcome.html`
  - Button styling + animation keyframes: `dashboard/static/css/app.css`
- Standard button markup pattern:
  - `<button class="primary-btn setup-next-btn" ...><span class="setup-next-btn__label">...</span></button>`
- Behavior rules:
  - Keep frosted dark rainbow gradient background and white text.
  - Respect reduced-motion via `@media (prefers-reduced-motion: reduce)` by disabling jiggle animation.
  - Use the same visual treatment for multi-step primary CTAs to maintain consistency.

## Notes
- `USER_DATA_ROOT` in settings controls where per-user data lives (default `var/user_data/`).
- `STATIC_ROOT` defaults to `var/staticfiles/` for `collectstatic`.

## Recent Implementation Details (2026-02)

### Reminder System + Intra-Team Communication
- Reminder storage is persisted per user in `member.db` (`reminders` table) through `dashboard/resources_store.py`.
- Ask tools now include:
  - `set_reminder`
  - `edit_reminder`
  - `delete_reminder`
  - `list_reminders`
- Recipient guardrails:
  - Reminder recipients must be the actor or users in actor contact scope (shared team membership unless superuser).
  - Invalid/out-of-scope recipients are rejected at tool level.
- Worker:
  - `dashboard/management/commands/run_reminder_worker.py`
  - Service logic in `dashboard/reminder_service.py`
  - Processes due reminders every minute (default), dispatching APP/SMS/EMAIL by channel flags.
- Final reminder content generation:
  - Uses an agent-generated final message per channel.
  - SMS is short-form.
  - Email can include richer dossier context (resource links + workspace wiki links) when available.
- Email delivery for reminders:
  - Uses support inbox app-send flow (`send_support_inbox_email`) when support inbox monitoring is enabled/configured.
- Context logging:
  - Reminder lifecycle and delivery events are written via `add_ask_chat_context_event(...)` to `member.db` chat context.

### Alert Context Event Logging
- Health/cloud-log/calendar alert dispatch paths now log context events in the recipient user's `member.db`.
- Success and failure events are both logged with structured payloads so downstream chat agents can reference alert history.

### Calendar Cache Service (Asana First, Provider-Extensible)
- User calendar cache is persisted in each user's `member.db` (per-user SQLite):
  - `calendar_event_cache`: normalized event/task rows by provider.
  - `calendar_sync_state`: provider sync status, count, and last refresh epoch.
  - Existing `asana_task_cache` remains as provider raw payload cache.
- Dedicated orchestration module:
  - `dashboard/calendar_sync_service.py::refresh_calendar_cache_for_user`
  - `DEFAULT_CALENDAR_REFRESH_MIN_INTERVAL_SECONDS=60` throttle guard via `calendar_sync_state`.
- Current Asana refresh/write path:
  - `dashboard/views.py::_asana_overview_context_for_user(..., force_refresh=...)`
  - `dashboard/views.py::_write_asana_calendar_cache`
  - `dashboard/resources_store.py::replace_user_calendar_event_cache`
- Current read/update helpers:
  - `dashboard/resources_store.py::list_user_calendar_event_cache`
  - `dashboard/resources_store.py::update_user_calendar_event_completion`
- Service endpoints:
  - `POST /calendar/cache/refresh/` (`provider=asana|all`)
  - `POST /calendar/asana/tasks/<task_gid>/complete/`
- Background refresh command:
  - `python manage.py refresh_calendar_cache --provider asana`
  - Optional: `--username <username>`
- Overview calendar integration:
  - Asana tasks with due dates are mapped into planner items and rendered in agenda/calendar.
  - Checkbox completion in planner updates Asana and local calendar cache state.
- MCP agent context tool:
  - Tool name: `calendar_context`
  - File: `mcp/app.py`
  - Purpose: query unified per-user `calendar_event_cache` across providers (`asana`, future `outlook`, `gmail`, etc.).
  - Behavior: performs a live cache refresh on call by default (`refresh=true`) for supported providers, throttled by service min interval, then reads unified cache.
  - Supports filtering by provider/date/query/status and returns:
    - provider/date summary counts
    - concise `context_lines` for direct agent grounding
    - full filtered rows for deeper reasoning

### Outlook Integration Notes (Next)
- Reuse `calendar_event_cache` with `provider='outlook'` (or `microsoft_outlook` if we want explicit namespace).
- Add refresh adapter in `dashboard/calendar_sync_service.py` similar to Asana branch:
  - evaluate throttle using `get_user_calendar_sync_state(user, provider='outlook')`
  - fetch provider events
  - normalize rows to shared schema:
    - `event_id`, `title`, `due_date`, `due_time`, `is_completed`, `status`, `source_url`, `payload_json`
  - write via `replace_user_calendar_event_cache(user, provider='outlook', ...)`
- Extend MCP `calendar_context` `refreshable_providers` set to include `outlook`.
- Keep provider payload details in `payload` for agent context (organizer, attendees, location, recurrence, etc.).

### Resource Details Page
- The `Resource Details` view now provides:
  - `resource_url`: absolute URL to the current resource detail page.
  - `resource_env_value`: `ALSHIVAL_RESOURCE=<absolute-resource-url>`.
- Resource details template updates:
  - Overview panel now contains health graph + health check action.
  - Removed duplicated "Current status" text block and "Resource ID" from overview metadata.
  - "Virtual machine" details card removed from the page layout.
  - Notes card uses Team Comments-style author row with avatar support.
  - Resource API Keys card is positioned in the main details grid next to Notes.
  - Cloud Logs card is rendered as a full-width section.
  - Header includes click-to-copy `ALSHIVAL_RESOURCE=<url>`.
  - Header includes an Alerts icon button that opens the persisted alert settings modal.

### Notes / Team Comments Styling
- Notes now show:
  - Social avatar image when available (from `allauth` social account `extra_data`).
  - Initial-based fallback avatar when no image is available.
- Note attachment filename text is hidden unless a file is selected (no "No file selected." placeholder text).

### Resource Alerts Settings (Persisted)
- Resource details alert settings are persisted in each resource package DB (`resource.db`) table `resource_alert_settings`.
- Settings are per-user-per-resource and currently support:
  - Health Alerts: App/SMS/Email
  - Cloud Log Errors: App/SMS/Email
- Default values are:
  - App: enabled (`true`)
  - SMS: disabled (`false`)
  - Email: disabled (`false`)
- Backend write path:
  - `dashboard/views.py::update_resource_alert_settings`
  - `dashboard/resources_store.py::upsert_resource_alert_settings`

### Overview Agenda Resource Mapping UX
- Scope is currently Overview page only (`/`); do not assume this UX is enabled on other planner pages yet.
- In Agenda section task cards, a `Resources` badge is shown in the task row header (right side).
- Clicking `Resources` opens the agenda resource mapping modal (same `attach-resources` action path).
- The modal displays resources the user can access across personal/team/global scope (from the Overview resource options payload).
- Mapping persistence remains via `update_overview_agenda_item_resource_mapping` and existing Asana/agenda mapping flows in `dashboard/static/js/home-overview.js`.
- Shared planner badge rendering support lives in `dashboard/static/js/planner-ui.js` and styling in `dashboard/static/css/app.css`.

### Resource Route Aliases and Canonical Forwarding
- Resource routes now support alias history via `dashboard.models.ResourceRouteAlias`.
- A resource can be addressed by either:
  - user route `/u/<username>/resources/<uuid>/...`
  - team route `/team/<team_name>/resources/<uuid>/...`
- Canonical route behavior:
  - Every resource has one current alias (`is_current=1`).
  - Older aliases are preserved.
  - Requests to old aliases resolve and are redirected to the current canonical detail route.
- Route resolution/wiring:
  - `dashboard/views.py::_resolve_resource_route_context`
  - Team wrappers: `team_resource_detail`, `team_resource_note_add`, `team_resource_logs_ingest`, etc.
  - URL patterns in `dashboard/urls.py` for both `/u/...` and `/team/...` paths.

### Resource Package Ownership / Asset Transfer
- Resource package ownership is tracked in `dashboard.models.ResourcePackageOwner`.
- Package data is stored in owner-scoped roots:
  - User scope: `var/user_data/<user-slug-id>/resources/<resource_uuid>/`
  - Team scope: `var/team_data/<team-slug-id>/resources/<resource_uuid>/`
  - Global scope: `var/global_data/resources/<resource_uuid>/`
- Ownership transfer and filesystem moves are handled by:
  - `dashboard/resources_store.py::transfer_resource_package`
- Transfers move the package directory and update owner metadata; route aliases keep old routes resolving.

### Health Worker + Cloud Log Transition Events
- Worker command: `python manage.py run_resource_health_worker`
- Discovery behavior:
  - Iterates active users, collects resources via `list_resources(user)`, dedupes by `resource_uuid`.
  - This includes team/global-owned packages because ownership resolution happens in `resources_store`.
- Status transition cloud logs:
  - On `healthy -> unhealthy` and `unhealthy -> healthy`, the worker writes a resource cloud log entry.
  - Implemented in `dashboard/health.py::_log_health_transition` and triggered by `emit_transition_log=True`.
  - Log metadata includes source (`run_resource_health_worker`), method, target, previous/current status, latency, packet loss, and error.

### Team + Shared Resource KB Fanout (Alpha Temporary Behavior)
- `search_kb` reads from the actor's personal KB (`var/user_data/<user>/knowledge.db`) plus global KB; there is no active team KB query path.
- Resource health knowledge is always upserted to the resource package-local KB (`.../resources/<resource_uuid>/knowledge.db`) first.
- Fanout to member personal KBs happens when resource access is team-shared:
  - team-owned resources (`owner_scope=team`) fan out to all active members of the owner team.
  - user-owned resources with `ResourceTeamShare` fan out to all active users in the shared teams.
- Non-shared user-owned resources remain in the owner's personal KB path.
- Team `knowledge.db` stores are inactive in this mode and are pruned during knowledge cleanup.
- Cleanup keeps per-user shared entries by including both:
  - team-owned resources from the user's team memberships.
  - resources shared to the user's teams via `ResourceTeamShare`.
- Known tradeoff: duplicated vectors/documents across member KBs increase storage.
- TODO: move to non-duplicated team-shared retrieval/federated search and remove per-member fanout duplication.

### Resource API Keys Card Behavior
- API keys list now stretches to fill available card space:
  - Card is a vertical flex container.
  - Key list is scrollable and occupies remaining card height.

### Terminal (Ask Alshival) Local Shell Behavior
- Superuser `Ask Alshival` shell sessions now resolve to a per-authenticated-user home directory by default:
  - Path pattern: `<USER_DATA_ROOT>/<slug-username>-<user_id>/home`.
  - Home directory is created on first launch.
- Shell mode selection is local-first by default; host shell mode is opt-in via:
  - `WEB_TERMINAL_PREFER_HOST_SHELL=1` (or `true/yes/on`).
- Shell launch changed to avoid login-profile overrides:
  - Uses `bash --noprofile --norc -i` when bash is available.
- Static local identity env overrides (`WEB_TERMINAL_LOCAL_USERNAME` + `WEB_TERMINAL_LOCAL_HOME`) are only used when:
  - `WEB_TERMINAL_FORCE_STATIC_IDENTITY=1` (or `true/yes/on`).

### Wiki -> ChromaDB Synchronization
- Workspace wiki and resource wiki are indexed differently in ChromaDB.

#### Global Workspace Wiki (Per-Page Records)
- A workspace page is indexed into global KB only when all are true:
  - `scope=workspace`
  - `resource_uuid=""` (not resource-scoped)
  - `is_draft=false`
  - no team access entries (public workspace page)
- Global KB target path:
  - `var/global_data/knowledge.db` collection `resources`
- Record shape:
  - `id = workspace_wiki:<wiki_page_id>`
  - `document = "<title> | <path> | <body_markdown>"`
  - metadata includes `source=workspace_wiki`, `wiki_page_id`, `title`, `path`, `owner_scope=global`, and timestamps.
- Sync hooks (create/edit/delete):
  - `dashboard/views.py::_sync_global_workspace_wiki_kb_page`
  - Called from `wiki_create_page`, `wiki_update_page`, and `wiki_delete_page`.
- Delete behavior:
  - On workspace wiki delete, `workspace_wiki:<id>` is explicitly deleted from global Chroma before DB row removal.
  - On edit, if a page is no longer globally indexable (draft/team-scoped/resource-scoped), its global Chroma record is removed.

#### Resource Wiki (Per-Resource Records)
- Resource wiki is not stored as one Chroma record per wiki page.
- It is embedded inside the resource knowledge document (`resource_document_json.resource_wiki_pages`) and flattened into the resource `document` text.
- Resource record id remains:
  - `id = <resource_uuid>`
- Sync hooks (create/edit/delete):
  - `dashboard/views.py::_upsert_resource_kb_after_wiki_mutation`
  - Called from resource wiki and resource-scoped wiki create/update/delete flows.
- Delete behavior:
  - Deleting a resource wiki page triggers a re-upsert of the resource record so removed page content is no longer in `document`/`resource_document_json`.
  - The resource record itself is only removed when the resource is removed and cleanup prunes stale IDs.

#### Search Deduping Behavior
- Public published workspace wiki pages indexed into global Chroma are excluded from DB fallback wiki search to avoid duplicate results.
- This exclusion exists in both:
  - `dashboard/views.py::_tool_search_kb_for_actor`
  - `mcp/app.py::_workspace_wiki_results_for_actor`

### Resource Wiki <-> GitHub Wiki Sync (2026-03)
- Resource wiki pages can be linked to GitHub repositories via `resource_metadata.github_repositories` (multi-select on resource add/edit forms).
- Sync service:
  - `dashboard/github_wiki_sync_service.py`
  - Entry point: `sync_resource_wiki_with_github(...)`
- Repository selection behavior:
  - Current sync uses the first linked repository as the canonical wiki upstream.
  - GitHub wiki remote target is `https://github.com/<owner>/<repo>.wiki.git`.
- Transport behavior:
  - Pull and push use git operations (`clone`, `fetch`, `pull --ff-only`, `push`), not REST `repos/<repo>.wiki` content endpoints.
  - This requires `git` in the app image (`Dockerfile` installs it for web + worker services).
- Automatic push on front-end wiki mutations:
  - `resource_wiki_create_page` (published pages only)
  - `resource_wiki_update_page` (handles publish, draft transitions, and path renames)
  - `resource_wiki_delete_page`
  - All three keep local save/delete as source-of-truth and show warnings if remote sync fails.
- Manual sync:
  - UI button: `Sync Wiki` in `dashboard/templates/pages/wiki.html`
  - Endpoints:
    - `POST /u/<username>/resources/<uuid>/wiki/sync/`
    - `POST /team/<team_name>/resources/<uuid>/wiki/sync/`
  - Behavior: pull remote markdown pages into local `WikiPage` rows, then push local published pages back to GitHub wiki.
- Hourly background pull worker:
  - Command: `python manage.py run_github_wiki_sync_worker --interval-seconds 3600`
  - File: `dashboard/management/commands/run_github_wiki_sync_worker.py`
  - Default mode pulls remote GitHub wiki content into local resource wiki cache (`push_changes=False`).
- Compose services:
  - `docker-compose.yml`: `github-wiki-worker`
  - `docker-compose.dev.yml`: `github-wiki-worker`

### User Records in Global Chroma
- Dedicated collection:
  - Global path: `var/global_data/knowledge.db`
  - Collection name: `user_records`
- Record identity:
  - `id = user:<user_id>`
- Document content includes searchable user profile tokens:
  - username, email, full name, normalized phone, status/role flags, team names, feature keys
- Metadata includes:
  - `user_id`, `username`, `email`, `phone_number`, `phone_digits`
  - `is_active`, `is_staff`, `is_superuser`
  - `team_names`, `feature_keys`, `updated_at`
- Sync lifecycle (signal-based):
  - User save/delete (`AUTH_USER_MODEL`)
  - `UserNotificationSettings` save/delete (phone changes)
  - `UserFeatureAccess` save/delete
  - user/group membership changes (`m2m_changed` on user groups)
- Backfill command:
  - `python manage.py sync_user_records_kb`
  - Reindexes all users into `user_records`
- Agent tools:
  - MCP server tool: `search_users(query?, phone?, limit?)` (superuser-only)
  - Ask Alshival tool: `search_users(query?, phone?, limit?)` (superuser-only)

### Topbar KB Search Suggestions (Type-Aware Routing)
- A topbar suggestion API is exposed at:
  - `GET /search/kb/` (`dashboard/urls.py`, `dashboard/views.py::search_kb_suggestions`)
- Response shape:
  - `{ ok, query, result_count, results[] }`
  - each `results[]` item includes `kind`, `title`, `subtitle`, `snippet`, `url`.
- Data source:
  - suggestions are built from `_tool_search_kb_for_actor` results and normalized by `_build_topbar_kb_suggestions`.
- Type routing behavior:
  - Workspace wiki results (`source=workspace_wiki`) resolve to `/wiki/?page=<path>`.
  - Resource wiki-shaped results resolve to canonical resource wiki routes via `dashboard/views.py::_resource_wiki_url_for_uuid`.
  - Resource results resolve to canonical resource detail routes via `dashboard/views.py::_resource_detail_url_for_uuid`.
  - Unknown/unroutable results are skipped from topbar suggestions.
- Resource-context enrichment:
  - When topbar receives `context_resource_uuid` (set on resource routes), search prepends resource-scoped KB + resource wiki matches from `_resource_context_kb_rows_for_actor`.
  - This is wired by `data-resource-uuid` in `dashboard/templates/partials/topbar.html` and sent by `dashboard/static/js/topbar-search.js`.
- Frontend wiring:
  - Template hook: `dashboard/templates/partials/topbar.html` (`data-topbar-search*` attributes).
  - Script: `dashboard/static/js/topbar-search.js` (debounced fetch, keyboard navigation, outside-click close, enter-to-open).
  - Styles: `dashboard/static/css/app.css` (`.topbar-search-*` classes).
  - Script inclusion: `dashboard/templates/base.html`.

### Resource Detail Health Chart Mode Selection
- Resource detail sends chart records (`status`, `checked_at`, `check_method`, `latency_ms`) from:
  - `dashboard/views.py::resource_detail` (`health_history_chart` payload).
- Client chart logic in `dashboard/templates/pages/resource_detail.html` dynamically chooses chart mode:
  - Latency mode when latency values are available.
  - Status timeline mode when latency is unavailable (for fallback-only check methods).
- Legend/method behavior:
  - Method summary is derived from `check_method` and labels `ping+<fallback>` checks explicitly.
  - Fallback-assisted ping points are colorized separately to make fallback usage visible in the graph.

### Resource Wiki Nav Highlighting Behavior
- Sidebar active-state logic is intentionally split:
  - `Resources` stays active for any route containing `/resources/`.
  - `Wiki` is active only for `/wiki/` routes that are not resource-scoped.
- Implementation location:
  - `dashboard/templates/partials/sidenav.html`
- This prevents the resource wiki pages from highlighting both nav items at the same time.

### Sidebar Workspace Pulse Widget (Replaces Placeholder Focus Card)
- The placeholder "Today's focus" card is replaced with a live "Workspace Pulse" widget in sidebar.
- Data provider:
  - `dashboard/context_processors.py::sidebar_workspace_widget`
  - registered in template context processors via `alshival/settings.py`.
- Exposed metrics:
  - `resources_total`, `resources_healthy`, `resources_unhealthy`, `resources_unknown`, `resources_attention`, `unread_notifications`, `team_count`.
- UI locations:
  - Markup/actions: `dashboard/templates/partials/sidenav.html`
  - Styling: `dashboard/static/css/app.css` (`.sidenav-widget-*` classes)

### Asana Calendar/Agenda Upgrades (2026-02)
- Goal:
  - unify Asana planning signals in Calendar UI, support safer task recovery (uncheck recently completed), and map Asana work to resources for resource-scoped planning.

#### Agenda Completed Task Window
- Overview/Resource planner agenda sections now include:
  - all open Asana tasks
  - completed Asana tasks where `completed_at` is within a rolling 14-day window.
- Window source of truth:
  - backend constant: `dashboard/views.py::_ASANA_AGENDA_COMPLETED_WINDOW_DAYS = 14`
  - exposed to templates as `asana_completed_window_days`
  - consumed by front-end via `data-asana-completed-window-days`.
- Completion state persistence:
  - Asana API fetch now requests `completed_at` in task `opt_fields`.
  - Toggle completion endpoint writes `completed_at` on complete and clears it on uncomplete.
  - Cache freshness check now requires `completed_at` field presence in cached task rows.

#### Asana Comments (Read + Reply from Calendar)
- New endpoints:
  - `GET /calendar/asana/tasks/<task_gid>/comments/`
  - `POST /calendar/asana/tasks/<task_gid>/comments/add/`
- View functions:
  - `dashboard/views.py::list_asana_task_comments`
  - `dashboard/views.py::add_asana_task_comment`
- Behavior:
  - comments are read from Asana task stories (`/tasks/<gid>/stories`) filtered to comment-type stories.
  - replies post back to Asana stories API (`text` payload).
  - token refresh retry path is reused for auth-expired errors.
- UI:
  - planner section rows expose `Comments` action.
  - modal displays timeline and supports inline reply.

#### Asana Resource Mapping Model
- Per-user mapping tables in `member.db`:
  - `asana_board_resource_map(board_gid, resource_uuid, created_at, updated_at)`
  - `asana_task_resource_map(task_gid, resource_uuid, created_at, updated_at)`
- Store helpers (`dashboard/resources_store.py`):
  - `list_user_asana_board_resource_mappings`
  - `list_user_asana_task_resource_mappings`
  - `set_user_asana_board_resource_mapping`
  - `set_user_asana_task_resource_mapping`
- Mapping semantics:
  - board-level mappings apply to all tasks in that board.
  - task-level mappings are additive (additional resources per task).
  - final task resource set = task-level mappings UNION board-level mappings across task memberships.

#### Asana Mapping Endpoints
- New endpoints:
  - `POST /calendar/asana/boards/<board_gid>/resources/update/`
  - `POST /calendar/asana/tasks/<task_gid>/resources/update/`
- View functions:
  - `dashboard/views.py::update_asana_board_resource_mapping`
  - `dashboard/views.py::update_asana_task_resource_mapping`
- Authorization/scope:
  - requested `resource_uuids` are filtered against resources accessible to current user via `_asana_resource_options_for_user` (built from `_wiki_resource_options_for_user`).

#### Planner UI Action Framework
- Core planner now supports section-row actions:
  - action data shape on section items: `actions: [{id, label}]`.
  - callbacks:
    - `onAgendaSectionAction(item, actionId, section)`
    - `onActionError(item, actionId, error)`
- Implementation:
  - `dashboard/static/js/planner-ui.js`
  - action buttons rendered per section row and dispatched through delegated click handling.

#### Overview Calendar Wiring
- Template wiring:
  - `dashboard/templates/pages/home.html`
  - adds Asana URL templates and mapping/task JSON blobs.
- Script:
  - `dashboard/static/js/home-overview.js`
- Behavior:
  - agenda section buckets by Asana board (`Asana - <Board Name>`).
  - section rows include actions: `Comments`, `Resources`.
  - Asana completion toggle remains two-way and updates local planner state.
  - connector failure path retains popup guidance to update directly in Asana board/task.

#### Resource Detail Planner Wiring
- Template wiring:
  - `dashboard/templates/pages/resource_detail.html`
  - planner root now includes Asana URL templates + resource uuid + completed window data attrs.
  - ships resource-specific JSON payloads for tasks/mappings/options.
- Script:
  - `dashboard/static/js/resource-planner.js` (new)
- Behavior:
  - resource planner shows only Asana tasks mapped to current resource uuid.
  - board/task mapping edits refresh visible resource planner tasks immediately (no page reload required).
  - comments/reply and completion toggles are supported in resource planner agenda.

#### Files Touched for This Upgrade
- Backend:
  - `dashboard/views.py`
  - `dashboard/resources_store.py`
  - `dashboard/urls.py`
- Frontend:
  - `dashboard/static/js/planner-ui.js`
  - `dashboard/static/js/home-overview.js`
  - `dashboard/static/js/resource-planner.js`
  - `dashboard/static/css/app.css`
  - `dashboard/templates/pages/home.html`
  - `dashboard/templates/pages/resource_detail.html`

### Asana Extended API (2026-02)
Full Asana API surface added. New view functions in `dashboard/views.py`, registered in `dashboard/urls.py`.

New endpoints:
- `GET /calendar/asana/tasks/<gid>/subtasks/`
- `GET /calendar/asana/boards/<gid>/sections/`
- `POST /calendar/asana/sections/<gid>/add-task/`
- `POST /calendar/asana/tasks/<gid>/assign/`
- `GET /calendar/asana/workspaces/<gid>/members/`
- `GET /calendar/asana/tasks/<gid>/dependencies/`
- `POST /calendar/asana/tasks/<gid>/dependencies/add/`
- `POST /calendar/asana/tasks/<gid>/dependencies/remove/`
- `GET /calendar/asana/boards/<gid>/status/`
- `GET /calendar/asana/tasks/<gid>/attachments/`
- `POST /calendar/asana/boards/<gid>/webhook/register/`
- `POST /calendar/asana/webhook/receive/` (csrf_exempt, handles Asana handshake + HMAC verification)

Webhook secrets are stored per-user at `var/user_data/<user>/asana_webhooks.json`.

New MCP tools in `mcp/app.py` (imported helpers from `dashboard.views`):
`asana_get_subtasks`, `asana_list_sections`, `asana_move_task_to_section`, `asana_update_assignee`, `asana_list_workspace_members`, `asana_get_dependencies`, `asana_add_dependency`, `asana_remove_dependency`, `asana_get_project_status`, `asana_get_attachments`

`_ASANA_TASK_OPT_FIELDS` extended with `assignee.gid`, `assignee.name`, `num_subtasks`.
`_asana_task_row_from_api_task` now returns `assignee_gid`, `assignee_name`, `subtask_count`.

Frontend (`home-overview.js`, `home.html`):
- Assignee badge on task rows
- Subtask count badge on task rows
- "Move to section" action + modal on task rows
