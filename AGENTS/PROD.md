# PROD

## Multi-Developer Deployment Note

This is a team project with multiple developers pushing to `main`. When deploying after a `git pull`:

```bash
git pull
docker compose up --build          # production
# or
docker compose -f docker-compose.yml -f docker-compose-https.yml up --build  # HTTPS
```

**Always pass `--build`** after pulling — this forces the Docker image to rebuild with the
latest code and any new Python dependencies. Running without `--build` will serve a stale image.

The `./var` bind-mount preserves all runtime data (DB, user files, knowledge base) across rebuilds.
**Never** run `docker volume prune` or `docker system prune --volumes`.

## Runtime Overview
- Main web app: Django + Uvicorn (`alshival.asgi:application`)
- Background workers:
  - Resource health worker (`run_resource_health_worker`)
  - Global API key worker (`run_global_api_key_worker`)
- Optional MCP service: `mcp.app:app`

## Deployment Defaults (Docker)
- SQLite DB path: `/app/var/db.sqlite3`
- User data root: `/app/var/user_data`
- Static root: `/app/var/staticfiles`
- Entrypoint: `docker/entrypoint.sh`
  - Applies migrations (unless `RUN_MIGRATE=0`)
  - Runs per-user home data migration (`migrate_user_home_data`) when `RUN_USER_HOME_MIGRATION=1` (defaults to `RUN_MIGRATE`)
    - Optional flags: `RUN_USER_HOME_MIGRATION_DRY_RUN=1`, `RUN_USER_HOME_MIGRATION_SKIP_PRUNE=1`, `RUN_USER_HOME_MIGRATION_FINALIZE=1`
  - Runs collectstatic (unless `RUN_COLLECTSTATIC=0`)

## Resource Details: Current Production UI State
- Overview includes health chart and Run Health Check action.
- Notes section uses Team Comments-style author row with avatar support.
- Resource API Keys list fills available card space and scrolls internally.
- Cloud Logs section is full-width.
- Header includes:
  - `ALSHIVAL_RESOURCE=<absolute-resource-url>` copy action
  - Alerts icon that opens persisted alert settings modal

## Alerts Settings Status
- Resource-level user alert settings are persisted in each resource package DB (`resource.db`, table `resource_alert_settings`).
- Supported setting families:
  - Health Alerts: App / SMS / Email
  - Cloud Log Errors: App / SMS / Email
- Defaults:
  - App enabled
  - SMS disabled
  - Email disabled
- Current status:
  - Preferences persist and are editable in the Resource Details modal.
  - Downstream SMS/email delivery dispatch is not yet wired.

## Resource Route Canonicalization
- Resource endpoints are available on:
  - `/u/<username>/resources/<uuid>/...`
  - `/team/<team_name>/resources/<uuid>/...`
- Alias history is tracked in `ResourceRouteAlias`.
- Old routes remain valid and resolve to resource context, while canonical detail requests redirect to the current alias route.

## Resource Package Ownership and Moves
- Ownership metadata is tracked in `ResourcePackageOwner`.
- Package directories move between user/team/global roots on scope change (`transfer_resource_package`).
- This allows resources to behave like portable assets while preserving path history through route aliases.

## Resource Health Worker Behavior
- `run_resource_health_worker` scans active users and deduplicates checks by resource UUID.
- Because package owner resolution is centralized in `resources_store`, team/global-owned resources are included.
- The worker writes cloud log entries on health transitions:
  - `healthy -> unhealthy`
  - `unhealthy -> healthy`

## Local Shell / Ask Alshival Behavior
- Superuser shell sessions default to per-user home:
  - `<USER_DATA_ROOT>/<slug-username>-<user_id>/home`
- Home is created automatically on first launch.
- Shell mode is local-first by default; set `WEB_TERMINAL_PREFER_HOST_SHELL=1` to prefer host shell mode.
- Static env-based identity override requires:
  - `WEB_TERMINAL_FORCE_STATIC_IDENTITY=1`
  - plus `WEB_TERMINAL_LOCAL_USERNAME` and `WEB_TERMINAL_LOCAL_HOME`
