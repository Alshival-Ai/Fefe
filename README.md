# Alshival

Django-based infrastructure/resource management app.

> **Multi-developer project.** Multiple developers push to a single `main` branch on GitHub.
> Always run `git pull` before starting work and always pass `--build` to Docker Compose
> after pulling so your container picks up the latest code changes.

## Setup and Installation

1. Create and activate a virtual environment:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
```

Recommended Python version: `3.13` (the Docker image is pinned to Python 3.13).

2. Install backend dependencies:

```bash
pip install -r requirements.txt
```

3. Configure `.env` (edit existing file or create one). For public domains, set your HTTPS URL + hostnames:

```env
HOST=127.0.0.1
PORT=8000
APP_BASE_URL=http://127.0.0.1:8000
ALLOWED_HOSTS=127.0.0.1,localhost
CSRF_TRUSTED_ORIGINS=http://127.0.0.1:8000
ALSHIVAL_SSH_KEY_MASTER_KEYS=<your-random-key>
```

Example (public HTTPS):

```env
APP_BASE_URL=https://dev.alshival.dev
ALLOWED_HOSTS=dev.alshival.dev
CSRF_TRUSTED_ORIGINS=https://dev.alshival.dev
```

4. Apply database migrations:

```bash
python manage.py migrate
```

5. Run the app:

```bash
python manage.py runserver
```

On a fresh database, the app now opens at `/setup/` before login. The setup screen lets you:
- Create the first admin user (when no users exist yet)
- Save optional API keys

After setup is completed, unauthenticated users are sent to `/accounts/login/` as usual.

Optional dev helper:

```bash
./scripts/dev.sh
```

## Frontend (Optional)

Run Vite dev server:

```bash
cd frontend
npm install
npm run dev
```

Build frontend assets:

```bash
cd frontend
npm run build
```

## Domain/Base URL config

Set these in `.env` when you move from localhost to your real domain:

- `APP_BASE_URL` (example: `https://app.example.com`)
- `ALLOWED_HOSTS` (example: `app.example.com,www.app.example.com`)
- `CSRF_TRUSTED_ORIGINS` (example: `https://app.example.com,https://www.app.example.com`)

Notes:
- `APP_BASE_URL` is also used to auto-add its host/origin into Django host/CSRF config.
- For SDK log routing, prefer setting `ALSHIVAL_RESOURCE` to the full resource URL in the client app.

## Docker

> **Always use `--build`** when running Docker Compose after a `git pull`. This ensures the
> container image is rebuilt with the latest code. Omitting `--build` will run a stale image
> that does not include recent changes from other developers.

### Production-style run

```bash
git pull
docker compose up --build
```

This starts:
- `web`: Django + Uvicorn
- `worker`: periodic resource health checker (every 5 minutes), using a pooled model of ~1 worker per 10 active users (capped)
- `calendar-worker`: periodic Asana + Outlook cache sync (every 60 seconds)
- `global-key-worker`: rotates global internal API keys
- `user-key-worker`: rotates per-user internal account API keys (stored in each user's `member.db`)
- `github-mcp`: uses the official `ghcr.io/github/github-mcp-server` image (avoids local source build issues)

### HTTP reverse proxy setup (`-http`)

Use the nginx HTTP overlay to expose the app on host port `80`:

```bash
docker compose -f docker-compose.yml -f docker-compose-http.yml up --build
```

This adds:
- `nginx-http`: reverse proxy on `:80` forwarding to `web:8000`

Naming convention:
- HTTP-only setup files use `-http` suffix.
- TLS/certbot setup will use `-https` suffix later.

### HTTPS reverse proxy setup (`-https`)

Use the nginx HTTPS overlay to expose the app on host ports `80/443`:

```bash
docker compose -f docker-compose.yml -f docker-compose-https.yml up --build
```

Or use the helper:

```bash
./tools/prod.sh
```

Certbot (auto TLS):
- `./tools/prod.sh` will auto-fetch a cert for `dev.alshival.dev` using `support@alshival.ai` if none exists.
- Override defaults with environment variables:
  - `CERTBOT_DOMAIN=your.domain`
  - `CERTBOT_EMAIL=you@example.com`
- Optional: `CERTBOT_CERT_NAME=your.domain-0001` when certbot issued a suffixed cert name.
  - If not set, `./tools/prod.sh` auto-detects the latest matching cert name.

This adds:
- `nginx-https`: reverse proxy on `:443` forwarding to `web:8000` with a `:80` HTTP->HTTPS redirect

TLS certs:
- Mount `fullchain.pem` and `privkey.pem` into `./docker/ssl/` on the host.

### Fast dev loop (recommended)

Initial build (and after every `git pull`):

```bash
git pull
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

After that, if you haven't pulled new changes, you can skip `--build`:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```

Why this is faster:
- Source is bind-mounted into the container (`.:/app`)
- `uvicorn --reload` auto-restarts on code changes without needing a rebuild
- `collectstatic` is skipped in dev (`RUN_COLLECTSTATIC=0`)

> Note: Even with bind-mount + `--reload`, **new Python dependencies** (added to `requirements.txt`)
> require `--build` to be installed inside the container.

App URL: `http://127.0.0.1:8000`

Worker logs:

```bash
docker compose logs -f worker
```

Container defaults:
- SQLite DB: `/app/var/db.sqlite3`
- Per-user data: `/app/var/user_data`
- Collected static files: `/app/var/staticfiles`

These are persisted to host `./var` via compose volume.

## Knowledge Base Note (Alpha)

- Team-owned resource health knowledge is currently duplicated into each active team member's personal KB (`var/user_data/<user>/home/.alshival/knowledge.db`).
- This is intentional for alpha simplicity so user-scoped retrieval can include team assets without team KB joins.
- Tradeoff: increased storage/memory usage due to duplication.
- Team `knowledge.db` stores are treated as inactive in this mode and are pruned by cleanup.
- Planned follow-up: replace duplication with shared team KB retrieval/fusion.
