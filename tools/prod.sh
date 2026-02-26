#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Ensure host bind-mounted entrypoint stays runnable in containers.
chmod +x "$ROOT_DIR/docker/entrypoint.sh"

find_docker() {
  local cand resolved
  for cand in "${DOCKER_BIN:-}" docker /usr/bin/docker /usr/local/bin/docker /snap/bin/docker; do
    [ -n "$cand" ] || continue

    # If a command name is provided (e.g. "docker"), resolve it from PATH.
    if [[ "$cand" != */* ]]; then
      resolved="$(type -P "$cand" 2>/dev/null || true)"
      [ -n "$resolved" ] || continue
    else
      resolved="$cand"
    fi

    [ -f "$resolved" ] || continue
    if [ -x "$resolved" ]; then
      echo "$resolved"
      return 0
    fi
  done
  return 1
}

DOCKER="$(find_docker)" || {
  echo "Error: docker CLI not found (or resolved to a directory)." >&2
  echo "If docker is installed, set DOCKER_BIN to its full path (e.g. /usr/bin/docker)." >&2
  exit 1
}

export LOCAL_UID="$(id -u)"
export LOCAL_GID="$(id -g)"
export CERTBOT_DOMAIN="${CERTBOT_DOMAIN:-dev.alshival.dev}"
export CERTBOT_EMAIL="${CERTBOT_EMAIL:-support@alshival.ai}"

PRUNE=false
ARGS=()
for arg in "$@"; do
  if [ "$arg" = "--prune" ]; then
    PRUNE=true
    continue
  fi
  ARGS+=("$arg")
done

remove_path() {
  local target="$1"
  [ -e "$target" ] || return 0
  if rm -rf "$target" 2>/dev/null; then
    return 0
  fi
  sudo -n rm -rf "$target"
}

if [ "$PRUNE" = "true" ]; then
  "$DOCKER" compose \
    -f docker-compose.yml \
    -f docker-compose-https.yml \
    down --volumes --remove-orphans || true
  "$DOCKER" system prune -a --volumes -f
  remove_path "$ROOT_DIR/var"
  remove_path "$ROOT_DIR/db.sqlite3"
fi

mkdir -p "$ROOT_DIR/var"
if [ ! -w "$ROOT_DIR/var" ]; then
  sudo -n chown -R "$LOCAL_UID:$LOCAL_GID" "$ROOT_DIR/var"
fi

CERTBOT_DIR="$ROOT_DIR/docker/certbot"
CERT_PATH="$CERTBOT_DIR/conf/live/$CERTBOT_DOMAIN/fullchain.pem"

if [ -d "$CERTBOT_DIR" ]; then
  if [ ! -w "$CERTBOT_DIR" ]; then
    sudo -n chown -R "$LOCAL_UID:$LOCAL_GID" "$CERTBOT_DIR" || true
  fi
fi

if [ ! -f "$CERT_PATH" ]; then
  mkdir -p "$CERTBOT_DIR/www" "$CERTBOT_DIR/conf"
  if [ ! -w "$CERTBOT_DIR/conf" ]; then
    sudo -n chown -R "$LOCAL_UID:$LOCAL_GID" "$CERTBOT_DIR" || true
  fi
  echo "No TLS cert found for $CERTBOT_DOMAIN. Fetching via certbot..." >&2

  "$DOCKER" compose \
    -f docker-compose.yml \
    -f docker-compose-http.yml \
    up -d web nginx-http

  "$DOCKER" run --rm \
    -v "$CERTBOT_DIR/conf:/etc/letsencrypt" \
    -v "$CERTBOT_DIR/www:/var/www/certbot" \
    certbot/certbot:latest certonly \
    --webroot -w /var/www/certbot \
    -d "$CERTBOT_DOMAIN" \
    --email "$CERTBOT_EMAIL" \
    --agree-tos \
    --no-eff-email \
    --keep-until-expiring \
    --non-interactive

  "$DOCKER" compose \
    -f docker-compose.yml \
    -f docker-compose-http.yml \
    down
fi

exec "$DOCKER" compose \
  -f docker-compose.yml \
  -f docker-compose-https.yml \
  up --build "${ARGS[@]}"
