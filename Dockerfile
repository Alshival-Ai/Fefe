FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG APP_USER=app
ARG APP_UID=1000
ARG APP_GID=1000

WORKDIR /app

COPY requirements.txt ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        iputils-ping \
        nodejs \
        npm \
        openssh-client \
        sudo \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @openai/codex \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .
RUN chmod +x /app/docker/entrypoint.sh

RUN if ! getent group "${APP_GID}" >/dev/null; then groupadd --gid "${APP_GID}" "${APP_USER}"; fi \
    && if ! id -u "${APP_USER}" >/dev/null 2>&1; then useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home --shell /bin/bash "${APP_USER}"; fi \
    && printf "%s ALL=(ALL) NOPASSWD:ALL\n" "${APP_USER}" > "/etc/sudoers.d/${APP_USER}" \
    && chmod 0440 "/etc/sudoers.d/${APP_USER}" \
    && chown -R "${APP_UID}:${APP_GID}" /app

USER ${APP_UID}:${APP_GID}

EXPOSE 8000
ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["uvicorn", "alshival.asgi:application", "--host", "0.0.0.0", "--port", "8000"]
