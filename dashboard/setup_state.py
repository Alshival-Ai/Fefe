from __future__ import annotations

import os

from django.conf import settings
from django.db.utils import OperationalError, ProgrammingError

from .models import SystemSetup


def get_setup_state() -> SystemSetup | None:
    try:
        return SystemSetup.objects.order_by("id").first()
    except (OperationalError, ProgrammingError):
        return None


def get_or_create_setup_state() -> SystemSetup | None:
    try:
        setup = SystemSetup.objects.order_by("id").first()
        if setup is None:
            setup = SystemSetup.objects.create()
        return setup
    except (OperationalError, ProgrammingError):
        return None


def is_setup_complete() -> bool:
    setup = get_setup_state()
    return bool(setup and setup.is_completed)


def get_ingest_api_key() -> str:
    setup = get_setup_state()
    if setup and setup.ingest_api_key:
        return setup.ingest_api_key.strip()
    return str(getattr(settings, "ALSHIVAL_INGEST_API_KEY", "")).strip()


def is_global_monitoring_enabled() -> bool:
    setup = get_setup_state()
    if setup is None:
        return True
    return bool(getattr(setup, "monitoring_enabled", True))


def get_alshival_default_model() -> str:
    setup = get_setup_state()
    if setup is None:
        return "gpt-4.1-mini"
    model = str(getattr(setup, "default_model", "") or "").strip()
    return model or "gpt-4.1-mini"


def is_microsoft_connector_configured() -> bool:
    try:
        from allauth.socialaccount.models import SocialApp
    except Exception:
        return False

    try:
        microsoft_app = (
            SocialApp.objects.filter(provider="microsoft")
            .exclude(client_id__exact="")
            .exclude(secret__exact="")
            .order_by("id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        return False
    except Exception:
        return False

    if microsoft_app is None:
        return False
    app_settings = dict(getattr(microsoft_app, "settings", {}) or {})
    return bool(str(app_settings.get("tenant") or "").strip())


def is_microsoft_login_enabled() -> bool:
    setup = get_setup_state()
    if setup is None:
        return False
    if not bool(getattr(setup, "microsoft_login_enabled", False)):
        return False
    return is_microsoft_connector_configured()


def is_github_connector_configured() -> bool:
    try:
        from allauth.socialaccount.models import SocialApp
    except Exception:
        return False

    try:
        github_app = (
            SocialApp.objects.filter(provider="github")
            .exclude(client_id__exact="")
            .exclude(secret__exact="")
            .order_by("id")
            .first()
        )
    except (OperationalError, ProgrammingError):
        return False
    except Exception:
        return False

    return github_app is not None


def is_github_login_enabled() -> bool:
    setup = get_setup_state()
    if setup is None:
        return False
    if not bool(getattr(setup, "github_login_enabled", False)):
        return False
    return is_github_connector_configured()


def is_twilio_configured() -> bool:
    setup = get_setup_state()
    twilio_account_sid = str(getattr(setup, "twilio_account_sid", "") or "").strip() if setup else ""
    twilio_auth_token = str(getattr(setup, "twilio_auth_token", "") or "").strip() if setup else ""
    twilio_from_number = str(getattr(setup, "twilio_from_number", "") or "").strip() if setup else ""

    if not twilio_account_sid:
        twilio_account_sid = str(os.getenv("TWILIO_ACCOUNT_SID", "") or "").strip()
    if not twilio_auth_token:
        twilio_auth_token = str(os.getenv("TWILIO_AUTH_TOKEN", "") or "").strip()
    if not twilio_from_number:
        twilio_from_number = str(os.getenv("TWILIO_FROM_NUMBER", "") or "").strip()

    return bool(twilio_account_sid and twilio_auth_token and twilio_from_number)


def is_email_provider_configured() -> bool:
    # Treat SMTP as configured only when explicitly set away from Django defaults.
    env_email_host = str(os.getenv("EMAIL_HOST", "") or "").strip()
    env_default_from = str(os.getenv("DEFAULT_FROM_EMAIL", "") or "").strip()
    cfg_email_host = str(getattr(settings, "EMAIL_HOST", "") or "").strip()
    cfg_default_from = str(getattr(settings, "DEFAULT_FROM_EMAIL", "") or "").strip()

    email_host = env_email_host or cfg_email_host
    default_from = env_default_from or cfg_default_from
    smtp_explicitly_configured = bool(env_email_host or env_default_from)
    smtp_is_non_default = bool(email_host and default_from) and not (
        email_host == "localhost" and default_from == "webmaster@localhost"
    )
    if smtp_explicitly_configured and smtp_is_non_default:
        return True

    try:
        from allauth.socialaccount.models import SocialApp
    except Exception:
        return False

    try:
        if is_microsoft_connector_configured():
            return True

        google_app = (
            SocialApp.objects.filter(provider="google")
            .exclude(client_id__exact="")
            .exclude(secret__exact="")
            .order_by("id")
            .first()
        )
        if google_app is not None:
            return True
    except (OperationalError, ProgrammingError):
        return False
    except Exception:
        return False

    return False
