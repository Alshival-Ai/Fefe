from pathlib import Path

from django.conf import settings
from django.contrib.auth.signals import user_logged_in
from django.dispatch import receiver
from django.utils.text import slugify


def _user_folder_base() -> Path:
    base = getattr(settings, 'USER_DATA_ROOT', None)
    if base:
        return Path(base)
    return Path(settings.BASE_DIR) / 'user_data'


@receiver(user_logged_in)
def ensure_user_folder(sender, request, user, **kwargs):
    base_dir = _user_folder_base()
    base_dir.mkdir(parents=True, exist_ok=True)

    username = user.get_username() or f"user-{user.pk}"
    safe_username = slugify(username) or f"user-{user.pk}"
    user_dir = base_dir / f"{safe_username}-{user.pk}"
    user_dir.mkdir(parents=True, exist_ok=True)
