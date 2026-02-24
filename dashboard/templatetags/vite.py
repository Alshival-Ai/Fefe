import json
from pathlib import Path

from django import template
from django.conf import settings
from django.utils.safestring import mark_safe

register = template.Library()


def _manifest_path() -> Path:
    return Path(settings.BASE_DIR) / 'dashboard' / 'static' / 'frontend' / 'manifest.json'


def _manifest_entry(asset: str) -> dict:
    manifest_file = _manifest_path()
    if not manifest_file.exists():
        return {}
    with manifest_file.open('r', encoding='utf-8') as handle:
        data = json.load(handle)
    return data.get(asset, {})


@register.simple_tag
def vite_hmr():
    if getattr(settings, 'VITE_DEV_MODE', settings.DEBUG):
        dev_server = getattr(settings, 'VITE_DEV_SERVER', 'http://localhost:5173')
        return mark_safe(
            f"<script type=\"module\" src=\"{dev_server}/@vite/client\"></script>"
        )
    return ''


@register.simple_tag
def vite_asset(asset: str):
    if getattr(settings, 'VITE_DEV_MODE', settings.DEBUG):
        dev_server = getattr(settings, 'VITE_DEV_SERVER', 'http://localhost:5173')
        return mark_safe(
            f"<script type=\"module\" src=\"{dev_server}/{asset}\"></script>"
        )

    entry = _manifest_entry(asset)
    if not entry:
        return ''

    tags = []
    css_files = entry.get('css', [])
    for css in css_files:
        tags.append(f"<link rel=\"stylesheet\" href=\"/static/frontend/{css}\" />")

    file_name = entry.get('file')
    if file_name:
        tags.append(f"<script type=\"module\" src=\"/static/frontend/{file_name}\"></script>")

    return mark_safe("\n".join(tags))
