"""
ASGI config for fefe project.
"""

from __future__ import annotations

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "fefe.settings")

from django.core.asgi import get_asgi_application
from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler

_django_app = ASGIStaticFilesHandler(get_asgi_application())
from dashboard.web_terminal import TerminalWebSocketApp

_terminal_app = TerminalWebSocketApp()


async def application(scope, receive, send):
    if scope.get("type") == "websocket":
        return await _terminal_app(scope, receive, send)
    return await _django_app(scope, receive, send)
