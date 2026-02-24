from __future__ import annotations

import threading
from typing import Any

_state = threading.local()


def set_current_user(user: Any) -> None:
    _state.user = user


def get_current_user() -> Any | None:
    return getattr(_state, "user", None)


def clear_current_user() -> None:
    if hasattr(_state, "user"):
        delattr(_state, "user")
