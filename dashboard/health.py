from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import socket
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from dashboard.request_context import get_current_user
from dashboard.resources_store import get_resource, log_resource_check, update_resource_health


STATUS_HEALTHY = "healthy"
STATUS_UNHEALTHY = "unhealthy"
STATUS_UNKNOWN = "unknown"


@dataclass
class HealthResult:
    resource_id: int
    status: str
    checked_at: str
    target: str
    error: str


def _http_healthcheck(url: str, timeout: float) -> tuple[str, str]:
    try:
        req = Request(url, headers={"User-Agent": "FefeHealth/1.0"})
        with urlopen(req, timeout=timeout) as resp:
            code = getattr(resp, "status", None)
            if code is None and hasattr(resp, "getcode"):
                code = resp.getcode()
        if code is None:
            return STATUS_UNKNOWN, "No HTTP status code"
        if 200 <= int(code) < 400:
            return STATUS_HEALTHY, ""
        return STATUS_UNHEALTHY, f"HTTP {code}"
    except Exception as exc:
        return STATUS_UNHEALTHY, str(exc)


def _socket_check(address: str, port: int, timeout: float) -> tuple[str, str]:
    try:
        with socket.create_connection((address, int(port)), timeout=timeout):
            return STATUS_HEALTHY, ""
    except Exception as exc:
        return STATUS_UNHEALTHY, str(exc)


def _target_from_resource(resource) -> str:
    if resource.healthcheck_url:
        return resource.healthcheck_url
    if resource.address and resource.port:
        return f"{resource.address}:{resource.port}"
    if resource.address:
        return resource.address
    return resource.target or "unknown"


def _coerce_port(value: str | None, default: int) -> int:
    try:
        if value:
            return int(value)
    except Exception:
        pass
    return int(default)


def _check_resource(resource) -> tuple[str, str]:
    if resource.healthcheck_url:
        return _http_healthcheck(resource.healthcheck_url, timeout=6)

    if resource.resource_type == "vm" and resource.address:
        port = _coerce_port(resource.ssh_port, 22)
        return _socket_check(resource.address, port, timeout=4)

    if resource.address and resource.port:
        return _socket_check(resource.address, resource.port, timeout=4)

    if resource.target:
        parsed = urlparse(resource.target)
        if parsed.scheme in ("http", "https"):
            return _http_healthcheck(resource.target, timeout=6)
        if ":" in resource.target:
            host, port_str = resource.target.rsplit(":", 1)
            return _socket_check(host, _coerce_port(port_str, 80), timeout=4)

    return STATUS_UNKNOWN, "No healthcheck target configured"


def check_health(resource_id: int, user=None) -> HealthResult:
    current_user = user or get_current_user()
    if current_user is None:
        raise RuntimeError("check_health requires a user for multi-tenant lookups.")

    resource = get_resource(current_user, resource_id)
    if resource is None:
        raise ValueError(f"Resource {resource_id} not found for user.")

    status, error = _check_resource(resource)
    checked_at = datetime.now(timezone.utc).isoformat()
    update_resource_health(current_user, resource_id, status, checked_at, error)
    log_resource_check(current_user, resource_id, status, checked_at, _target_from_resource(resource), error)
    return HealthResult(
        resource_id=resource_id,
        status=status,
        checked_at=checked_at,
        target=_target_from_resource(resource),
        error=error,
    )
