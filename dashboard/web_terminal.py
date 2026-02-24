from __future__ import annotations

import asyncio
import json
import os
import pty
import shlex
import shutil
from dataclasses import dataclass, field
from http.cookies import SimpleCookie
from tempfile import NamedTemporaryFile

from asgiref.sync import sync_to_async
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.sessions.backends.db import SessionStore

from dashboard.global_ssh_store import get_global_ssh_private_key
from dashboard.resources_store import get_resource, _decrypt_key_text, get_ssh_credential_private_key


WS_PATH = "/terminal/ws/"


class TerminalWebSocketApp:
    async def __call__(self, scope, receive, send):
        if scope.get("type") != "websocket":
            return
        if scope.get("path") != WS_PATH:
            await send({"type": "websocket.close", "code": 1000})
            return

        user = await _get_authenticated_user(scope)
        if not user or not user.is_active:
            await send({"type": "websocket.close", "code": 4403})
            return

        await send({"type": "websocket.accept"})

        try:
            ssh_config = await _build_ssh_config(scope, user)
        except Exception as exc:
            await send({"type": "websocket.send", "text": f"Terminal error: {exc}\r\n"})
            await send({"type": "websocket.close", "code": 1008})
            return

        session = SSHSession(ssh_config)
        try:
            await session.start()
        except Exception as exc:
            await send(
                {
                    "type": "websocket.send",
                    "text": f"Failed to start SSH session: {exc}\r\n",
                }
            )
            await send({"type": "websocket.close", "code": 1011})
            await session.stop()
            return

        read_task = asyncio.create_task(session.stream_to_websocket(send))
        try:
            while True:
                event = await receive()
                event_type = event.get("type")
                if event_type == "websocket.receive":
                    if "text" in event and event["text"] is not None:
                        text = event["text"]
                        if _handle_resize_message(text, session):
                            continue
                        await session.write(text)
                    elif "bytes" in event and event["bytes"] is not None:
                        await session.write(event["bytes"])
                elif event_type == "websocket.disconnect":
                    break
        finally:
            read_task.cancel()
            await session.stop()


def _handle_resize_message(text: str, session: "SSHSession") -> bool:
    if not text or not text.startswith("{"):
        return False
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return False
    if payload.get("type") != "resize":
        return False
    cols = int(payload.get("cols") or 0)
    rows = int(payload.get("rows") or 0)
    if cols > 0 and rows > 0:
        session.resize(cols, rows)
    return True


async def _get_authenticated_user(scope):
    headers = dict(scope.get("headers") or [])
    cookie_header = headers.get(b"cookie", b"").decode()
    if not cookie_header:
        return None
    cookies = SimpleCookie()
    cookies.load(cookie_header)
    session_cookie = cookies.get(settings.SESSION_COOKIE_NAME)
    if not session_cookie or not session_cookie.value:
        return None

    session_key = session_cookie.value
    session = SessionStore(session_key=session_key)
    try:
        data = await sync_to_async(session.load)()
    except Exception:
        return None

    user_id = data.get("_auth_user_id")
    if not user_id:
        return None

    User = get_user_model()
    try:
        user = await sync_to_async(User.objects.get)(pk=user_id)
    except User.DoesNotExist:
        return None
    session_hash = data.get("_auth_user_hash")
    if session_hash and session_hash != user.get_session_auth_hash():
        return None
    return user


class SSHSession:
    def __init__(self, config: "SSHConfig"):
        self.config = config
        self.master_fd = None
        self.slave_fd = None
        self.process = None

    async def start(self):
        ssh_bin = os.getenv("WEB_TERMINAL_SSH_BIN", "").strip()
        if not ssh_bin:
            ssh_bin = shutil.which("ssh") or "/usr/bin/ssh"
        if not os.path.exists(ssh_bin):
            raise RuntimeError(f"ssh binary not found at {ssh_bin}")

        args = [ssh_bin, "-tt"]
        if self.config.key_path:
            args.extend(["-i", self.config.key_path, "-o", "IdentitiesOnly=yes"])
        args.extend(
            [
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ServerAliveInterval=30",
                "-o",
                "ServerAliveCountMax=3",
                "-o",
                f"StrictHostKeyChecking={self.config.strict_checking}",
                "-p",
                str(self.config.port),
            ]
        )
        if self.config.known_hosts:
            args.extend(["-o", f"UserKnownHostsFile={self.config.known_hosts}"])
        if self.config.extra_args:
            args.extend(shlex.split(self.config.extra_args))
        args.append(f"{self.config.username}@{self.config.host}")

        self.master_fd, self.slave_fd = pty.openpty()
        self.process = await asyncio.create_subprocess_exec(
            *args,
            stdin=self.slave_fd,
            stdout=self.slave_fd,
            stderr=self.slave_fd,
            start_new_session=True,
        )
        os.close(self.slave_fd)
        self.slave_fd = None

    async def stream_to_websocket(self, send):
        try:
            while True:
                data = await asyncio.to_thread(os.read, self.master_fd, 2048)
                if not data:
                    break
                await send({"type": "websocket.send", "bytes": data})
        except Exception:
            pass
        try:
            await send({"type": "websocket.close", "code": 1000})
        except Exception:
            return

    async def write(self, payload):
        if self.master_fd is None:
            return
        if isinstance(payload, str):
            data = payload.encode()
        else:
            data = payload
        await asyncio.to_thread(os.write, self.master_fd, data)

    def resize(self, cols: int, rows: int):
        if self.master_fd is None:
            return
        try:
            import fcntl
            import termios
            import struct

            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, winsize)
        except Exception:
            return

    async def stop(self):
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()
        if self.master_fd is not None:
            try:
                os.close(self.master_fd)
            except OSError:
                pass
            self.master_fd = None
        for path in self.config.cleanup_paths:
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass


@dataclass
class SSHConfig:
    host: str
    username: str
    port: int
    key_path: str
    known_hosts: str
    strict_checking: str
    extra_args: str
    cleanup_paths: list[str] = field(default_factory=list)


async def _build_ssh_config(scope, user) -> SSHConfig:
    query = scope.get("query_string") or b""
    query_params = {}
    if query:
        for chunk in query.decode().split("&"):
            if not chunk:
                continue
            key, _, value = chunk.partition("=")
            query_params[key] = value
    resource_id = query_params.get("resource_id")
    if not resource_id:
        raise ValueError("resource_id is required.")
    return await _user_resource_ssh_config(user, resource_id)


@sync_to_async
def _user_resource_ssh_config(user, resource_id: str) -> SSHConfig:
    resource = get_resource(user, int(resource_id))
    if not resource:
        raise PermissionError("Resource access denied.")
    if resource.resource_type != "vm":
        raise ValueError("Resource is not a virtual machine.")
    if not resource.address:
        raise ValueError("Resource host is missing.")
    if not resource.ssh_username:
        raise ValueError("SSH username is missing.")

    key_text = _decrypt_key_text(resource.ssh_key_text)
    if not key_text and resource.ssh_credential_id:
        credential_lookup = None
        raw_credential_id = (resource.ssh_credential_id or '').strip()
        if raw_credential_id.startswith('global:'):
            try:
                credential_lookup = get_global_ssh_private_key(credential_id=int(raw_credential_id.split(':', 1)[1]))
            except (TypeError, ValueError):
                credential_lookup = None
        else:
            local_raw = raw_credential_id
            if raw_credential_id.startswith('local:'):
                local_raw = raw_credential_id.split(':', 1)[1]
            try:
                credential_lookup = get_ssh_credential_private_key(user, int(local_raw))
            except (TypeError, ValueError):
                credential_lookup = None
        if credential_lookup:
            key_text = credential_lookup
    if not key_text:
        raise ValueError("No SSH key available for this resource.")

    with NamedTemporaryFile(delete=False, prefix="fefe-key-", suffix=".key", mode="w") as tmp:
        tmp.write(key_text)
        key_path = tmp.name
    os.chmod(key_path, 0o600)

    strict_checking = os.getenv("WEB_TERMINAL_SSH_STRICT_HOST_KEY_CHECKING", "no").strip()
    known_hosts = os.getenv("WEB_TERMINAL_SSH_KNOWN_HOSTS", "").strip()
    if strict_checking == "no" and not known_hosts:
        known_hosts = "/dev/null"

    return SSHConfig(
        host=resource.address,
        username=resource.ssh_username,
        port=int(resource.ssh_port or 22),
        key_path=key_path,
        known_hosts=known_hosts,
        strict_checking=strict_checking,
        extra_args=os.getenv("WEB_TERMINAL_SSH_EXTRA_ARGS", "").strip(),
        cleanup_paths=[key_path],
    )
