from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import pwd
import pty
import re
import shlex
import shutil
import subprocess
import termios
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from tempfile import NamedTemporaryFile
from urllib.parse import parse_qs

from asgiref.sync import sync_to_async
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.sessions.backends.db import SessionStore
from django.utils.text import slugify

from dashboard.models import ResourcePackageOwner, ResourceRouteAlias, ResourceTeamShare, SystemSetup
from dashboard.global_ssh_store import get_global_ssh_private_key
from dashboard.request_auth import user_can_access_resource
from dashboard.resources_store import (
    _decrypt_key_text,
    create_account_api_key,
    get_resource,
    get_resource_by_uuid,
    get_ssh_credential_private_key,
    resolve_api_key_scope,
)


WS_PATH = "/terminal/ws/"
logger = logging.getLogger(__name__)
_CODEX_MCP_RUNTIME_API_KEYS: dict[int, str] = {}


def _ensure_terminal_env(env: dict[str, str]) -> dict[str, str]:
    resolved = dict(env or {})
    if not str(resolved.get("TERM", "") or "").strip():
        resolved["TERM"] = "xterm-256color"
    if not str(resolved.get("COLORTERM", "") or "").strip():
        resolved["COLORTERM"] = "truecolor"
    return resolved


def _resolve_codex_binary() -> str:
    configured = str(os.getenv("WEB_TERMINAL_CODEX_BIN", "") or "").strip()
    if configured:
        if os.path.isabs(configured):
            return configured if os.path.exists(configured) else ""
        resolved = shutil.which(configured)
        if resolved:
            return resolved
    resolved = shutil.which("codex")
    if resolved:
        return resolved
    for candidate in ("/usr/local/bin/codex", "/usr/bin/codex"):
        if os.path.exists(candidate):
            return candidate
    return ""


def _interactive_shell_exec_command(shell: str) -> str:
    shell_name = os.path.basename(str(shell or "").strip())
    if shell_name == "bash":
        return f"exec {shlex.quote(shell)} --noprofile --norc -i"
    if shell_name == "zsh":
        return f"exec {shlex.quote(shell)} -l -i"
    return f"exec {shlex.quote(shell)} -i"


def _is_truthy_env(value: str | None, *, default: bool = False) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _codex_mcp_enabled() -> bool:
    return _is_truthy_env(os.getenv("WEB_TERMINAL_CODEX_MCP_ENABLED"), default=True)


def _codex_mcp_server_name() -> str:
    raw = str(os.getenv("WEB_TERMINAL_CODEX_MCP_SERVER_NAME", "") or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9_-]", "-", raw).strip("-_")
    return normalized or "alshival"


def _codex_mcp_url() -> str:
    configured = str(os.getenv("WEB_TERMINAL_CODEX_MCP_URL", "") or "").strip()
    if configured:
        return configured
    if os.path.exists("/.dockerenv"):
        return "http://mcp:8080/mcp/"
    return "http://127.0.0.1:8080/mcp/"


def _codex_mcp_api_key_header() -> str:
    configured = str(os.getenv("WEB_TERMINAL_CODEX_MCP_API_KEY_HEADER", "") or "").strip()
    if configured:
        return configured
    default_header = str(os.getenv("MCP_API_KEY_HEADER", "") or "").strip()
    return default_header or "x-api-key"


def _codex_mcp_api_key_env_name() -> str:
    configured = str(os.getenv("WEB_TERMINAL_CODEX_MCP_API_KEY_ENV", "") or "").strip()
    if configured:
        return configured
    return "MCP_API_KEY"


def _codex_mcp_api_key_value() -> str:
    explicit = str(os.getenv("WEB_TERMINAL_CODEX_MCP_API_KEY", "") or "").strip()
    if explicit:
        return explicit
    inherited = str(os.getenv(_codex_mcp_api_key_env_name(), "") or "").strip()
    if inherited:
        return inherited
    fallback = str(os.getenv("MCP_API_KEY", "") or "").strip()
    return fallback


def _get_runtime_codex_mcp_api_key_for_user(user) -> str:
    configured = _codex_mcp_api_key_value()
    if configured:
        return configured
    user_id = int(getattr(user, "id", 0) or 0)
    if user_id <= 0:
        return ""
    cached = str(_CODEX_MCP_RUNTIME_API_KEYS.get(user_id) or "").strip()
    if cached:
        try:
            if resolve_api_key_scope(user, cached, "") == "account":
                return cached
        except Exception:
            pass
        _CODEX_MCP_RUNTIME_API_KEYS.pop(user_id, None)
    try:
        _key_id, raw_key = create_account_api_key(user, "Codex MCP Runtime Key")
    except Exception:
        return ""
    resolved = str(raw_key or "").strip()
    if resolved:
        _CODEX_MCP_RUNTIME_API_KEYS[user_id] = resolved
    return resolved


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
            session = await _build_terminal_session(scope, user)
        except Exception as exc:
            logger.exception("Failed to build terminal session")
            await send({"type": "websocket.send", "text": f"Terminal error: {exc}\r\n"})
            await send({"type": "websocket.close", "code": 1008})
            return

        try:
            await session.start()
        except Exception as exc:
            logger.exception("Failed to start terminal session")
            if isinstance(session, HostResourceSSHSession):
                try:
                    fallback = LocalResourceSSHSession(
                        user,
                        session.config,
                        openai_api_key=getattr(session, "_openai_api_key", ""),
                    )
                    await fallback.start()
                    session = fallback
                    await send(
                        {
                            "type": "websocket.send",
                            "text": "Host shell unavailable; started local resource shell mode instead.\r\n",
                        }
                    )
                except Exception as fallback_exc:
                    logger.exception("Failed to start fallback local resource shell session")
                    await send(
                        {
                            "type": "websocket.send",
                            "text": f"Failed to start terminal session: {fallback_exc}\r\n",
                        }
                    )
                    await send({"type": "websocket.close", "code": 1011})
                    await session.stop()
                    return
            elif isinstance(session, HostShellSession):
                try:
                    fallback = LocalShellSession(
                        user,
                        openai_api_key=getattr(session, "_openai_api_key", ""),
                    )
                    await fallback.start()
                    session = fallback
                    await send(
                        {
                            "type": "websocket.send",
                            "text": "Host shell unavailable; started local shell mode instead.\r\n",
                        }
                    )
                except Exception as fallback_exc:
                    logger.exception("Failed to start fallback local shell session")
                    await send(
                        {
                            "type": "websocket.send",
                            "text": f"Failed to start terminal session: {fallback_exc}\r\n",
                        }
                    )
                    await send({"type": "websocket.close", "code": 1011})
                    await session.stop()
                    return
            else:
                await send(
                    {
                        "type": "websocket.send",
                        "text": f"Failed to start terminal session: {exc}\r\n",
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


def _handle_resize_message(text: str, session) -> bool:
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


class PTYProcessSession:
    def __init__(self):
        self.master_fd = None
        self.slave_fd = None
        self.process = None
        self.cleanup_paths: list[str] = []

    async def start(self):
        args = self._build_args()
        env = self._build_env()
        cwd = self._build_cwd()
        self.master_fd, self.slave_fd = pty.openpty()
        self.process = await asyncio.create_subprocess_exec(
            *args,
            stdin=self.slave_fd,
            stdout=self.slave_fd,
            stderr=self.slave_fd,
            env=env,
            cwd=cwd,
            preexec_fn=self._build_preexec(self.slave_fd),
        )
        os.close(self.slave_fd)
        self.slave_fd = None

    def _build_args(self) -> list[str]:
        raise NotImplementedError

    def _build_env(self) -> dict[str, str]:
        return _ensure_terminal_env(os.environ.copy())

    @staticmethod
    def _build_preexec(slave_fd: int):
        def _preexec() -> None:
            try:
                os.setsid()
            except Exception:
                pass
            try:
                fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)
            except Exception:
                pass

        return _preexec

    def _build_cwd(self) -> str | None:
        return None

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
        data = payload.encode() if isinstance(payload, str) else payload
        await asyncio.to_thread(os.write, self.master_fd, data)

    def resize(self, cols: int, rows: int):
        if self.master_fd is None:
            return
        try:
            import fcntl
            import struct
            import termios

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
        for path in self.cleanup_paths:
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


def _build_ssh_exec_args(config: SSHConfig) -> list[str]:
    ssh_bin = os.getenv("WEB_TERMINAL_SSH_BIN", "").strip()
    if not ssh_bin:
        ssh_bin = shutil.which("ssh") or "/usr/bin/ssh"
    if not os.path.exists(ssh_bin):
        raise RuntimeError(f"ssh binary not found at {ssh_bin}")

    args = [ssh_bin, "-tt"]
    if config.key_path:
        args.extend(["-i", config.key_path, "-o", "IdentitiesOnly=yes"])
    args.extend(
        [
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            f"StrictHostKeyChecking={config.strict_checking}",
            "-p",
            str(config.port),
        ]
    )
    if config.known_hosts:
        args.extend(["-o", f"UserKnownHostsFile={config.known_hosts}"])
    if config.extra_args:
        args.extend(shlex.split(config.extra_args))
    args.append(f"{config.username}@{config.host}")
    return args


class SSHSession(PTYProcessSession):
    def __init__(self, config: SSHConfig):
        super().__init__()
        self.config = config
        self.cleanup_paths = [config.key_path]

    def _build_args(self) -> list[str]:
        return _build_ssh_exec_args(self.config)


class HostShellSession(PTYProcessSession):
    def __init__(self, user, *, openai_api_key: str = ""):
        super().__init__()
        self.user = user
        self._openai_api_key = str(openai_api_key or "").strip()
        self._mcp_api_key_value = ""
        self._target_username = ""
        self._os_username = ""
        self._os_home = ""
        self._os_shell = ""

    async def start(self):
        await asyncio.to_thread(self._ensure_target_account)
        await asyncio.to_thread(self._ensure_codex_ready_for_target)
        await super().start()

    @staticmethod
    def _sudo_bin() -> str:
        configured = str(os.getenv("WEB_TERMINAL_SUDO_BIN", "") or "").strip()
        if configured:
            if os.path.isabs(configured):
                return configured if os.path.exists(configured) else ""
            resolved = shutil.which(configured)
            if resolved:
                return resolved
        return shutil.which("sudo") or ""

    @staticmethod
    def _is_root() -> bool:
        return os.geteuid() == 0

    @classmethod
    def _can_switch_users(cls) -> bool:
        if cls._is_root():
            return True
        sudo_bin = cls._sudo_bin()
        if not sudo_bin:
            return False
        try:
            probe = subprocess.run(
                [sudo_bin, "-n", "true"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=2,
            )
            return probe.returncode == 0
        except Exception:
            return False

    def _privileged_prefix(self) -> list[str]:
        sudo_bin = self._sudo_bin()
        if sudo_bin:
            return [sudo_bin, "-n"]
        if self._is_root():
            return []
        raise RuntimeError("Host terminal mode requires sudo (or root execution) for account provisioning.")

    def _run_as_user_command(self, username: str, command: list[str]) -> list[str]:
        sudo_bin = self._sudo_bin()
        if sudo_bin:
            return [sudo_bin, "-n", "-H", "-u", username, "--", *command]
        if not self._is_root():
            raise RuntimeError("Host terminal mode requires sudo to launch a shell as the target user.")
        runuser_bin = shutil.which("runuser") or "/usr/sbin/runuser"
        if os.path.exists(runuser_bin):
            return [runuser_bin, "-u", username, "--", *command]
        su_bin = shutil.which("su") or "/bin/su"
        if os.path.exists(su_bin):
            quoted = " ".join(shlex.quote(part) for part in command)
            return [su_bin, "-", username, "-c", quoted]
        raise RuntimeError("Host terminal mode requires sudo, runuser, or su to switch users.")

    def _resolve_target_username(self) -> str:
        if self._target_username:
            return self._target_username
        configured = str(os.getenv("WEB_TERMINAL_HOST_USERNAME", "") or "").strip().lower()
        reserved_usernames = {"root", "app"}
        extra_reserved = str(os.getenv("WEB_TERMINAL_RESERVED_USERNAMES", "") or "").strip()
        if extra_reserved:
            for token in extra_reserved.split(","):
                token_value = str(token or "").strip().lower()
                if token_value:
                    reserved_usernames.add(token_value)

        if configured and configured not in reserved_usernames:
            self._target_username = configured
            return self._target_username

        raw_username = str(getattr(self.user, "username", "") or "").strip().lower()
        if not raw_username:
            raw_username = f"user-{int(getattr(self.user, 'pk', 0) or 0)}"
        normalized = re.sub(r"[^a-z0-9_-]", "-", raw_username).strip("-_")
        if not normalized:
            normalized = f"user-{int(getattr(self.user, 'pk', 0) or 0)}"
        if normalized[0].isdigit():
            normalized = f"u-{normalized}"
        candidate = normalized[:32]
        if candidate in reserved_usernames:
            candidate = f"u-{candidate}"
        self._target_username = candidate[:32]
        return self._target_username

    def _resolve_host_home_root(self) -> str:
        configured_root = str(os.getenv("WEB_TERMINAL_HOST_HOME_ROOT", "") or "").strip()
        if configured_root:
            os.makedirs(configured_root, exist_ok=True)
            return configured_root
        user_data_root = str(getattr(settings, "USER_DATA_ROOT", "") or "").strip()
        if not user_data_root:
            user_data_root = str(settings.BASE_DIR / "var" / "user_data")
        host_home_root = os.path.join(user_data_root, "host_homes")
        os.makedirs(host_home_root, exist_ok=True)
        return host_home_root

    def _desired_target_home(self, username: str) -> str:
        return os.path.join(self._resolve_host_home_root(), username, "home")

    def _ensure_target_home_mapping(self, *, username: str, current_home: str) -> None:
        desired_home = self._desired_target_home(username)
        if str(current_home or "").strip() == desired_home:
            return
        privileged_prefix = self._privileged_prefix()
        usermod_bin = shutil.which("usermod") or "/usr/sbin/usermod"
        subprocess.run(
            [*privileged_prefix, "mkdir", "-p", os.path.dirname(desired_home)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        move_result = subprocess.run(
            [*privileged_prefix, usermod_bin, "--home", desired_home, "--move-home", username],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if move_result.returncode != 0:
            set_home_result = subprocess.run(
                [*privileged_prefix, usermod_bin, "--home", desired_home, username],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if set_home_result.returncode != 0:
                raise RuntimeError(
                    f"Failed to set persistent home for host user '{username}' at '{desired_home}'."
                )
            subprocess.run(
                [*privileged_prefix, "mkdir", "-p", desired_home],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        subprocess.run(
            [*privileged_prefix, "chown", "-R", f"{username}:{username}", desired_home],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def _ensure_target_account(self) -> None:
        target_username = self._resolve_target_username()
        target_home = self._desired_target_home(target_username)
        try:
            entry = pwd.getpwnam(target_username)
            self._ensure_target_home_mapping(
                username=target_username,
                current_home=str(entry.pw_dir or "").strip(),
            )
            return
        except KeyError:
            pass

        default_shell = str(os.getenv("WEB_TERMINAL_HOST_DEFAULT_SHELL", "") or "").strip() or "/bin/bash"
        useradd_bin = shutil.which("useradd") or "/usr/sbin/useradd"
        privileged_prefix = self._privileged_prefix()
        os.makedirs(os.path.dirname(target_home), exist_ok=True)
        result = subprocess.run(
            [
                *privileged_prefix,
                useradd_bin,
                "--create-home",
                "--home-dir",
                target_home,
                "--shell",
                default_shell,
                target_username,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to create host user '{target_username}' for terminal access.")
        try:
            pwd.getpwnam(target_username)
        except KeyError as exc:
            raise RuntimeError(f"Host user '{target_username}' was not created.") from exc

    def _ensure_codex_ready_for_target(self) -> None:
        target_username = self._resolve_target_username()
        if _codex_mcp_enabled():
            self._mcp_api_key_value = _get_runtime_codex_mcp_api_key_for_user(self.user)
        ensure_flag = str(os.getenv("WEB_TERMINAL_ENSURE_CODEX", "1") or "").strip().lower()
        if ensure_flag not in {"0", "false", "no", "off"}:
            self._ensure_codex_global_install()
            self._ensure_codex_profile(target_username)
        self._ensure_codex_api_key_login(target_username)

    def _ensure_codex_api_key_login(self, username: str) -> None:
        if not self._openai_api_key:
            return
        codex_bin = _resolve_codex_binary()
        if not codex_bin:
            return
        cmd = self._run_as_user_command(username, [codex_bin, "login", "--with-api-key"])
        try:
            result = subprocess.run(
                cmd,
                input=f"{self._openai_api_key}\n".encode("utf-8"),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if result.returncode != 0:
                logger.warning(
                    "Failed to persist Codex API key for host terminal user '%s'.",
                    username,
                )
        except Exception:
            logger.warning(
                "Failed to provision Codex API key for host terminal user '%s'.",
                username,
            )

    def _ensure_codex_global_install(self) -> None:
        if shutil.which("codex") or os.path.exists("/usr/local/bin/codex") or os.path.exists("/usr/bin/codex"):
            return
        npm_bin = shutil.which("npm") or "/usr/bin/npm"
        if not os.path.exists(npm_bin):
            raise RuntimeError("Unable to auto-install codex: missing npm.")
        privileged_prefix = self._privileged_prefix()
        install_result = subprocess.run(
            [*privileged_prefix, npm_bin, "install", "-g", "@openai/codex"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if install_result.returncode != 0:
            raise RuntimeError("Failed to install @openai/codex globally.")
        if os.path.exists("/usr/local/bin/codex") and not os.path.exists("/usr/bin/codex"):
            subprocess.run(
                [*privileged_prefix, "ln", "-s", "/usr/local/bin/codex", "/usr/bin/codex"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        if not (shutil.which("codex") or os.path.exists("/usr/local/bin/codex") or os.path.exists("/usr/bin/codex")):
            raise RuntimeError("Codex installation completed but codex binary is still unavailable.")

    def _ensure_codex_profile(self, username: str) -> None:
        python_bin = shutil.which("python3") or "/usr/bin/python3"
        if not os.path.exists(python_bin):
            raise RuntimeError("Unable to configure codex profile: missing python3.")
        profile_header = "[profiles.pro]"
        mcp_header = f"[mcp_servers.{_codex_mcp_server_name()}]"
        mcp_config_enabled = _codex_mcp_enabled()
        mcp_enabled = mcp_config_enabled and bool(str(self._mcp_api_key_value or _codex_mcp_api_key_value()).strip())
        profile_lines = [
            'approval_policy = "never"',
            'sandbox_mode = "danger-full-access"',
        ]
        mcp_lines = [
            f'url = "{_codex_mcp_url()}"',
            "startup_timeout_sec = 20",
            "tool_timeout_sec = 90",
            f"enabled = {'true' if mcp_enabled else 'false'}",
            f'bearer_token_env_var = "{_codex_mcp_api_key_env_name()}"',
        ]
        script = """
import os
from pathlib import Path

path = Path(os.path.expanduser("~")) / ".codex" / "config.toml"
path.parent.mkdir(parents=True, exist_ok=True)
text = path.read_text(encoding="utf-8") if path.exists() else ""
lines = text.splitlines()

def upsert_section(lines, header, section_lines):
    idx = -1
    for i, line in enumerate(lines):
        if line.strip() == header:
            idx = i
            break
    if idx == -1:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(header)
        lines.extend(section_lines)
        return lines

    end = len(lines)
    for j in range(idx + 1, len(lines)):
        if lines[j].lstrip().startswith("["):
            end = j
            break

    for target_line in section_lines:
        key = target_line.split("=", 1)[0].strip()
        match_idx = -1
        for j in range(idx + 1, end):
            stripped = lines[j].strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith(f"{key} ") or stripped.startswith(f"{key}="):
                match_idx = j
                break
        if match_idx != -1:
            lines[match_idx] = target_line
        else:
            lines.insert(end, target_line)
            end += 1
    return lines

lines = upsert_section(lines, __PROFILE_HEADER__, __PROFILE_LINES__)
if __MCP_CONFIG_ENABLED__:
    lines = upsert_section(lines, __MCP_HEADER__, __MCP_LINES__)

path.write_text("\\n".join(lines).rstrip() + "\\n", encoding="utf-8")
"""
        script = (
            script.replace("__PROFILE_HEADER__", json.dumps(profile_header))
            .replace("__PROFILE_LINES__", json.dumps(profile_lines))
            .replace("__MCP_CONFIG_ENABLED__", "True" if mcp_config_enabled else "False")
            .replace("__MCP_HEADER__", json.dumps(mcp_header))
            .replace("__MCP_LINES__", json.dumps(mcp_lines))
        )
        cmd = self._run_as_user_command(username, [python_bin, "-c", script])
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to configure ~/.codex/config.toml for host user '{username}'.")

    def _resolve_os_identity(self) -> tuple[str, str, str]:
        if self._os_username and self._os_home and self._os_shell:
            return (self._os_username, self._os_home, self._os_shell)
        try:
            entry = pwd.getpwnam(self._resolve_target_username())
            self._os_username = str(entry.pw_name or "").strip()
            self._os_home = str(entry.pw_dir or "").strip()
            self._os_shell = str(entry.pw_shell or "").strip()
        except Exception:
            self._os_username = ""
            self._os_home = ""
            self._os_shell = "/bin/bash"
        return (self._os_username, self._os_home, self._os_shell)

    def _build_args(self) -> list[str]:
        username, _home, os_shell = self._resolve_os_identity()
        if not username:
            raise RuntimeError("Unable to resolve host terminal account.")
        shell = str(os.getenv("WEB_TERMINAL_HOST_SHELL", "") or "").strip() or os_shell
        if not shell:
            shell = shutil.which("bash") or shutil.which("sh") or "/bin/sh"
        if not os.path.exists(shell):
            raise RuntimeError(f"host shell not found at {shell}")
        shell_name = os.path.basename(shell)
        auto_launch_codex = str(os.getenv("WEB_TERMINAL_AUTO_LAUNCH_CODEX", "1") or "").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        if auto_launch_codex:
            codex_command = str(os.getenv("WEB_TERMINAL_CODEX_COMMAND", "codex -p pro") or "").strip() or "codex -p pro"
            if shell_name == "bash":
                fallback_shell = f"exec {shlex.quote(shell)} --noprofile --norc -i"
            elif shell_name == "zsh":
                fallback_shell = f"exec {shlex.quote(shell)} -l -i"
            else:
                fallback_shell = f"exec {shlex.quote(shell)} -i"
            startup_script = (
                "set +e; "
                "cd \"$HOME\" >/dev/null 2>&1 || true; "
                "export TERM=\"${TERM:-xterm-256color}\"; "
                "export COLORTERM=\"${COLORTERM:-truecolor}\"; "
                "trap '' INT; "
                f"{codex_command}; "
                "trap - INT; "
                f"{fallback_shell}"
            )
            shell_args = [shell, "-lc", startup_script]
        elif shell_name == "bash":
            shell_args = [shell, "--noprofile", "--norc", "-i"]
        elif shell_name == "zsh":
            shell_args = [shell, "-l", "-i"]
        else:
            shell_args = [shell]
        return self._run_as_user_command(username, shell_args)

    def _build_env(self) -> dict[str, str]:
        env = _ensure_terminal_env(os.environ.copy())
        username, home_dir, _shell = self._resolve_os_identity()
        if username:
            env["USER"] = username
            env["LOGNAME"] = username
        if home_dir and os.path.isdir(home_dir):
            env["HOME"] = home_dir
        if self._openai_api_key:
            env["OPENAI_API_KEY"] = self._openai_api_key
        if _codex_mcp_enabled():
            mcp_key_name = _codex_mcp_api_key_env_name()
            mcp_key_value = str(self._mcp_api_key_value or _codex_mcp_api_key_value()).strip()
            if mcp_key_name and mcp_key_value:
                env[mcp_key_name] = mcp_key_value
        return env

    def _build_cwd(self) -> str | None:
        # Do not chdir to the target user's home before privilege/user switch.
        # If that home is 0700, the current app user cannot enter it and process
        # launch fails with PermissionError before sudo/runuser executes.
        fallback_home = str(os.getenv("HOME", "")).strip()
        if fallback_home and os.path.isdir(fallback_home):
            return fallback_home
        return None


class LocalShellSession(PTYProcessSession):
    def __init__(self, user, *, openai_api_key: str = ""):
        super().__init__()
        self.user = user
        self._openai_api_key = str(openai_api_key or "").strip()
        self._mcp_api_key_value = ""
        self._resolved_username = ""
        self._resolved_home = ""

    async def start(self):
        await self._prepare_local_environment()
        await super().start()

    def _build_args(self) -> list[str]:
        shell = os.getenv("WEB_TERMINAL_LOCAL_SHELL", "").strip()
        if not shell:
            shell = shutil.which("bash") or shutil.which("sh") or "/bin/sh"
        if not os.path.exists(shell):
            raise RuntimeError(f"local shell not found at {shell}")
        auto_launch_codex = str(os.getenv("WEB_TERMINAL_AUTO_LAUNCH_CODEX", "1") or "").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        if auto_launch_codex:
            codex_command = str(os.getenv("WEB_TERMINAL_CODEX_COMMAND", "codex -p pro") or "").strip() or "codex -p pro"
            if shell.endswith("bash"):
                fallback_shell = f"exec {shlex.quote(shell)} --noprofile --norc -i"
            elif shell.endswith("zsh"):
                fallback_shell = f"exec {shlex.quote(shell)} -l -i"
            else:
                fallback_shell = f"exec {shlex.quote(shell)} -i"
            # Keep wrapper shell alive on Ctrl+C so Codex can exit and we still land in a shell prompt.
            startup_script = (
                "set +e; "
                "export TERM=\"${TERM:-xterm-256color}\"; "
                "export COLORTERM=\"${COLORTERM:-truecolor}\"; "
                "trap '' INT; "
                f"{codex_command}; "
                "trap - INT; "
                f"{fallback_shell}"
            )
            return [shell, "-lc", startup_script]
        if shell.endswith("bash"):
            return [shell, "--noprofile", "--norc", "-i"]
        return [shell]

    def _build_env(self) -> dict[str, str]:
        env = _ensure_terminal_env(os.environ.copy())
        local_username, local_home = self._resolve_local_identity()
        venv_dir = self._venv_dir(local_home)
        venv_bin = os.path.join(venv_dir, "bin")
        if local_username:
            env["USER"] = local_username
            env["LOGNAME"] = local_username
            env["PS1"] = f"{local_username}@\\h:\\w\\$ "
        if local_home and os.path.isdir(local_home):
            env["HOME"] = local_home
        if os.path.isdir(venv_bin):
            current_path = str(env.get("PATH") or "")
            env["PATH"] = f"{venv_bin}:{current_path}" if current_path else venv_bin
            env["VIRTUAL_ENV"] = venv_dir
        if self._openai_api_key:
            env["OPENAI_API_KEY"] = self._openai_api_key
        if _codex_mcp_enabled():
            mcp_key_name = _codex_mcp_api_key_env_name()
            mcp_key_value = str(self._mcp_api_key_value or _codex_mcp_api_key_value()).strip()
            if mcp_key_name and mcp_key_value:
                env[mcp_key_name] = mcp_key_value
        return env

    def _build_cwd(self) -> str | None:
        _, local_home = self._resolve_local_identity()
        if local_home and os.path.isdir(local_home):
            return local_home
        fallback_home = str(os.getenv("HOME", "")).strip()
        if fallback_home and os.path.isdir(fallback_home):
            return fallback_home
        return None

    @staticmethod
    def _venv_dir(home_dir: str) -> str:
        return os.path.join(home_dir, ".alshival", "venv")

    async def _prepare_local_environment(self):
        _, home_dir = self._resolve_local_identity()
        if not home_dir:
            return
        os.makedirs(home_dir, exist_ok=True)
        if not os.path.isdir(home_dir):
            return
        await asyncio.to_thread(self._prepare_local_environment_sync, home_dir)

    def _resolve_local_identity(self) -> tuple[str, str]:
        if self._resolved_home:
            return (self._resolved_username, self._resolved_home)

        force_static_identity = str(os.getenv("WEB_TERMINAL_FORCE_STATIC_IDENTITY", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        env_username = str(os.getenv("WEB_TERMINAL_LOCAL_USERNAME", "")).strip()
        env_home = str(os.getenv("WEB_TERMINAL_LOCAL_HOME", "")).strip()
        if force_static_identity and env_username and env_home:
            self._resolved_username = env_username
            self._resolved_home = env_home
            return (self._resolved_username, self._resolved_home)

        raw_username = str(getattr(self.user, "username", "") or f"user-{getattr(self.user, 'pk', 0)}").strip()
        safe_username = slugify(raw_username) or f"user-{getattr(self.user, 'pk', 0)}"
        user_pk = int(getattr(self.user, "pk", 0) or 0)
        local_username = f"{safe_username}-{user_pk}"

        user_data_root = str(getattr(settings, "USER_DATA_ROOT", "") or "").strip()
        if not user_data_root:
            user_data_root = str(settings.BASE_DIR / "var" / "user_data")
        home_root = str(os.getenv("WEB_TERMINAL_LOCAL_HOME_ROOT", user_data_root)).strip() or user_data_root
        local_home = os.path.join(home_root, local_username, "home")

        self._resolved_username = local_username
        self._resolved_home = local_home
        return (self._resolved_username, self._resolved_home)

    def _prepare_local_environment_sync(self, home_dir: str):
        if _codex_mcp_enabled():
            self._mcp_api_key_value = _get_runtime_codex_mcp_api_key_for_user(self.user)
        self._ensure_codex_profile_sync(home_dir)
        self._ensure_codex_api_key_sync(home_dir)
        venv_dir = self._venv_dir(home_dir)
        venv_python = os.path.join(venv_dir, "bin", "python")
        base_env = os.environ.copy()
        base_env["HOME"] = home_dir

        os.makedirs(os.path.dirname(venv_dir), exist_ok=True)
        if not os.path.exists(venv_python):
            subprocess.run(
                ["python3", "-m", "venv", venv_dir],
                env=base_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        if not os.path.exists(venv_python):
            return

        package_name = str(os.getenv("WEB_TERMINAL_OPENAI_PACKAGE", "openai")).strip() or "openai"
        has_sdk = subprocess.run(
            [venv_python, "-c", "import openai"],
            env=base_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if has_sdk.returncode == 0:
            return

        subprocess.run(
            [venv_python, "-m", "pip", "install", "--disable-pip-version-check", "--quiet", package_name],
            env=base_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def _ensure_codex_api_key_sync(self, home_dir: str) -> None:
        if not self._openai_api_key:
            return
        codex_bin = _resolve_codex_binary()
        if not codex_bin:
            return
        base_env = os.environ.copy()
        base_env["HOME"] = home_dir
        try:
            result = subprocess.run(
                [codex_bin, "login", "--with-api-key"],
                env=base_env,
                input=f"{self._openai_api_key}\n".encode("utf-8"),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if result.returncode != 0:
                logger.warning(
                    "Failed to persist Codex API key for local terminal user '%s'.",
                    self._resolve_local_identity()[0],
                )
        except Exception:
            logger.warning(
                "Failed to provision Codex API key for local terminal user '%s'.",
                self._resolve_local_identity()[0],
            )

    def _ensure_codex_profile_sync(self, home_dir: str) -> None:
        config_dir = os.path.join(home_dir, ".codex")
        config_path = os.path.join(config_dir, "config.toml")
        try:
            os.makedirs(config_dir, exist_ok=True)
        except OSError:
            return

        try:
            raw = ""
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as handle:
                    raw = handle.read()
            lines = raw.splitlines()

            def upsert_section(source_lines: list[str], header: str, section_lines: list[str]) -> list[str]:
                idx = -1
                for i, line in enumerate(source_lines):
                    if line.strip() == header:
                        idx = i
                        break

                if idx == -1:
                    if source_lines and source_lines[-1].strip():
                        source_lines.append("")
                    source_lines.append(header)
                    source_lines.extend(section_lines)
                    return source_lines

                end = len(source_lines)
                for j in range(idx + 1, len(source_lines)):
                    if source_lines[j].lstrip().startswith("["):
                        end = j
                        break

                for target_line in section_lines:
                    key = target_line.split("=", 1)[0].strip()
                    match_idx = -1
                    for j in range(idx + 1, end):
                        stripped = source_lines[j].strip()
                        if not stripped or stripped.startswith("#"):
                            continue
                        if stripped.startswith(f"{key} ") or stripped.startswith(f"{key}="):
                            match_idx = j
                            break

                    if match_idx != -1:
                        source_lines[match_idx] = target_line
                    else:
                        source_lines.insert(end, target_line)
                        end += 1
                return source_lines

            lines = upsert_section(
                lines,
                "[profiles.pro]",
                [
                    'approval_policy = "never"',
                    'sandbox_mode = "danger-full-access"',
                ],
            )

            mcp_config_enabled = _codex_mcp_enabled()
            mcp_enabled = mcp_config_enabled and bool(str(self._mcp_api_key_value or _codex_mcp_api_key_value()).strip())
            if mcp_config_enabled:
                lines = upsert_section(
                    lines,
                    f"[mcp_servers.{_codex_mcp_server_name()}]",
                    [
                        f'url = "{_codex_mcp_url()}"',
                        "startup_timeout_sec = 20",
                        "tool_timeout_sec = 90",
                        f"enabled = {'true' if mcp_enabled else 'false'}",
                        f'bearer_token_env_var = "{_codex_mcp_api_key_env_name()}"',
                    ],
                )

            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write("\n".join(lines).rstrip() + "\n")
        except OSError:
            return


class HostResourceSSHSession(HostShellSession):
    def __init__(self, user, config: SSHConfig, *, openai_api_key: str = ""):
        super().__init__(user, openai_api_key=openai_api_key)
        self.config = config
        self.cleanup_paths = [config.key_path]

    async def start(self):
        await asyncio.to_thread(self._ensure_target_account)
        await PTYProcessSession.start(self)

    def _build_args(self) -> list[str]:
        username, _home, os_shell = self._resolve_os_identity()
        if not username:
            raise RuntimeError("Unable to resolve host terminal account.")
        shell = str(os.getenv("WEB_TERMINAL_HOST_SHELL", "") or "").strip() or os_shell
        if not shell:
            shell = shutil.which("bash") or shutil.which("sh") or "/bin/sh"
        if not os.path.exists(shell):
            raise RuntimeError(f"host shell not found at {shell}")

        ssh_command = " ".join(shlex.quote(part) for part in _build_ssh_exec_args(self.config))
        fallback_shell = _interactive_shell_exec_command(shell)
        startup_script = (
            "set +e; "
            "cd \"$HOME\" >/dev/null 2>&1 || true; "
            "export TERM=\"${TERM:-xterm-256color}\"; "
            "export COLORTERM=\"${COLORTERM:-truecolor}\"; "
            f"{ssh_command}; "
            "ssh_status=$?; "
            "if [ \"$ssh_status\" -ne 0 ]; then "
            "echo ''; "
            "echo \"[ssh session ended with status ${ssh_status}]\"; "
            "fi; "
            f"{fallback_shell}"
        )
        return self._run_as_user_command(username, [shell, "-lc", startup_script])


class LocalResourceSSHSession(LocalShellSession):
    def __init__(self, user, config: SSHConfig, *, openai_api_key: str = ""):
        super().__init__(user, openai_api_key=openai_api_key)
        self.config = config
        self.cleanup_paths = [config.key_path]

    async def start(self):
        _username, home_dir = self._resolve_local_identity()
        if home_dir:
            os.makedirs(home_dir, exist_ok=True)
        await PTYProcessSession.start(self)

    def _build_args(self) -> list[str]:
        shell = str(os.getenv("WEB_TERMINAL_LOCAL_SHELL", "") or "").strip()
        if not shell:
            shell = shutil.which("bash") or shutil.which("sh") or "/bin/sh"
        if not os.path.exists(shell):
            raise RuntimeError(f"local shell not found at {shell}")

        ssh_command = " ".join(shlex.quote(part) for part in _build_ssh_exec_args(self.config))
        fallback_shell = _interactive_shell_exec_command(shell)
        startup_script = (
            "set +e; "
            "cd \"$HOME\" >/dev/null 2>&1 || true; "
            "export TERM=\"${TERM:-xterm-256color}\"; "
            "export COLORTERM=\"${COLORTERM:-truecolor}\"; "
            f"{ssh_command}; "
            "ssh_status=$?; "
            "if [ \"$ssh_status\" -ne 0 ]; then "
            "echo ''; "
            "echo \"[ssh session ended with status ${ssh_status}]\"; "
            "fi; "
            f"{fallback_shell}"
        )
        return [shell, "-lc", startup_script]


class AgentSession:
    def __init__(self, user):
        self.user = user
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._buffer = ""
        self._closed = False

    async def start(self):
        banner = (
            "Alshival Agent Terminal\r\n"
            "Type help for commands. Ctrl+C will clear the current line.\r\n\r\n"
        )
        await self._queue.put(banner.encode("utf-8"))
        await self._queue.put(b"alshival> ")

    async def stream_to_websocket(self, send):
        while True:
            payload = await self._queue.get()
            if payload is None:
                break
            await send({"type": "websocket.send", "bytes": payload})
        try:
            await send({"type": "websocket.close", "code": 1000})
        except Exception:
            return

    async def write(self, payload):
        if self._closed:
            return
        data = payload.encode("utf-8", errors="ignore") if isinstance(payload, str) else bytes(payload)
        if not data:
            return
        await self._queue.put(data)

        text = data.decode("utf-8", errors="ignore")
        for ch in text:
            if ch in {"\r", "\n"}:
                command = self._buffer.strip()
                self._buffer = ""
                response = _agent_response(command, self.user)
                if response:
                    await self._queue.put((response + "\r\n").encode("utf-8"))
                await self._queue.put(b"alshival> ")
            elif ch in {"\x7f", "\b"}:
                self._buffer = self._buffer[:-1]
            elif ch.isprintable() or ch == " ":
                self._buffer += ch

    def resize(self, cols: int, rows: int):
        return

    async def stop(self):
        if self._closed:
            return
        self._closed = True
        await self._queue.put(None)


def resolve_local_shell_identity_for_user(user) -> tuple[str, str]:
    session = LocalShellSession(user)
    return session._resolve_local_identity()


def ensure_local_shell_home_for_user(user) -> str:
    _local_username, local_home = resolve_local_shell_identity_for_user(user)
    if not local_home:
        return ""
    try:
        os.makedirs(local_home, exist_ok=True)
    except OSError:
        return ""
    return local_home if os.path.isdir(local_home) else ""


def _agent_response(command: str, user) -> str:
    raw = str(command or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered in {"help", "?", "man"}:
        return (
            "Commands:\r\n"
            "  help         Show this help\r\n"
            "  whoami       Show current user\r\n"
            "  status       Show agent status\r\n"
            "  ping         Basic connectivity check\r\n"
            "  clear        Clear recommendation"
        )
    if lowered == "whoami":
        username = str(getattr(user, "username", "") or f"user-{getattr(user, 'id', 0)}")
        role = "superuser" if bool(getattr(user, "is_superuser", False)) else "member"
        return f"{username} ({role})"
    if lowered == "status":
        ts = datetime.now(timezone.utc).isoformat()
        return f"agent online; utc={ts}"
    if lowered == "ping":
        return "pong"
    if lowered == "clear":
        return "Use Ctrl+L to clear your terminal viewport."
    return f"You said: {raw}"


def _owner_for_resource_uuid(resource_uuid: str):
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return None

    package = (
        ResourcePackageOwner.objects.select_related("owner_user")
        .filter(resource_uuid=resolved_uuid)
        .first()
    )
    if package is not None and package.owner_user_id:
        return package.owner_user

    alias = (
        ResourceRouteAlias.objects.select_related("owner_user")
        .filter(resource_uuid=resolved_uuid, owner_user_id__isnull=False)
        .order_by("-is_current", "-updated_at", "-created_at")
        .first()
    )
    if alias is not None and alias.owner_user_id:
        return alias.owner_user

    share = (
        ResourceTeamShare.objects.select_related("owner")
        .filter(resource_uuid=resolved_uuid, owner_id__isnull=False)
        .order_by("-updated_at", "-created_at")
        .first()
    )
    if share is not None and share.owner_id and bool(getattr(share.owner, "is_active", False)):
        return share.owner
    return None


def _resolve_owner_and_resource_for_uuid(*, actor, resource_uuid: str):
    resolved_uuid = str(resource_uuid or "").strip()
    if not resolved_uuid:
        return None, None

    candidate_users: list[object] = []
    seen_user_ids: set[int] = set()

    owner = _owner_for_resource_uuid(resolved_uuid)
    if owner is not None and bool(getattr(owner, "is_active", False)):
        owner_id = int(getattr(owner, "id", 0) or 0)
        if owner_id > 0:
            candidate_users.append(owner)
            seen_user_ids.add(owner_id)

    for row in (
        ResourceRouteAlias.objects.select_related("owner_user")
        .filter(resource_uuid=resolved_uuid, owner_user_id__isnull=False)
        .order_by("-is_current", "-updated_at")
    ):
        owner_user = row.owner_user
        if owner_user is None or not bool(owner_user.is_active):
            continue
        owner_user_id = int(owner_user.id)
        if owner_user_id in seen_user_ids:
            continue
        candidate_users.append(owner_user)
        seen_user_ids.add(owner_user_id)

    for row in (
        ResourceTeamShare.objects.select_related("owner")
        .filter(resource_uuid=resolved_uuid, owner_id__isnull=False)
        .order_by("-updated_at", "-created_at")
    ):
        owner_user = row.owner
        if owner_user is None or not bool(owner_user.is_active):
            continue
        owner_user_id = int(owner_user.id)
        if owner_user_id in seen_user_ids:
            continue
        candidate_users.append(owner_user)
        seen_user_ids.add(owner_user_id)

    if actor is not None and bool(getattr(actor, "is_active", False)):
        actor_id = int(getattr(actor, "id", 0) or 0)
        if actor_id > 0 and actor_id not in seen_user_ids:
            candidate_users.append(actor)
            seen_user_ids.add(actor_id)

    User = get_user_model()
    for user in User.objects.filter(is_active=True).order_by("id"):
        user_id = int(user.id)
        if user_id in seen_user_ids:
            continue
        candidate_users.append(user)
        seen_user_ids.add(user_id)

    for owner_user in candidate_users:
        resource = get_resource_by_uuid(owner_user, resolved_uuid)
        if resource is not None:
            return owner_user, resource
    return None, None


async def _build_terminal_session(scope, user):
    query = scope.get("query_string") or b""
    query_params = parse_qs(query.decode("utf-8", errors="ignore"))
    mode = str((query_params.get("mode") or [""])[0] or "").strip().lower()

    if mode == "shell":
        if not bool(getattr(user, "is_staff", False)):
            raise PermissionError("Local shell access is restricted to staff.")
        openai_api_key = await _resolve_terminal_openai_api_key()
        # Local shell mode is the default for Ask Alshival shell sessions.
        # Host shell mode remains available as an explicit opt-in.
        prefer_host_shell = str(os.getenv("WEB_TERMINAL_PREFER_HOST_SHELL", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if prefer_host_shell and HostShellSession._can_switch_users():
            return HostShellSession(user, openai_api_key=openai_api_key)
        return LocalShellSession(user, openai_api_key=openai_api_key)

    if mode == "agent":
        return AgentSession(user)

    resource_id = str((query_params.get("resource_id") or [""])[0] or "").strip()
    resource_uuid = str((query_params.get("resource_uuid") or [""])[0] or "").strip().lower()
    if not resource_id and not resource_uuid:
        raise ValueError("resource_id or resource_uuid is required.")
    ssh_config = await _user_resource_ssh_config(
        user,
        resource_id=resource_id,
        resource_uuid=resource_uuid,
    )
    if bool(getattr(user, "is_staff", False)):
        openai_api_key = await _resolve_terminal_openai_api_key()
        if HostShellSession._can_switch_users():
            return HostResourceSSHSession(user, ssh_config, openai_api_key=openai_api_key)
        return LocalResourceSSHSession(user, ssh_config, openai_api_key=openai_api_key)
    return SSHSession(ssh_config)


@sync_to_async
def _resolve_terminal_openai_api_key() -> str:
    setup = SystemSetup.objects.order_by("-updated_at", "-created_at").first()
    configured = str(getattr(setup, "openai_api_key", "") or "").strip() if setup is not None else ""
    if configured:
        return configured
    return str(os.getenv("OPENAI_API_KEY", "") or "").strip()


@sync_to_async
def _user_resource_ssh_config(user, *, resource_id: str = "", resource_uuid: str = "") -> SSHConfig:
    resolved_uuid = str(resource_uuid or "").strip().lower()
    owner_user = None
    resource = None

    if resolved_uuid:
        if not user_can_access_resource(user=user, resource_uuid=resolved_uuid):
            raise PermissionError("Resource access denied.")
        owner_user, resource = _resolve_owner_and_resource_for_uuid(
            actor=user,
            resource_uuid=resolved_uuid,
        )
    else:
        try:
            resolved_resource_id = int(resource_id)
        except (TypeError, ValueError):
            raise ValueError("resource_id must be an integer.")
        resource = get_resource(user, resolved_resource_id)
        if resource is not None:
            resolved_uuid = str(getattr(resource, "resource_uuid", "") or "").strip().lower()
            if resolved_uuid and not user_can_access_resource(user=user, resource_uuid=resolved_uuid):
                raise PermissionError("Resource access denied.")
            owner_user = _owner_for_resource_uuid(resolved_uuid) if resolved_uuid else None

    if not resource:
        raise PermissionError("Resource access denied.")
    credential_owner = owner_user or user
    if resource.resource_type != "vm":
        raise ValueError("Resource is not a virtual machine.")
    if not resource.address:
        raise ValueError("Resource host is missing.")
    if not resource.ssh_username:
        raise ValueError("SSH username is missing.")

    key_text = _decrypt_key_text(resource.ssh_key_text)
    if not key_text and resource.ssh_credential_id:
        credential_lookup = None
        raw_credential_id = (resource.ssh_credential_id or "").strip()
        if raw_credential_id.startswith("global:"):
            try:
                credential_lookup = get_global_ssh_private_key(credential_id=int(raw_credential_id.split(":", 1)[1]))
            except (TypeError, ValueError):
                credential_lookup = None
        else:
            local_raw = raw_credential_id
            if raw_credential_id.startswith("local:"):
                local_raw = raw_credential_id.split(":", 1)[1]
            credential_lookup = get_ssh_credential_private_key(credential_owner, local_raw)
        if credential_lookup:
            key_text = credential_lookup
    if not key_text:
        raise ValueError("No SSH key available for this resource.")

    with NamedTemporaryFile(delete=False, prefix="alshival-key-", suffix=".key", mode="w") as tmp:
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
    )
