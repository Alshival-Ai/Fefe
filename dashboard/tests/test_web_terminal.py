import asyncio
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import SimpleTestCase, TestCase, override_settings

from dashboard.models import ResourceRouteAlias, ResourceTeamShare
from dashboard.request_auth import user_can_access_resource
from dashboard.resources_store import add_resource, get_resource
from dashboard.web_terminal import (
    HostShellSession,
    LocalShellSession,
    _build_terminal_session,
    _user_resource_ssh_config,
)


class WebTerminalAccessTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls._override = override_settings(
            USER_DATA_ROOT=str(Path(cls._tmp.name) / "user_data"),
            TEAM_DATA_ROOT=str(Path(cls._tmp.name) / "team_data"),
            GLOBAL_DATA_ROOT=str(Path(cls._tmp.name) / "global_data"),
        )
        cls._override.enable()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        try:
            cls._override.disable()
        finally:
            cls._tmp.cleanup()
        super().tearDownClass()

    def _create_user(self, username: str):
        User = get_user_model()
        return User.objects.create_user(
            username=username,
            password="pass1234",
            email=f"{username}@example.com",
        )

    def _create_vm_resource(self, *, owner, name: str, access_scope: str = "account", team_names=None):
        resource_id = add_resource(
            owner,
            name=name,
            resource_type="vm",
            target="10.0.0.20",
            address="10.0.0.20",
            notes="",
            ssh_username="ubuntu",
            ssh_key_text="dummy-private-key",
            ssh_port="22",
            access_scope=access_scope,
            team_names=team_names or [],
        )
        resource = get_resource(owner, resource_id)
        self.assertIsNotNone(resource)
        return resource

    def _resolve_ssh_config(self, *, user, resource_uuid: str):
        # `_user_resource_ssh_config` is wrapped with `sync_to_async`; call the
        # original sync function in tests to avoid SQLite thread lock behavior.
        sync_func = getattr(_user_resource_ssh_config, "func", None)
        if callable(sync_func):
            return sync_func(user=user, resource_uuid=resource_uuid)
        return asyncio.run(_user_resource_ssh_config(user=user, resource_uuid=resource_uuid))

    def test_team_owned_resource_allows_member_without_route_alias(self):
        owner = self._create_user("owner_team_case")
        member = self._create_user("member_team_case")
        team = Group.objects.create(name="Team Without Alias")
        owner.groups.add(team)
        member.groups.add(team)

        resource = self._create_vm_resource(
            owner=owner,
            name="team-owned-vm",
            access_scope="team",
            team_names=[team.name],
        )
        self.assertFalse(ResourceRouteAlias.objects.filter(resource_uuid=resource.resource_uuid).exists())
        self.assertTrue(user_can_access_resource(user=member, resource_uuid=resource.resource_uuid))

        ssh_config = self._resolve_ssh_config(user=member, resource_uuid=resource.resource_uuid)
        self.assertEqual(ssh_config.host, "10.0.0.20")
        self.assertEqual(ssh_config.username, "ubuntu")
        Path(ssh_config.key_path).unlink(missing_ok=True)

    def test_shared_account_resource_allows_team_member(self):
        owner = self._create_user("owner_share_case")
        member = self._create_user("member_share_case")
        team = Group.objects.create(name="Shared Team")
        member.groups.add(team)

        resource = self._create_vm_resource(owner=owner, name="shared-account-vm")
        self.assertFalse(user_can_access_resource(user=member, resource_uuid=resource.resource_uuid))
        ResourceTeamShare.objects.create(
            owner=owner,
            resource_uuid=resource.resource_uuid,
            resource_name=resource.name,
            team=team,
            granted_by=owner,
        )
        self.assertTrue(user_can_access_resource(user=member, resource_uuid=resource.resource_uuid))

        ssh_config = self._resolve_ssh_config(user=member, resource_uuid=resource.resource_uuid)
        self.assertEqual(ssh_config.host, "10.0.0.20")
        self.assertEqual(ssh_config.username, "ubuntu")
        Path(ssh_config.key_path).unlink(missing_ok=True)


class WebTerminalShellModeSelectionTests(SimpleTestCase):
    def _scope(self, query: str = "mode=shell") -> dict:
        return {"query_string": query.encode("utf-8")}

    def _staff_user(self):
        return SimpleNamespace(is_staff=True, is_active=True, username="staff-user", pk=1)

    def test_shell_mode_defaults_to_local_session(self):
        with patch("dashboard.web_terminal._resolve_terminal_openai_api_key", new=AsyncMock(return_value="")), \
             patch.object(HostShellSession, "_can_switch_users", return_value=True), \
             patch.dict(os.environ, {}, clear=False):
            os.environ.pop("WEB_TERMINAL_PREFER_HOST_SHELL", None)
            session = asyncio.run(_build_terminal_session(self._scope(), self._staff_user()))
        self.assertIsInstance(session, LocalShellSession)

    def test_shell_mode_uses_host_when_explicitly_enabled(self):
        with patch("dashboard.web_terminal._resolve_terminal_openai_api_key", new=AsyncMock(return_value="")), \
             patch.object(HostShellSession, "_can_switch_users", return_value=True), \
             patch.dict(os.environ, {"WEB_TERMINAL_PREFER_HOST_SHELL": "1"}, clear=False):
            session = asyncio.run(_build_terminal_session(self._scope(), self._staff_user()))
        self.assertIsInstance(session, HostShellSession)
