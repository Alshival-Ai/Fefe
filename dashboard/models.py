from django.conf import settings
from django.contrib.auth.models import Group
from django.db import models


class SystemSetup(models.Model):
    is_completed = models.BooleanField(default=False)
    openai_api_key = models.CharField(max_length=255, blank=True, default="")
    ingest_api_key = models.CharField(max_length=255, blank=True, default="")
    twilio_account_sid = models.CharField(max_length=64, blank=True, default="")
    twilio_auth_token = models.CharField(max_length=255, blank=True, default="")
    twilio_from_number = models.CharField(max_length=32, blank=True, default="")
    monitoring_enabled = models.BooleanField(default=True)
    maintenance_mode = models.BooleanField(default=False)
    maintenance_message = models.CharField(max_length=255, blank=True, default="")
    default_model = models.CharField(max_length=120, blank=True, default="gpt-4.1-mini")
    microsoft_login_enabled = models.BooleanField(default=False)
    github_login_enabled = models.BooleanField(default=False)
    ask_github_mcp_enabled = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]

    def __str__(self) -> str:
        return f"SystemSetup(completed={self.is_completed})"


class GlobalTeamSSHCredential(models.Model):
    name = models.CharField(max_length=120)
    team_name = models.CharField(max_length=120, blank=True, default="")
    encrypted_private_key = models.TextField()
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_global_team_ssh_keys_created",
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]
        indexes = [
            models.Index(fields=["is_active", "updated_at"], name="dash_gl_ssh_active_upd_idx"),
            models.Index(fields=["team_name", "is_active"], name="dash_gl_ssh_team_active_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.team_name or 'Global'}:{self.name}"


class GlobalTeamAPIKey(models.Model):
    name = models.CharField(max_length=120)
    team_name = models.CharField(max_length=120, blank=True, default="")
    key_prefix = models.CharField(max_length=80)
    key_hash = models.CharField(max_length=128)
    encrypted_key = models.TextField(blank=True, default="")
    expires_at = models.DateTimeField(null=True, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_global_team_api_keys_created",
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]
        indexes = [
            models.Index(fields=["is_active", "updated_at"], name="dash_gl_api_active_upd_idx"),
            models.Index(fields=["team_name", "is_active"], name="dash_gl_api_team_active_idx"),
            models.Index(fields=["key_prefix"], name="dash_gl_api_prefix_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.team_name or 'Global'}:{self.name}"


class UserFeatureAccess(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="dashboard_feature_access",
    )
    feature_key = models.CharField(max_length=64)
    is_enabled = models.BooleanField(default=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_feature_access_updated",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["feature_key", "-updated_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["user", "feature_key"],
                name="dash_user_feature_ux",
            ),
        ]
        indexes = [
            models.Index(fields=["feature_key", "is_enabled"], name="dash_feat_key_enabled_idx"),
            models.Index(fields=["user", "is_enabled"], name="dash_feat_user_enabled_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}:{self.feature_key}:{'on' if self.is_enabled else 'off'}"


class UserNotificationSettings(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="dashboard_notification_settings",
    )
    phone_number = models.CharField(max_length=32, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]
        indexes = [
            models.Index(fields=["phone_number"], name="dash_user_notif_phone_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}:{self.phone_number or 'no-phone'}"


class ResourceTeamShare(models.Model):
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="dashboard_resource_team_shares",
    )
    resource_uuid = models.CharField(max_length=64)
    resource_name = models.CharField(max_length=255, blank=True, default="")
    team = models.ForeignKey(
        Group,
        on_delete=models.CASCADE,
        related_name="dashboard_resource_team_shares",
    )
    granted_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_team_shares_created",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["owner", "resource_uuid", "team"],
                name="dash_res_team_owner_uuid_team_ux",
            ),
        ]
        indexes = [
            models.Index(fields=["owner", "team"], name="dash_res_team_owner_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.owner_id}:{self.resource_uuid}:{self.team.name}"


class ResourcePackageOwner(models.Model):
    OWNER_SCOPE_USER = "user"
    OWNER_SCOPE_TEAM = "team"
    OWNER_SCOPE_GLOBAL = "global"
    OWNER_SCOPE_CHOICES = [
        (OWNER_SCOPE_USER, "User"),
        (OWNER_SCOPE_TEAM, "Team"),
        (OWNER_SCOPE_GLOBAL, "Global"),
    ]

    resource_uuid = models.CharField(max_length=64, unique=True)
    owner_scope = models.CharField(max_length=16, choices=OWNER_SCOPE_CHOICES, default=OWNER_SCOPE_USER)
    owner_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_packages_owned",
    )
    owner_team = models.ForeignKey(
        Group,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_packages_owned",
    )
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_packages_created",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_packages_updated",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]
        indexes = [
            models.Index(fields=["owner_scope", "updated_at"], name="dash_pkg_owner_scope_idx"),
            models.Index(fields=["owner_user", "updated_at"], name="dash_pkg_owner_user_idx"),
            models.Index(fields=["owner_team", "updated_at"], name="dash_pkg_owner_team_idx"),
        ]

    def __str__(self) -> str:
        if self.owner_scope == self.OWNER_SCOPE_TEAM and self.owner_team_id:
            return f"{self.resource_uuid}:team:{self.owner_team_id}"
        if self.owner_scope == self.OWNER_SCOPE_GLOBAL:
            return f"{self.resource_uuid}:global"
        return f"{self.resource_uuid}:user:{self.owner_user_id or 0}"


class ResourceRouteAlias(models.Model):
    ROUTE_KIND_USER = "user"
    ROUTE_KIND_TEAM = "team"
    ROUTE_KIND_CHOICES = [
        (ROUTE_KIND_USER, "User"),
        (ROUTE_KIND_TEAM, "Team"),
    ]

    resource_uuid = models.CharField(max_length=64)
    route_kind = models.CharField(max_length=16, choices=ROUTE_KIND_CHOICES, default=ROUTE_KIND_USER)
    route_value = models.CharField(max_length=120)
    owner_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_route_aliases",
    )
    is_current = models.BooleanField(default=False)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_route_aliases_created",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_resource_route_aliases_updated",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-is_current", "-updated_at", "-created_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["resource_uuid", "route_kind", "route_value"],
                name="dash_route_alias_uuid_kind_value_ux",
            ),
        ]
        indexes = [
            models.Index(fields=["resource_uuid", "is_current"], name="dash_route_alias_uuid_curr_idx"),
            models.Index(fields=["route_kind", "route_value"], name="dash_route_alias_kind_val_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.resource_uuid}:{self.route_kind}:{self.route_value}:{'current' if self.is_current else 'old'}"


class WikiPage(models.Model):
    path = models.CharField(max_length=220, unique=True)
    title = models.CharField(max_length=220)
    is_draft = models.BooleanField(default=False)
    body_markdown = models.TextField(blank=True, default="")
    body_html_fallback = models.TextField(blank=True, default="")
    team_access = models.ManyToManyField(
        Group,
        blank=True,
        related_name="dashboard_wiki_pages",
    )
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_wiki_pages_created",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_wiki_pages_updated",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["path", "-updated_at"]
        indexes = [
            models.Index(fields=["path"], name="dash_wiki_path_idx"),
            models.Index(fields=["is_draft", "updated_at"], name="dash_wiki_draft_upd_idx"),
            models.Index(fields=["updated_at"], name="dash_wiki_updated_idx"),
        ]

    def __str__(self) -> str:
        return self.path
