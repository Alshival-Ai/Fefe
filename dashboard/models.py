from django.conf import settings
from django.db import models


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
