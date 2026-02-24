from dataclasses import dataclass

from .models import GlobalTeamSSHCredential
from .resources_store import _decrypt_key_text, _encrypt_key_text, _is_encrypted


@dataclass
class GlobalSSHCredentialItem:
    id: int
    name: str
    team_name: str
    created_at: str


def list_global_ssh_credentials() -> list[GlobalSSHCredentialItem]:
    rows = list(
        GlobalTeamSSHCredential.objects.filter(is_active=True)
        .order_by("-updated_at")
        .values("id", "name", "team_name", "created_at", "encrypted_private_key")
    )
    changed_ids: list[int] = []
    for row in rows:
        key_text = (row.get("encrypted_private_key") or "").strip()
        if key_text and not _is_encrypted(key_text):
            changed_ids.append(int(row["id"]))
    if changed_ids:
        for row in rows:
            row_id = int(row["id"])
            if row_id in changed_ids:
                key_text = (row.get("encrypted_private_key") or "").strip()
                GlobalTeamSSHCredential.objects.filter(id=row_id).update(
                    encrypted_private_key=_encrypt_key_text(key_text),
                )
        rows = list(
            GlobalTeamSSHCredential.objects.filter(is_active=True)
            .order_by("-updated_at")
            .values("id", "name", "team_name", "created_at")
        )

    return [
        GlobalSSHCredentialItem(
            id=int(row["id"]),
            name=str(row["name"]),
            team_name=str(row.get("team_name") or ""),
            created_at=str(row["created_at"]),
        )
        for row in rows
    ]


def add_global_ssh_credential(
    *,
    user,
    name: str,
    team_name: str,
    private_key_text: str,
) -> int:
    created = GlobalTeamSSHCredential.objects.create(
        name=name,
        team_name=team_name,
        encrypted_private_key=_encrypt_key_text(private_key_text),
        created_by=user,
    )
    return int(created.id)


def delete_global_ssh_credential(*, credential_id: int) -> None:
    GlobalTeamSSHCredential.objects.filter(id=credential_id).update(is_active=False)


def get_global_ssh_private_key(*, credential_id: int) -> str | None:
    row = (
        GlobalTeamSSHCredential.objects.filter(id=credential_id, is_active=True)
        .values("encrypted_private_key")
        .first()
    )
    if not row:
        return None
    decrypted = _decrypt_key_text((row.get("encrypted_private_key") or "").strip())
    if not decrypted:
        return None
    return decrypted
