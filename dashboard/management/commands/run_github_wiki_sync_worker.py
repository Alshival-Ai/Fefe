from __future__ import annotations

import time
from datetime import datetime, timezone

from allauth.socialaccount.models import SocialAccount
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db.utils import OperationalError, ProgrammingError

from dashboard.github_wiki_sync_service import (
    resource_github_repository_names,
    sync_resource_wiki_with_github,
)
from dashboard.knowledge_store import upsert_resource_health_knowledge
from dashboard.resources_store import list_resources


class Command(BaseCommand):
    help = "Periodically pull linked GitHub wiki pages into resource wiki cache."

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval-seconds",
            type=int,
            default=3600,
            help="Polling interval in seconds (default: 3600).",
        )
        parser.add_argument(
            "--user",
            action="append",
            default=[],
            help="Optional username/email filter (repeatable).",
        )
        parser.add_argument(
            "--max-users",
            type=int,
            default=0,
            help="Optional max users to process each cycle (0 = all).",
        )
        parser.add_argument(
            "--max-resources",
            type=int,
            default=0,
            help="Optional max resources to sync each cycle (0 = all discovered).",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run a single sync cycle and exit.",
        )

    def _github_connected_user_ids(self) -> set[int]:
        try:
            return {
                int(value)
                for value in SocialAccount.objects.filter(provider="github").values_list("user_id", flat=True)
            }
        except (OperationalError, ProgrammingError):
            return set()
        except Exception:
            return set()

    def _target_users(self, *, user_filters: list[str], max_users: int):
        user_model = get_user_model()
        queryset = user_model.objects.filter(is_active=True)

        cleaned_filters = [str(value or "").strip() for value in user_filters if str(value or "").strip()]
        if cleaned_filters:
            identity_filter = Q()
            for value in cleaned_filters:
                identity_filter |= Q(username__iexact=value)
                identity_filter |= Q(email__iexact=value)
            queryset = queryset.filter(identity_filter)
        else:
            connected_ids = self._github_connected_user_ids()
            if connected_ids:
                queryset = queryset.filter(id__in=sorted(connected_ids))
            else:
                queryset = queryset.none()

        queryset = queryset.order_by("id")
        if max_users > 0:
            queryset = queryset[:max_users]
        return queryset

    def _run_cycle(
        self,
        *,
        user_filters: list[str],
        max_users: int,
        max_resources: int,
    ) -> dict[str, int]:
        totals = {
            "users": 0,
            "resources": 0,
            "synced": 0,
            "partial": 0,
            "unavailable": 0,
            "failed": 0,
            "skipped": 0,
        }

        processed_resource_uuids: set[str] = set()
        stop_early = False

        for user in self._target_users(user_filters=user_filters, max_users=max_users).iterator():
            if stop_early:
                break
            totals["users"] += 1
            try:
                resources = list(list_resources(user))
            except Exception as exc:
                totals["failed"] += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"[github-wiki-sync-worker] user={user.get_username()} status=error error=list_resources_failed:{exc}"
                    )
                )
                continue

            for resource in resources:
                resource_uuid = str(getattr(resource, "resource_uuid", "") or "").strip().lower()
                if not resource_uuid or resource_uuid in processed_resource_uuids:
                    continue

                repositories = resource_github_repository_names(resource)
                if not repositories:
                    totals["skipped"] += 1
                    continue

                if max_resources > 0 and totals["resources"] >= max_resources:
                    stop_early = True
                    break

                processed_resource_uuids.add(resource_uuid)
                totals["resources"] += 1

                try:
                    result = sync_resource_wiki_with_github(
                        actor=user,
                        resource=resource,
                        token_users=[user],
                        pull_remote=True,
                        push_changes=False,
                    )
                except Exception as exc:
                    totals["failed"] += 1
                    self.stderr.write(
                        self.style.ERROR(
                            f"[github-wiki-sync-worker] user={user.get_username()} resource={resource_uuid} "
                            f"status=error error=sync_exception:{exc}"
                        )
                    )
                    continue

                code = str(result.get("code") or "").strip().lower()
                if code in {"ok", "partial_error"}:
                    try:
                        checked_at = str(getattr(resource, "last_checked_at", "") or "").strip()
                        if not checked_at:
                            checked_at = datetime.now(timezone.utc).isoformat()
                        upsert_resource_health_knowledge(
                            user=user,
                            resource=resource,
                            status=str(getattr(resource, "last_status", "") or "").strip().lower() or "unknown",
                            checked_at=checked_at,
                            error=str(getattr(resource, "last_error", "") or "").strip(),
                            check_method="wiki_sync_worker",
                            latency_ms=None,
                            packet_loss_pct=None,
                        )
                    except Exception as exc:
                        self.stderr.write(
                            self.style.WARNING(
                                f"[github-wiki-sync-worker] user={user.get_username()} resource={resource_uuid} "
                                f"kb_reindex_error={exc}"
                            )
                        )
                if code == "ok":
                    totals["synced"] += 1
                    continue
                if code == "partial_error":
                    totals["partial"] += 1
                elif code in {"missing_github_repositories", "missing_github_token"}:
                    totals["unavailable"] += 1
                else:
                    totals["failed"] += 1

                errors = result.get("errors") if isinstance(result.get("errors"), list) else []
                error_detail = "; ".join(str(item) for item in errors[:3]) if errors else code
                self.stderr.write(
                    self.style.WARNING(
                        f"[github-wiki-sync-worker] user={user.get_username()} resource={resource_uuid} "
                        f"status={code or 'unknown'} detail={error_detail}"
                    )
                )

        return totals

    def handle(self, *args, **options):
        interval_seconds = max(300, int(options.get("interval_seconds") or 3600))
        user_filters = list(options.get("user") or [])
        max_users = max(0, int(options.get("max_users") or 0))
        max_resources = max(0, int(options.get("max_resources") or 0))
        run_once = bool(options.get("once"))

        self.stdout.write(
            "[github-wiki-sync-worker] started "
            f"interval={interval_seconds}s once={run_once} max_users={max_users or 'all'} "
            f"max_resources={max_resources or 'all'}"
        )

        while True:
            started = time.monotonic()
            try:
                totals = self._run_cycle(
                    user_filters=user_filters,
                    max_users=max_users,
                    max_resources=max_resources,
                )
                self.stdout.write(
                    self.style.SUCCESS(
                        "[github-wiki-sync-worker] cycle users={users} resources={resources} synced={synced} "
                        "partial={partial} unavailable={unavailable} failed={failed} skipped={skipped}".format(**totals)
                    )
                )
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f"[github-wiki-sync-worker] cycle failed: {exc}"))
                if run_once:
                    raise

            if run_once:
                return

            elapsed = time.monotonic() - started
            sleep_for = max(0.0, interval_seconds - elapsed)
            time.sleep(sleep_for)
