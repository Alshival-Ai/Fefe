from __future__ import annotations

import time

from allauth.socialaccount.models import SocialAccount
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db.utils import OperationalError, ProgrammingError

from dashboard.calendar_sync_service import refresh_calendar_cache_for_user


class Command(BaseCommand):
    help = "Periodically refresh Asana/Outlook calendar caches for connected users."

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval-seconds",
            type=int,
            default=60,
            help="Polling interval in seconds (default: 60).",
        )
        parser.add_argument(
            "--provider",
            default="all",
            choices=["asana", "outlook", "all"],
            help="Calendar provider to refresh each cycle.",
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
            "--force",
            action="store_true",
            help="Force refresh each cycle, ignoring throttle state.",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run a single sync cycle and exit.",
        )

    def _provider_user_ids(self, provider: str) -> set[int]:
        resolved_provider = str(provider or "all").strip().lower() or "all"
        user_ids: set[int] = set()
        try:
            if resolved_provider in {"asana", "all"}:
                user_ids.update(
                    int(value)
                    for value in SocialAccount.objects.filter(provider="asana").values_list("user_id", flat=True)
                )
            if resolved_provider in {"outlook", "all"}:
                user_ids.update(
                    int(value)
                    for value in SocialAccount.objects.filter(provider="microsoft").values_list("user_id", flat=True)
                )
        except (OperationalError, ProgrammingError):
            return set()
        except Exception:
            return set()
        return user_ids

    def _target_users(self, *, provider: str, user_filters: list[str], max_users: int):
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
            connected_ids = self._provider_user_ids(provider)
            if connected_ids:
                queryset = queryset.filter(id__in=sorted(connected_ids))
            else:
                queryset = queryset.none()
        queryset = queryset.order_by("id")
        if max_users > 0:
            queryset = queryset[:max_users]
        return queryset

    def _provider_result_rows(self, *, provider: str, result: dict[str, object]) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        if provider in {"all", "asana"}:
            asana_result = result.get("asana")
            if isinstance(asana_result, dict):
                rows.append(asana_result)
        if provider in {"all", "outlook"}:
            outlook_result = result.get("outlook")
            if isinstance(outlook_result, dict):
                rows.append(outlook_result)
        return rows

    def _run_cycle(
        self,
        *,
        provider: str,
        user_filters: list[str],
        max_users: int,
        interval_seconds: int,
        force: bool,
    ) -> dict[str, int]:
        totals = {
            "users": 0,
            "ok": 0,
            "failed": 0,
            "attempted": 0,
            "skipped": 0,
        }

        queryset = self._target_users(
            provider=provider,
            user_filters=user_filters,
            max_users=max_users,
        )

        for user in queryset.iterator():
            totals["users"] += 1
            try:
                result = refresh_calendar_cache_for_user(
                    user,
                    provider=provider,
                    force=force,
                    min_interval_seconds=max(1, int(interval_seconds)),
                )
            except Exception as exc:
                totals["failed"] += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"[calendar-sync-worker] user={user.get_username()} status=error error={exc}"
                    )
                )
                continue

            provider_rows = self._provider_result_rows(
                provider=provider,
                result=result if isinstance(result, dict) else {},
            )
            user_error_messages = []
            for row in provider_rows:
                if bool(row.get("refresh_attempted")):
                    totals["attempted"] += 1
                if bool(row.get("refresh_skipped")):
                    totals["skipped"] += 1
                error_message = str(row.get("error") or "").strip()
                if error_message:
                    user_error_messages.append(error_message)

            if user_error_messages:
                totals["failed"] += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"[calendar-sync-worker] user={user.get_username()} status=error "
                        f"error={'; '.join(user_error_messages)}"
                    )
                )
            else:
                totals["ok"] += 1

        return totals

    def handle(self, *args, **options):
        interval_seconds = max(15, int(options.get("interval_seconds") or 60))
        provider = str(options.get("provider") or "all").strip().lower() or "all"
        user_filters = list(options.get("user") or [])
        max_users = max(0, int(options.get("max_users") or 0))
        force = bool(options.get("force"))
        run_once = bool(options.get("once"))

        self.stdout.write(
            "[calendar-sync-worker] started "
            f"provider={provider} interval={interval_seconds}s once={run_once} "
            f"force={force} max_users={max_users or 'all'}"
        )

        while True:
            started = time.monotonic()
            try:
                totals = self._run_cycle(
                    provider=provider,
                    user_filters=user_filters,
                    max_users=max_users,
                    interval_seconds=interval_seconds,
                    force=force,
                )
                self.stdout.write(
                    self.style.SUCCESS(
                        "[calendar-sync-worker] cycle users={users} ok={ok} failed={failed} "
                        "attempted={attempted} skipped={skipped}".format(**totals)
                    )
                )
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f"[calendar-sync-worker] cycle failed: {exc}"))
                if run_once:
                    raise

            if run_once:
                return

            elapsed = time.monotonic() - started
            sleep_for = max(0.0, interval_seconds - elapsed)
            time.sleep(sleep_for)
