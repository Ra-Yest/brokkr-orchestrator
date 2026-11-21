"""Fail-safe notification backends (Telegram Bot API + NullNotifier)."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from abc import ABC, abstractmethod

from orchestrator.config import NotificationsConfig, TelegramNotificationConfig


class BaseNotifier(ABC):
    """Abstract notifier for critical alerts and run summaries."""

    @abstractmethod
    def send(self, title: str, message: str) -> bool:
        """Send a notification. Must never raise exceptions into the orchestrator core."""

    def should_notify(self, event: str) -> bool:
        """Return True if this notifier should deliver notifications for `event`."""
        return True

    def notify(self, event: str, title: str, message: str) -> bool:
        """Send an event-filtered notification; never raises into the orchestrator."""
        if not self.should_notify(event):
            return False
        try:
            return self.send(title, message)
        except Exception as exc:
            print(
                f"[WARN] Notification '{event}' failed ({type(exc).__name__}): {exc}",
                file=sys.stderr,
                flush=True,
            )
            return False


class NullNotifier(BaseNotifier):
    def send(self, title: str, message: str) -> bool:
        return False

    def should_notify(self, event: str) -> bool:
        return False


class TelegramNotifier(BaseNotifier):
    """Sends concise status/failure alerts via the Telegram Bot API."""

    def __init__(self, config: TelegramNotificationConfig) -> None:
        self.config = config

    def should_notify(self, event: str) -> bool:
        return self.config.enabled and event in self.config.events

    def send(self, title: str, message: str) -> bool:
        if not self.config.enabled:
            return False
        token = os.environ.get(self.config.bot_token_env, "").strip()
        chat_id = os.environ.get(self.config.chat_id_env, "").strip()
        if not token or not chat_id:
            missing = [
                env_name
                for env_name, val in (
                    (self.config.bot_token_env, token),
                    (self.config.chat_id_env, chat_id),
                )
                if not val
            ]
            print(
                f"[WARN] Telegram notification skipped: missing env var(s) {', '.join(missing)}",
                file=sys.stderr,
                flush=True,
            )
            return False

        text = f"[{title}]\n{message}"[:4000]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                status = int(getattr(resp, "status", 200))
                if 200 <= status < 300:
                    return True
                print(
                    f"[WARN] Telegram notification returned HTTP {status} for '{title}'",
                    file=sys.stderr,
                    flush=True,
                )
                return False
        except urllib.error.HTTPError as exc:
            print(
                f"[WARN] Telegram notification failed with HTTP {exc.code} for '{title}'",
                file=sys.stderr,
                flush=True,
            )
            return False
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            print(
                f"[WARN] Telegram notification failed ({type(exc).__name__}: {reason}) for '{title}'",
                file=sys.stderr,
                flush=True,
            )
            return False


def build_notifier(config: NotificationsConfig) -> BaseNotifier:
    if config.telegram.enabled:
        return TelegramNotifier(config.telegram)
    return NullNotifier()
