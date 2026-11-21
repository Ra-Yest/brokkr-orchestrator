"""Fail-safe notification backends (Telegram Bot API + NullNotifier)."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from abc import ABC, abstractmethod

from orchestrator.config import NotificationsConfig, TelegramNotificationConfig


class BaseNotifier(ABC):
    """Abstract notifier for critical alerts and run summaries."""

    @abstractmethod
    def send(self, title: str, message: str) -> bool:
        """Send a notification. Must never raise exceptions into the orchestrator core."""


class NullNotifier(BaseNotifier):
    def send(self, title: str, message: str) -> bool:
        return False


class TelegramNotifier(BaseNotifier):
    """Sends concise status/failure alerts via the Telegram Bot API."""

    def __init__(self, config: TelegramNotificationConfig) -> None:
        self.config = config

    def send(self, title: str, message: str) -> bool:
        if not self.config.enabled:
            return False
        token = os.environ.get(self.config.bot_token_env, "").strip()
        chat_id = os.environ.get(self.config.chat_id_env, "").strip()
        if not token or not chat_id:
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
                return 200 <= int(getattr(resp, "status", 200)) < 300
        except (urllib.error.URLError, TimeoutError, OSError):
            return False


def build_notifier(config: NotificationsConfig) -> BaseNotifier:
    if config.telegram.enabled:
        return TelegramNotifier(config.telegram)
    return NullNotifier()
