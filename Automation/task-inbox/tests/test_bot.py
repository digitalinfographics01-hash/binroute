import pytest
import requests

import bot


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_send_telegram_message_posts_expected_payload(monkeypatch):
    captured = {}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse({"ok": True})

    monkeypatch.setattr(bot.requests, "post", fake_post)
    result = bot.send_telegram_message("BOT_TOKEN", "12345", "Hello Muhammad")

    assert captured["url"] == "https://api.telegram.org/botBOT_TOKEN/sendMessage"
    assert captured["json"] == {"chat_id": "12345", "text": "Hello Muhammad"}
    assert result == {"ok": True}


def test_send_telegram_message_raises_on_http_error(monkeypatch):
    def fake_post(url, json, timeout):
        return FakeResponse({"ok": False}, status_code=400)

    monkeypatch.setattr(bot.requests, "post", fake_post)
    with pytest.raises(requests.exceptions.HTTPError):
        bot.send_telegram_message("BOT_TOKEN", "12345", "Hello")
