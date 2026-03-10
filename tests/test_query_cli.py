from __future__ import annotations

import json

from click.testing import CliRunner
import yaml

from discord_cli.cli.main import cli
from discord_cli.db import MessageDB


def test_recent_command_shows_latest_messages(seeded_db: MessageDB):
    runner = CliRunner()

    result = runner.invoke(cli, ["recent", "-n", "2"])

    assert result.exit_code == 0
    assert "second message" in result.output
    assert "third message" in result.output
    assert "first message" not in result.output


def test_recent_command_supports_json(seeded_db: MessageDB):
    runner = CliRunner()

    result = runner.invoke(cli, ["recent", "-c", "general", "-n", "2", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    rows = payload["data"]
    assert [row["msg_id"] for row in rows] == ["100", "101"]
    assert all(row["channel_name"] == "general" for row in rows)


def test_recent_command_auto_yaml_when_stdout_is_not_tty(seeded_db: MessageDB, monkeypatch):
    monkeypatch.setenv("OUTPUT", "auto")
    runner = CliRunner()

    result = runner.invoke(cli, ["recent", "-c", "general", "-n", "2"])

    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["ok"] is True
    rows = payload["data"]
    assert [row["msg_id"] for row in rows] == ["100", "101"]


def test_timeline_command_supports_json(seeded_db: MessageDB):
    runner = CliRunner()

    result = runner.invoke(cli, ["timeline", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    rows = payload["data"]
    assert rows
    assert rows[0]["period"] == "2026-03-10"


def test_recent_command_rejects_ambiguous_channel(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "messages.db"))

    with MessageDB() as db:
        db.insert_batch(
            [
                {
                    "msg_id": "1",
                    "channel_id": "c-general",
                    "channel_name": "general",
                    "guild_id": "g-1",
                    "guild_name": "Dev",
                    "sender_id": "u-1",
                    "sender_name": "Alice",
                    "content": "hello",
                    "timestamp": "2026-03-10T01:00:00+00:00",
                },
                {
                    "msg_id": "2",
                    "channel_id": "c-general-chat",
                    "channel_name": "general-chat",
                    "guild_id": "g-1",
                    "guild_name": "Dev",
                    "sender_id": "u-2",
                    "sender_name": "Bob",
                    "content": "world",
                    "timestamp": "2026-03-10T02:00:00+00:00",
                },
            ]
        )

    runner = CliRunner()
    result = runner.invoke(cli, ["recent", "-c", "gen"])

    assert result.exit_code != 0
    assert "ambiguous" in result.output


def test_status_auto_yaml_when_stdout_is_not_tty(monkeypatch):
    monkeypatch.setenv("OUTPUT", "auto")
    monkeypatch.setenv("DISCORD_TOKEN", "token")

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"id": "u-1", "username": "alice", "global_name": "Alice"}

    monkeypatch.setattr("httpx.get", lambda *args, **kwargs: FakeResponse())
    runner = CliRunner()

    result = runner.invoke(cli, ["status"])

    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["ok"] is True
    assert payload["schema_version"] == "1"
    assert payload["data"]["authenticated"] is True
    assert payload["data"]["user"]["username"] == "alice"


def test_whoami_auto_yaml_when_stdout_is_not_tty(monkeypatch):
    monkeypatch.setenv("OUTPUT", "auto")

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_get_me(client):
        return {
            "id": "u-1",
            "username": "alice",
            "global_name": "Alice",
            "created_at": "2026-03-10T00:00:00+00:00",
        }

    monkeypatch.setattr("discord_cli.client.get_client", lambda: FakeClient())
    monkeypatch.setattr("discord_cli.client.get_me", fake_get_me)
    runner = CliRunner()

    result = runner.invoke(cli, ["whoami"])

    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["ok"] is True
    assert payload["data"]["user"]["username"] == "alice"
