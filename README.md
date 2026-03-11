# discord-cli

[![CI](https://github.com/jackwener/discord-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/jackwener/discord-cli/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/kabi_discord_cli.svg)](https://pypi.org/project/kabi-discord-cli/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)

[中文](./README_CN.md)

Telethon-style local-first tooling for Discord: sync messages into SQLite, search them from the terminal, export structured results, and feed them to AI agents.

discord-cli uses the Discord HTTP API with a **user token** from your local session. It is meant for accounts you control, on machines you control.

## Warning

- discord-cli reads a Discord **user token** from your local Discord/browser session.
- Discord may restrict or suspend accounts that automate user-token traffic.
- Use it only on your own account and only if you understand the risk.

## Features

- Local-first SQLite storage for history, sync, search, export, and analytics
- `discord dc sync-all` discovers accessible text channels and bootstraps from the API
- Query commands support `--yaml` and `--json` for scripting and AI agent integration
- Non-TTY stdout defaults to YAML; override with `OUTPUT=yaml|json|rich|auto`
- Structured output contract: [SCHEMA.md](./SCHEMA.md)

> **AI Agent Tip:** Prefer `--yaml` for structured output unless a strict JSON parser is required. Use `-n` to limit results.
- Optional Claude-powered `analyze` and `summary`
- Safer local channel resolution for `search`, `recent`, `today`, `export`, and `purge`

## Installation

```bash
# PyPI
uv tool install kabi-discord-cli
# or
pipx install kabi-discord-cli

# With AI commands
uv tool install 'kabi-discord-cli[ai]'

# From GitHub
uv tool install git+https://github.com/jackwener/discord-cli.git

# From source
git clone git@github.com:jackwener/discord-cli.git
cd discord-cli
uv sync --extra dev
```

Upgrade to the latest version:

```bash
uv tool upgrade kabi-discord-cli
# Or: pipx upgrade kabi-discord-cli
```

> **Tip:** Upgrade regularly to avoid unexpected errors from outdated API handling.

AI commands require the optional `ai` extra plus `ANTHROPIC_API_KEY`.

## Quick Start

```bash
# Extract and save a token from your local Discord/browser session
discord auth --save

# Verify auth
discord status
discord whoami

# Explore guilds and channels
discord dc guilds
discord dc channels <guild_id>

# Bootstrap local storage
discord dc sync-all -n 500

# Query local cache
discord today
discord recent -n 50
discord search "rust" -c general --json
discord timeline --by hour --json
```

## Commands

### Auth & Account

| Command | Description |
|---------|-------------|
| `auth [--save]` | Extract a token from local Discord/browser session |
| `status` | Check if the configured token is valid |
| `whoami [--json]` | Show the current Discord profile |

### Discord API (`discord dc ...`)

| Command | Description |
|---------|-------------|
| `dc guilds [--json]` | List joined guilds |
| `dc channels GUILD [--json]` | List text channels in a guild |
| `dc history CHANNEL [-n 1000]` | Fetch message history for one channel |
| `dc sync CHANNEL [-n 5000]` | Incrementally sync one channel |
| `dc sync-all [-n 5000]` | Discover and sync accessible text channels |
| `dc tail CHANNEL [--once]` | Poll and follow new messages like `tail -f` |
| `dc search GUILD KEYWORD [-c CHANNEL_ID] [--json]` | Use Discord native search |
| `dc members GUILD [--max 50] [--json]` | List guild members |
| `dc info GUILD [--json]` | Show guild info |

### Local Query

| Command | Description |
|---------|-------------|
| `search KEYWORD [-c CHANNEL] [-n 50] [--json]` | Search locally stored messages |
| `recent [-c CHANNEL] [--hours N] [-n 50] [--json]` | Show newest locally stored messages |
| `stats [--json]` | Message statistics per channel |
| `today [-c CHANNEL] [--json]` | Show today's messages |
| `top [-c CHANNEL] [--hours N] [--json]` | Top senders |
| `timeline [-c CHANNEL] [--hours N] [--by day\|hour] [--json]` | Activity timeline |

### Data & AI

| Command | Description |
|---------|-------------|
| `export CHANNEL [-f text\|json] [-o FILE] [--hours N]` | Export stored messages |
| `purge CHANNEL [-y]` | Delete stored messages for a channel |
| `analyze CHANNEL [--hours 24] [-p PROMPT]` | Claude analysis for one channel |
| `summary [-c CHANNEL] [--hours N]` | Claude summary across today or last N hours |

## Behavior Notes

- Most top-level query commands read from local SQLite, not directly from Discord.
- `discord dc sync-all` now bootstraps by discovering guilds and channels through the API, so it works on a fresh database.
- Channel names are resolved against the local database. If a name matches multiple channels, the CLI will stop and ask you to use a more specific name or a channel ID.

## AI Usage

Install AI support first:

```bash
uv sync --extra ai
export ANTHROPIC_API_KEY=...
```

Then:

```bash
discord analyze general --hours 24
discord summary --hours 12
discord search "release" --json
```

discord-cli also ships with [SKILL.md](./SKILL.md) for agent integration.

## Development

```bash
uv sync --extra dev --extra ai
uv run ruff check .
uv run python -m pytest
uv build
```

## More Projects

- [xiaohongshu-cli](https://github.com/jackwener/xiaohongshu-cli) — Xiaohongshu (小红书) CLI for notes and account workflows
- [twitter-cli](https://github.com/jackwener/twitter-cli) — Twitter/X CLI for timelines, bookmarks, and posting
- [bilibili-cli](https://github.com/jackwener/bilibili-cli) — Bilibili CLI for videos, users, search, and feeds
- [tg-cli](https://github.com/jackwener/tg-cli) — Telegram CLI for local-first sync, search, and export

## License

Apache-2.0
