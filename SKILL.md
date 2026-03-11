---
name: discord-cli
description: Discord CLI with YAML-first structured output for AI agents — fetch chat history, search messages, sync channels, and AI analysis
author: jackwener
version: "1.0.0"
tags:
  - discord
  - chat
  - sync
  - ai-analysis
  - cli
---

# discord-cli Skill

CLI tool for Discord — fetch chat history, search messages, sync channels, AI analysis.

## Agent Defaults

When you need machine-readable output:

1. Prefer `--yaml` for structured output unless a strict JSON parser is required.
2. Use `-n` to keep result sets small and token-efficient.
3. Use `-o <file>` with `export` to save large datasets to a file.
4. Prefer specific queries over broad ones. Example: use `discord search "keyword" -c general --yaml` instead of scanning all channels.
5. Non-TTY stdout defaults to YAML automatically. Use `OUTPUT=yaml|json|rich|auto` to override.
6. All machine-readable output uses the envelope documented in [SCHEMA.md](./SCHEMA.md).

## Prerequisites

- Python 3.10+

```bash
# Install
uv tool install kabi-discord-cli
# Or: pipx install kabi-discord-cli

# Upgrade to latest (recommended to avoid API errors)
uv tool upgrade kabi-discord-cli
# Or: pipx upgrade kabi-discord-cli
```

- Token configured via `discord auth --save`
- AI commands require `ANTHROPIC_API_KEY` env var

## Commands

### Auth & Account

```bash
discord auth --save          # Auto-extract & save token
discord status               # Check token validity (exit 0 = valid)
discord status --yaml        # Structured auth status
discord whoami               # User profile
discord whoami --yaml        # Structured profile
```

### Servers & Channels

```bash
discord dc guilds            # List servers
discord dc guilds --yaml     # YAML output
discord dc channels <GUILD>  # List text channels
discord dc info <GUILD>      # Server details
discord dc members <GUILD>   # List members
```

### Fetching Messages

```bash
discord dc history <CHANNEL_ID> -n 1000   # Fetch history
discord dc sync <CHANNEL_ID>              # Incremental sync
discord dc sync-all                       # Sync all known channels
discord dc tail <CHANNEL_ID> -n 20        # Follow new messages live
discord dc search <GUILD> "keyword"       # Native Discord search
```

### Querying Stored Messages

```bash
discord search "keyword"                  # Search local DB
discord search "keyword" -c general       # Filter by channel
discord stats                             # Per-channel stats
discord today                             # Today's messages
discord today -c general --yaml           # Filter + YAML
discord top                               # Most active senders
discord top --hours 24                    # Last 24h only
discord timeline                          # Activity chart
discord timeline --by hour               # Hourly granularity
```

### Data & AI

```bash
discord export <CHANNEL> -f json -o out.json   # Export
discord purge <CHANNEL> -y                     # Delete stored
discord analyze <CHANNEL> --hours 24           # AI analysis
discord summary                                # AI summary of today
discord summary --hours 48                     # Last 48h summary
```

## Workflow: Daily Sync

```bash
# 1. First time: fetch history for channels you care about
discord dc guilds --yaml
discord dc channels <guild_id> --yaml
discord dc history <channel_id> -n 2000

# 2. Daily: incremental sync
discord dc sync-all

# 3. Read today's messages (structured output for agents)
discord today --yaml

# 4. AI summary
discord summary
```

## Notes

- Uses Discord user token (not bot token) for read-only access
- Rate limits are handled automatically with retry
- Messages stored in SQLite at `~/Library/Application Support/discord-cli/messages.db`
- AI commands require `ANTHROPIC_API_KEY` env var

## Safety Notes

- Do not ask users to share raw token values in chat logs.
- Prefer auto-extraction via `discord auth --save` over manual token input.
- Token is stored locally and never uploaded.
