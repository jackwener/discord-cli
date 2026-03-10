# discord-cli

[![CI](https://github.com/jackwener/discord-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/jackwener/discord-cli/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/kabi_discord_cli.svg)](https://pypi.org/project/kabi-discord-cli/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)

[English](./README.md)

一个面向本地缓存和 AI agent 的 Discord CLI：把消息同步到 SQLite，本地搜索、导出和分析，再把结构化结果交给外部 agent。

discord-cli 通过 Discord HTTP API 访问你本机登录态里的 **user token**。它只适合你自己控制的账号和设备。

## 风险提示

- discord-cli 会读取本地 Discord/浏览器会话中的 **user token**
- 使用 user token 访问 Discord API 可能触发平台风控或账号限制
- 只建议在你自己的账号上使用，并且要清楚这类自动化的风险

## 功能特性

- 基于 SQLite 的本地消息缓存，支持 history、sync、search、export 和 analytics
- `discord dc sync-all` 会直接从 API 发现可访问的文字频道，空库也能冷启动
- 查询命令支持 `--json`，方便脚本和 AI agent 调用
- `timeline --json` 提供机器可读的活跃度数据
- 结构化输出协议见 [SCHEMA.md](./SCHEMA.md)

> **AI Agent 提示：** 需要结构化输出时始终使用 `--json`，不要解析默认的富文本显示。用 `-n` 控制返回数量。
- 可选的 Claude `analyze` / `summary`
- 更安全的 channel 解析：遇到重名或模糊匹配会直接报错，而不是误操作

## 安装

```bash
# PyPI
uv tool install kabi-discord-cli
# 或
pipx install kabi-discord-cli

# 安装 AI 相关命令
uv tool install 'kabi-discord-cli[ai]'

# 从 GitHub 安装
uv tool install git+https://github.com/jackwener/discord-cli.git

# 从源码安装
git clone git@github.com:jackwener/discord-cli.git
cd discord-cli
uv sync --extra dev
```

如果要使用 AI 命令，还需要安装 `ai` extra 并配置 `ANTHROPIC_API_KEY`。

## 快速开始

```bash
# 从本地 Discord / 浏览器登录态提取 token 并保存
discord auth --save

# 检查认证
discord status
discord whoami

# 浏览 guild 和 channel
discord dc guilds
discord dc channels <guild_id>

# 冷启动同步本地库
discord dc sync-all -n 500

# 查询本地缓存
discord today
discord recent -n 50
discord search "rust" -c general --json
discord timeline --by hour --json
```

## 命令一览

### 认证与账号

| 命令 | 说明 |
|------|------|
| `auth [--save]` | 从本地 Discord/浏览器会话提取 token |
| `status` | 检查当前 token 是否有效 |
| `whoami [--json]` | 查看当前账号资料 |

### Discord API (`discord dc ...`)

| 命令 | 说明 |
|------|------|
| `dc guilds [--json]` | 列出已加入的 guild |
| `dc channels GUILD [--json]` | 列出 guild 下的文字频道 |
| `dc history CHANNEL [-n 1000]` | 拉取单个频道历史消息 |
| `dc sync CHANNEL [-n 5000]` | 增量同步单个频道 |
| `dc sync-all [-n 5000]` | 自动发现并同步可访问的文字频道 |
| `dc tail CHANNEL [--once]` | 像 `tail -f` 一样轮询新消息 |
| `dc search GUILD KEYWORD [-c CHANNEL_ID] [--json]` | 使用 Discord 原生搜索 |
| `dc members GUILD [--max 50] [--json]` | 列出 guild 成员 |
| `dc info GUILD [--json]` | 查看 guild 详情 |

### 本地查询

| 命令 | 说明 |
|------|------|
| `search KEYWORD [-c CHANNEL] [-n 50] [--json]` | 搜索本地缓存消息 |
| `recent [-c CHANNEL] [--hours N] [-n 50] [--json]` | 查看最新缓存消息 |
| `stats [--json]` | 各频道消息统计 |
| `today [-c CHANNEL] [--json]` | 查看今天的消息 |
| `top [-c CHANNEL] [--hours N] [--json]` | 查看最活跃发言人 |
| `timeline [-c CHANNEL] [--hours N] [--by day\|hour] [--json]` | 查看消息活跃度时间线 |

### 数据与 AI

| 命令 | 说明 |
|------|------|
| `export CHANNEL [-f text\|json] [-o FILE] [--hours N]` | 导出本地消息 |
| `purge CHANNEL [-y]` | 删除某个频道的本地缓存 |
| `analyze CHANNEL [--hours 24] [-p PROMPT]` | 用 Claude 分析单个频道 |
| `summary [-c CHANNEL] [--hours N]` | 汇总今天或最近 N 小时的消息 |

## 行为说明

- 大多数顶层查询命令读的是本地 SQLite，不是每次都直接查 Discord
- `discord dc sync-all` 现在会从 API 发现 guild/channel，所以空数据库也能直接冷启动
- channel 名称解析基于本地数据库；如果一个名字命中多个频道，CLI 会报错并要求你改用更具体的名字或 channel ID

## AI 用法

先安装 AI 依赖：

```bash
uv sync --extra ai
export ANTHROPIC_API_KEY=...
```

然后：

```bash
discord analyze general --hours 24
discord summary --hours 12
discord search "release" --json
```

仓库里还附带了给 agent 使用的 [SKILL.md](./SKILL.md)。

## 开发

```bash
uv sync --extra dev --extra ai
uv run ruff check .
uv run python -m pytest
uv build
```

## 推荐项目

- [xiaohongshu-cli](https://github.com/jackwener/xiaohongshu-cli) — 小红书笔记与账号工作流 CLI
- [twitter-cli](https://github.com/jackwener/twitter-cli) — Twitter/X 时间线、搜索与发推 CLI
- [bilibili-cli](https://github.com/jackwener/bilibili-cli) — Bilibili 视频、用户、搜索与动态 CLI
- [tg-cli](https://github.com/jackwener/tg-cli) — Telegram 本地优先同步、检索与导出 CLI

## License

Apache-2.0
