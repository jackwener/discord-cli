"""Query commands — search, stats, today, top, timeline."""

from collections import defaultdict

import click
from rich.console import Console
from rich.table import Table

from ._channels import resolve_channel_id_or_raise
from ._output import emit_structured, structured_output_options
from ..db import MessageDB

console = Console(stderr=True)


@click.group("query", invoke_without_command=True)
def query_group():
    """Query and analysis commands (registered at top-level)."""
    pass


@query_group.command("search")
@click.argument("keyword")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("-n", "--limit", default=50, help="Max results")
@structured_output_options
def search(keyword: str, channel: str | None, limit: int, as_json: bool, as_yaml: bool):
    """Search stored messages by KEYWORD."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.search(keyword, channel_id=channel_id, limit=limit)

    if results and emit_structured(results, as_json=as_json, as_yaml=as_yaml):
        return

    if not results:
        if emit_structured([], as_json=as_json, as_yaml=as_yaml):
            return
        console.print("[yellow]No messages found.[/yellow]")
        return

    for msg in results:
        ts = (msg.get("timestamp") or "")[:19]
        sender = msg.get("sender_name") or "Unknown"
        ch_name = msg.get("channel_name") or ""
        content = (msg.get("content") or "")[:200]
        console.print(
            f"[dim]{ts}[/dim] [cyan]#{ch_name}[/cyan] | "
            f"[bold]{sender}[/bold]: {content}"
        )

    console.print(f"\n[dim]Found {len(results)} messages[/dim]")


@query_group.command("recent")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("--hours", type=int, help="Only show messages from last N hours")
@click.option("-n", "--limit", default=50, help="Show last N messages")
@structured_output_options
def recent(channel: str | None, hours: int | None, limit: int, as_json: bool, as_yaml: bool):
    """Show the most recent stored messages."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.get_latest(channel_id=channel_id, hours=hours, limit=limit)

    if results and emit_structured(results, as_json=as_json, as_yaml=as_yaml):
        return

    if not results:
        if emit_structured([], as_json=as_json, as_yaml=as_yaml):
            return
        console.print("[yellow]No recent messages found.[/yellow]")
        return

    show_channel = channel_id is None
    for msg in results:
        ts = (msg.get("timestamp") or "")[:19]
        sender = msg.get("sender_name") or "Unknown"
        ch_name = msg.get("channel_name") or ""
        content = (msg.get("content") or "")[:200].replace("\n", " ")
        prefix = f"[cyan]#{ch_name}[/cyan] | " if show_channel and ch_name else ""
        console.print(f"[dim]{ts}[/dim] {prefix}[bold]{sender}[/bold]: {content}")

    console.print(f"\n[dim]Showing {len(results)} recent messages[/dim]")


@query_group.command("stats")
@structured_output_options
def stats(as_json: bool, as_yaml: bool):
    """Show message statistics per channel."""
    with MessageDB() as db:
        channels = db.get_channels()
        total = db.count()

    payload = {"total": total, "channels": channels}
    if emit_structured(payload, as_json=as_json, as_yaml=as_yaml):
        return

    table = Table(title=f"Message Stats (Total: {total})")
    table.add_column("Channel ID", style="dim")
    table.add_column("Channel", style="bold")
    table.add_column("Guild", style="cyan")
    table.add_column("Messages", justify="right")
    table.add_column("First", style="dim")
    table.add_column("Last", style="dim")

    for c in channels:
        ch_id = str(c["channel_id"])
        table.add_row(
            ch_id[-6:] + "…" if len(ch_id) > 6 else ch_id,
            f"#{c['channel_name']}" if c["channel_name"] else "—",
            c.get("guild_name") or "—",
            str(c["msg_count"]),
            (c["first_msg"] or "")[:10],
            (c["last_msg"] or "")[:10],
        )

    console.print(table)


@query_group.command("today")
@click.option("-c", "--channel", help="Filter by channel name")
@structured_output_options
def today(channel: str | None, as_json: bool, as_yaml: bool):
    """Show today's messages, grouped by channel."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        msgs = db.get_today(channel_id=channel_id)

    if msgs and emit_structured(msgs, as_json=as_json, as_yaml=as_yaml):
        return

    if not msgs:
        if emit_structured([], as_json=as_json, as_yaml=as_yaml):
            return
        console.print("[yellow]No messages today.[/yellow]")
        return

    grouped: dict[str, list[dict]] = defaultdict(list)
    for m in msgs:
        key = f"#{m.get('channel_name') or 'unknown'}"
        if m.get("guild_name"):
            key = f"{m['guild_name']} > {key}"
        grouped[key].append(m)

    for ch_label, ch_msgs in sorted(grouped.items(), key=lambda x: -len(x[1])):
        console.print(f"\n[bold cyan]═══ {ch_label} ({len(ch_msgs)} msgs) ═══[/bold cyan]")
        for m in ch_msgs:
            ts = (m.get("timestamp") or "")[11:19]
            sender = m.get("sender_name") or "Unknown"
            content = (m.get("content") or "")[:200].replace("\n", " ")
            console.print(f"  [dim]{ts}[/dim] [bold]{sender[:15]}[/bold]: {content}")

    console.print(f"\n[green]Total: {len(msgs)} messages today[/green]")


@query_group.command("top")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("--hours", type=int, help="Only count messages within N hours")
@click.option("-n", "--limit", default=20, help="Top N senders")
@structured_output_options
def top(channel: str | None, hours: int | None, limit: int, as_json: bool, as_yaml: bool):
    """Show most active senders."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.top_senders(channel_id=channel_id, hours=hours, limit=limit)

    if results and emit_structured(results, as_json=as_json, as_yaml=as_yaml):
        return

    if not results:
        if emit_structured([], as_json=as_json, as_yaml=as_yaml):
            return
        console.print("[yellow]No sender data found.[/yellow]")
        return

    table = Table(title="Top Senders")
    table.add_column("#", style="dim", justify="right")
    table.add_column("Sender", style="bold")
    table.add_column("Messages", justify="right")
    table.add_column("First", style="dim")
    table.add_column("Last", style="dim")

    for i, r in enumerate(results, 1):
        table.add_row(
            str(i),
            r["sender_name"],
            str(r["msg_count"]),
            (r["first_msg"] or "")[:10],
            (r["last_msg"] or "")[:10],
        )

    console.print(table)


@query_group.command("timeline")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("--hours", type=int, help="Only show last N hours")
@click.option("--by", "granularity", type=click.Choice(["day", "hour"]), default="day")
@structured_output_options
def timeline(channel: str | None, hours: int | None, granularity: str, as_json: bool, as_yaml: bool):
    """Show message activity over time as a bar chart."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.timeline(channel_id=channel_id, hours=hours, granularity=granularity)

    if results and emit_structured(results, as_json=as_json, as_yaml=as_yaml):
        return

    if not results:
        if emit_structured([], as_json=as_json, as_yaml=as_yaml):
            return
        console.print("[yellow]No timeline data.[/yellow]")
        return

    max_count = max(r["msg_count"] for r in results)
    bar_width = 40

    for r in results:
        period = r["period"]
        count = r["msg_count"]
        bar_len = int(count / max_count * bar_width) if max_count > 0 else 0
        bar = "█" * bar_len
        console.print(f"[dim]{period}[/dim] {bar} [bold]{count}[/bold]")
