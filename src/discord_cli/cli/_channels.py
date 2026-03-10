"""Helpers for resolving stored channel names safely."""

import click

from ..db import ChannelResolutionError, MessageDB
from ._output import emit_error


def resolve_channel_id_or_raise(db: MessageDB, channel: str) -> str:
    """Resolve a stored channel ID or raise a CLI-friendly error."""
    try:
        return db.resolve_channel(channel)["channel_id"]
    except ChannelResolutionError as exc:
        if emit_error("channel_resolution_error", str(exc)):
            raise SystemExit(1) from None
        raise click.ClickException(str(exc)) from exc
