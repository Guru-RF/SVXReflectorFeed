#!/usr/bin/env python3
"""
remove_offline_nodes.py

Maintenance helper for SVXReflectorFeed.

Removes offline nodes from the Redis roster hash (svx:nodes) that
svx_talker_ws.py persists. Handy to clear nodes that accumulated before the
automatic offline prune existed, or to force a cleanup on demand.

Safe by default: this is a DRY RUN unless you pass --apply. Without --apply it
only reports what it *would* delete.

Note: svx_talker_ws.py keeps the roster in memory and re-serves it in every
WebSocket snapshot, so restart the service after applying — otherwise connected
clients keep seeing the removed nodes until they reconnect:

    sudo systemctl restart svx-talker-ws

Env vars (same defaults as svx_talker_ws.py):
- REDIS_URL       (default redis://127.0.0.1:6379/0)
- SVX_NODES_KEY   (default svx:nodes)

Examples:
    python3 remove_offline_nodes.py                        # dry run: show offline nodes
    python3 remove_offline_nodes.py --apply                # remove ALL offline nodes
    python3 remove_offline_nodes.py --older-than 24h --apply
    python3 remove_offline_nodes.py --older-than 24h --include-undated --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import redis


def parse_duration(s: str) -> float:
    """Parse '3600', '90s', '30m', '24h', '7d' into seconds."""
    s = s.strip().lower()
    if not s:
        raise ValueError("empty duration")
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if s[-1] in units:
        return float(s[:-1]) * units[s[-1]]
    return float(s)


def now_ms() -> int:
    return int(time.time() * 1000)


def classify(
    raw: Dict[str, str],
    older_than_s: Optional[float],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Split roster fields into (to_remove, undated_offline, kept).

    - to_remove:       offline nodes eligible for deletion (or unparseable junk)
    - undated_offline: offline but no offline_since_ms, so age can't be checked
    - kept:            online nodes (never removed) + offline-but-too-recent

    With no age filter, every offline node is eligible. With --older-than, only
    offline nodes whose offline_since_ms is old enough are eligible; those
    lacking a timestamp land in undated_offline.
    """
    to_remove: List[str] = []
    undated: List[str] = []
    kept: List[str] = []
    cutoff_ms = now_ms() - int(older_than_s * 1000) if older_than_s is not None else None

    for cs, v in raw.items():
        try:
            st = json.loads(v)
        except Exception:
            # Unparseable value — stale junk, safe to remove.
            to_remove.append(cs)
            continue
        if not isinstance(st, dict) or st.get("online") is True:
            kept.append(cs)
            continue
        # node is offline
        if cutoff_ms is None:
            to_remove.append(cs)
            continue
        since = st.get("offline_since_ms")
        if since is None:
            undated.append(cs)
        elif since <= cutoff_ms:
            to_remove.append(cs)
        else:
            kept.append(cs)
    return to_remove, undated, kept


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Remove offline nodes from the SVXReflectorFeed Redis roster."
    )
    ap.add_argument(
        "--apply", action="store_true",
        help="actually delete (default: dry run, report only)",
    )
    ap.add_argument(
        "--older-than", metavar="DUR",
        help="only remove nodes offline at least this long (e.g. 3600, 30m, 24h, 7d)",
    )
    ap.add_argument(
        "--include-undated", action="store_true",
        help="with --older-than, also remove offline nodes that have no offline_since_ms",
    )
    ap.add_argument(
        "--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
        help="Redis connection URL (default: $REDIS_URL or redis://127.0.0.1:6379/0)",
    )
    ap.add_argument(
        "--nodes-key", default=os.getenv("SVX_NODES_KEY", "svx:nodes"),
        help="roster hash key (default: $SVX_NODES_KEY or svx:nodes)",
    )
    args = ap.parse_args(argv)

    older_than_s: Optional[float] = None
    if args.older_than:
        try:
            older_than_s = parse_duration(args.older_than)
        except ValueError:
            print(f"error: invalid --older-than value: {args.older_than!r}", file=sys.stderr)
            return 2

    try:
        r = redis.from_url(args.redis_url, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"error: cannot connect to Redis at {args.redis_url}: {e}", file=sys.stderr)
        return 1

    raw = r.hgetall(args.nodes_key)
    if not raw:
        print(f"Roster '{args.nodes_key}' is empty — nothing to do.")
        return 0

    to_remove, undated, kept = classify(raw, older_than_s)

    if args.include_undated and undated:
        to_remove.extend(undated)
        undated = []

    scope = f"offline ≥ {args.older_than}" if older_than_s is not None else "offline"
    print(
        f"Roster '{args.nodes_key}': {len(raw)} nodes "
        f"({len(kept)} online/kept, {len(to_remove)} {scope}, "
        f"{len(undated)} undated-offline)."
    )

    if undated:
        print(
            "  undated offline (no offline_since_ms, skipped — pass --include-undated "
            f"to remove): {', '.join(sorted(undated))}"
        )

    if not to_remove:
        print("Nothing to remove.")
        return 0

    print(f"  to remove: {', '.join(sorted(to_remove))}")

    if not args.apply:
        print("\nDry run — no changes made. Re-run with --apply to delete.")
        return 0

    removed = r.hdel(args.nodes_key, *to_remove)
    print(f"\nRemoved {removed} node(s) from '{args.nodes_key}'.")
    print("Restart the feed so clients stop seeing them: sudo systemctl restart svx-talker-ws")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
