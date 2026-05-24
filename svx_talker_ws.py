#!/usr/bin/env python3
"""
svx_talker_ws.py

Polls SvxReflector status endpoint (local), detects when a node starts/stops talking,
stores each talk "session" in Redis with a TTL (default 24h), and serves BOTH:

1) Current node state (online/isTalker/tg/monitoredTGs/location/coords)
2) Talk sessions (last 24h) and live talk start/stop events

via WebSockets.

WebSocket messages (JSON):

- On connect:
  {
    "type": "snapshot",
    "generated": "...",
    "nodes": [ {node_state}, ... ],
    "active": [ {session}, ... ],
    "sessions": [ {session}, ... ]
  }

- On node state change (online/offline/isTalker/tg/coords/location etc):
  {"type":"node_upsert","node":{node_state}}

- On talk start:
  {"type":"talk_start","session":{session}}

- On talk stop:
  {"type":"talk_stop","session":{session}}

Node state format:
{
  "callsign": "ON0XXX",
  "online": true/false,
  "isTalker": true/false,
  "tg": 8,
  "monitoredTGs": [8, 1745],
  "location": "Gent",
  "lat": 51.0450,
  "lon": 3.7017
}

Redis keys:
- svx:session:<start_ms>:<callsign>  -> JSON session record, EX TTL
- svx:sessions                       -> ZSET index (score=start_ms, member=session_key)
- svx:active                         -> SET of callsigns currently talking

Env vars:
- SVX_STATUS_URL      (default http://127.0.0.1:8080/status)
- SVX_POLL_INTERVAL   (default 0.5 seconds; try 0.1 or 0.05 if you miss short PTTs)
- REDIS_URL           (default redis://127.0.0.1:6379/0)
- SVX_TTL_SECONDS     (default 86400)
- SVX_SNAPSHOT_LIMIT  (default 500)
- SVX_WS_HOST         (default 127.0.0.1)
- SVX_WS_PORT         (default 8765)

Feed outage handling:
- SVX_STATUS_STALE_AFTER        seconds without successful poll before marking nodes offline (default 3.0)
- SVX_POLL_FAILURE_MAX_SLEEP    maximum backoff delay between retries when status is down (default 5.0)
- SVX_POLL_FAILURE_LOG_EVERY    rate-limit for connection failure warnings (default 30.0)

Log-noise suppression:
- WS_NOISE_REPORT_INTERVAL (default 60 seconds; set 0 to disable)
  Suppresses noisy handshake errors from non-WebSocket HTTP requests hitting the WS port:
  - missing Upgrade header (plain HTTP GET)
  - invalid HTTP request / POST, etc.
  and logs a periodic summary instead.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import redis.asyncio as redis
import websockets
from websockets.exceptions import ConnectionClosed

try:
    # Used only to identify and suppress noisy handshake spam.
    from websockets.exceptions import InvalidUpgrade, InvalidMessage  # type: ignore
except Exception:  # pragma: no cover
    InvalidUpgrade = None  # type: ignore
    InvalidMessage = None  # type: ignore


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return float(v)


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep for `seconds` unless stop is set."""
    if seconds <= 0:
        await asyncio.sleep(0)
        return
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


# -------------------- Log noise suppression --------------------

class _WsNoiseCounter:
    def __init__(self) -> None:
        self._counts: Dict[str, int] = {}

    def inc(self, reason: str, n: int = 1) -> None:
        self._counts[reason] = self._counts.get(reason, 0) + n

    def take(self) -> Dict[str, int]:
        d = dict(self._counts)
        self._counts.clear()
        return d


class _SuppressHandshakeNoise(logging.Filter):
    """
    Suppress noisy websockets logs when random HTTP clients hit the WS endpoint without
    WebSocket headers or with non-GET methods. Count them and report periodically instead.

    Examples we suppress:
    - InvalidUpgrade: missing Upgrade header
    - InvalidMessage: did not receive a valid HTTP request (often from POST / scanners)
    - ValueError: unsupported HTTP method; expected GET; got POST  (some versions surface this directly)
    - INFO "connection rejected (426 Upgrade Required)"
    """

    def __init__(self, counter: _WsNoiseCounter) -> None:
        super().__init__()
        self.counter = counter

    def filter(self, record: logging.LogRecord) -> bool:
        # Only consider websockets internal loggers
        if not record.name.startswith("websockets"):
            return True

        msg = record.getMessage()

        # Suppress the "connection rejected (426 Upgrade Required)" line (avoid double-counting)
        if "connection rejected" in msg and "Upgrade Required" in msg:
            return False

        # Suppress and count common handshake failures
        if msg == "opening handshake failed" and record.exc_info:
            exc = record.exc_info[1]

            # Case 1: plain HTTP GET without Upgrade header
            if InvalidUpgrade is not None and isinstance(exc, InvalidUpgrade):
                s = str(exc)
                if "missing Upgrade header" in s:
                    self.counter.inc("missing_upgrade")
                    return False

            # Case 2: invalid HTTP request (POST, garbage, etc.)
            if InvalidMessage is not None and isinstance(exc, InvalidMessage):
                self.counter.inc("invalid_http")
                return False

            # Some versions may propagate ValueError directly
            if isinstance(exc, ValueError) and "unsupported HTTP method" in str(exc):
                self.counter.inc("invalid_http")
                return False

        return True


async def ws_noise_reporter(counter: _WsNoiseCounter, stop: asyncio.Event, every_s: float) -> None:
    log = logging.getLogger("svx_talker_ws")
    while not stop.is_set():
        await sleep_or_stop(stop, every_s)
        d = counter.take()
        if not d:
            continue
        total = sum(d.values())
        parts = ", ".join(f"{k}={v}" for k, v in sorted(d.items()))
        log.info("Ignored %d non-WebSocket requests to WS endpoint (%s).", total, parts)


def install_ws_noise_suppression(stop: asyncio.Event) -> Optional[asyncio.Task]:
    """
    Install a filter to suppress noisy websockets handshake errors and start
    a periodic reporter task.
    """
    interval = env_float("WS_NOISE_REPORT_INTERVAL", 60.0)
    if interval <= 0:
        return None

    counter = _WsNoiseCounter()
    flt = _SuppressHandshakeNoise(counter)

    # Attach filter to root handlers (most reliable: catches all records going to journald)
    root = logging.getLogger()
    if root.handlers:
        for h in root.handlers:
            h.addFilter(flt)
    else:
        root.addFilter(flt)

    return asyncio.create_task(ws_noise_reporter(counter, stop, interval), name="ws_noise_reporter")


# -------------------- Node / session helpers --------------------

def to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return f if (f == f) else None  # NaN check
    if isinstance(v, str):
        s = v.strip().replace(",", ".").replace("O", "0").replace("o", "0")
        try:
            f = float(s)
            return f if (f == f) else None
        except Exception:
            return None
    return None


def summarize_node(node: Dict[str, Any]) -> Dict[str, Any]:
    """Reduced subset for sessions (stored in Redis)."""
    qth_summary: Optional[Dict[str, Any]] = None
    qth_list = node.get("qth") or []
    if isinstance(qth_list, list) and qth_list:
        q0 = qth_list[0] if isinstance(qth_list[0], dict) else {}
        pos = q0.get("pos") or {}
        if not isinstance(pos, dict):
            pos = {}
        qth_summary = {
            "name": q0.get("name"),
            "loc": pos.get("loc"),
            "lat": pos.get("lat"),
            "long": pos.get("long"),
        }

    return {
        "nodeLocation": node.get("nodeLocation"),
        "sysop": node.get("sysop"),
        "tg": node.get("tg"),
        "monitoredTGs": node.get("monitoredTGs"),
        "restrictedTG": node.get("restrictedTG"),
        "sw": node.get("sw"),
        "swVer": node.get("swVer"),
        "projVer": node.get("projVer"),
        "machineArch": node.get("machineArch"),
        "protoVer": node.get("protoVer"),
        "qth": qth_summary,
    }


def make_node_state(callsign: str, node: Dict[str, Any], online: bool) -> Dict[str, Any]:
    """Stable node state for the UI (avoid fields that change every poll)."""
    lat = lon = None
    qth = node.get("qth") or []
    if isinstance(qth, list) and qth and isinstance(qth[0], dict):
        pos = qth[0].get("pos") or {}
        if isinstance(pos, dict):
            lat = to_float(pos.get("lat"))
            lon = to_float(pos.get("long"))

    monitored = node.get("monitoredTGs") or []
    if not isinstance(monitored, list):
        monitored = []
    cleaned: List[int] = []
    for x in monitored:
        try:
            cleaned.append(int(x))
        except Exception:
            continue
    cleaned = sorted(set(cleaned))

    tg_raw = node.get("tg")
    try:
        tg = int(tg_raw)
    except Exception:
        tg = 0

    return {
        "callsign": callsign,
        "online": online,
        "isTalker": bool(node.get("isTalker")) if online else False,
        "tg": tg if online else 0,
        "monitoredTGs": cleaned,
        "location": (node.get("nodeLocation") or "").strip(),
        "lat": lat,
        "lon": lon,
    }


@dataclass(frozen=True)
class Config:
    status_url: str = os.getenv("SVX_STATUS_URL", "http://127.0.0.1:8080/status")
    poll_interval_s: float = env_float("SVX_POLL_INTERVAL", 0.5)

    # When status can't be fetched for this many seconds, mark nodes offline + close talk sessions.
    status_stale_after_s: float = env_float("SVX_STATUS_STALE_AFTER", 3.0)

    # When status is down, use exponential backoff but cap the delay.
    poll_failure_max_sleep_s: float = env_float("SVX_POLL_FAILURE_MAX_SLEEP", 5.0)

    # Rate-limit warnings about status fetch failures.
    poll_failure_log_every_s: float = env_float("SVX_POLL_FAILURE_LOG_EVERY", 30.0)

    redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    ttl_seconds: int = env_int("SVX_TTL_SECONDS", 24 * 60 * 60)  # 24h
    snapshot_limit: int = env_int("SVX_SNAPSHOT_LIMIT", 500)

    ws_host: str = os.getenv("SVX_WS_HOST", "127.0.0.1")
    ws_port: int = env_int("SVX_WS_PORT", 8765)

    # Redis namespaces
    session_prefix: str = os.getenv("SVX_SESSION_PREFIX", "svx:session")
    sessions_zset: str = os.getenv("SVX_SESSIONS_ZSET", "svx:sessions")
    active_set: str = os.getenv("SVX_ACTIVE_SET", "svx:active")
    nodes_key: str = os.getenv("SVX_NODES_KEY", "svx:nodes")


class NodeCache:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._nodes: Dict[str, Dict[str, Any]] = {}

    async def replace(self, nodes: Dict[str, Dict[str, Any]]) -> None:
        async with self._lock:
            self._nodes = dict(nodes)

    async def list_nodes(self) -> List[Dict[str, Any]]:
        async with self._lock:
            return list(self._nodes.values())

    async def has(self, callsign: str) -> bool:
        async with self._lock:
            return callsign in self._nodes


class Hub:
    """Tracks connected WS clients and broadcasts JSON messages."""

    def __init__(self) -> None:
        self._clients: Set[Any] = set()
        self._lock = asyncio.Lock()

    async def register(self, ws: Any) -> None:
        async with self._lock:
            self._clients.add(ws)

    async def unregister(self, ws: Any) -> None:
        async with self._lock:
            self._clients.discard(ws)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        data = json.dumps(message, separators=(",", ":"), ensure_ascii=False)

        async with self._lock:
            clients = list(self._clients)

        if not clients:
            return

        await asyncio.gather(*(self._safe_send(ws, data) for ws in clients), return_exceptions=True)

    async def _safe_send(self, ws: Any, data: str) -> None:
        try:
            await ws.send(data)
        except ConnectionClosed:
            await self.unregister(ws)
        except Exception:
            await self.unregister(ws)


async def get_recent_sessions(r: redis.Redis, cfg: Config) -> List[Dict[str, Any]]:
    """Return session records from the last cfg.ttl_seconds, newest first."""
    now = now_ms()
    cutoff = now - (cfg.ttl_seconds * 1000)

    keys: List[str] = await r.zrevrangebyscore(
        cfg.sessions_zset,
        max="+inf",
        min=cutoff,
        start=0,
        num=cfg.snapshot_limit,
    )
    if not keys:
        return []

    values = await r.mget(keys)
    sessions: List[Dict[str, Any]] = []
    missing_keys: List[str] = []

    for k, v in zip(keys, values):
        if v is None:
            missing_keys.append(k)
            continue
        try:
            sessions.append(json.loads(v))
        except Exception:
            continue

    if missing_keys:
        await r.zrem(cfg.sessions_zset, *missing_keys)

    return sessions


async def build_snapshot(r: redis.Redis, cfg: Config, node_cache: NodeCache) -> Dict[str, Any]:
    sessions = await get_recent_sessions(r, cfg)
    active = [s for s in sessions if s.get("active")]
    nodes = await node_cache.list_nodes()
    return {
        "type": "snapshot",
        "generated": iso_from_ms(now_ms()),
        "nodes": nodes,
        "active": active,
        "sessions": sessions,
    }


async def ws_handler(
    ws: Any,
    path: str,
    hub: Hub,
    r: redis.Redis,
    cfg: Config,
    node_cache: NodeCache,
) -> None:
    await hub.register(ws)
    try:
        snap = await build_snapshot(r, cfg, node_cache)
        await ws.send(json.dumps(snap, ensure_ascii=False))

        try:
            async for msg in ws:
                cmd = (msg or "").strip().lower()
                if cmd in ("snapshot", "sync", "refresh"):
                    snap = await build_snapshot(r, cfg, node_cache)
                    await ws.send(json.dumps(snap, ensure_ascii=False))
                elif cmd == "ping":
                    await ws.send(json.dumps({"type": "pong", "ts": iso_from_ms(now_ms())}))
        except ConnectionClosed:
            pass

    except ConnectionClosed:
        pass
    finally:
        await hub.unregister(ws)


async def prune_task(r: redis.Redis, cfg: Config, stop: asyncio.Event) -> None:
    """Periodically prune the ZSET index, so it doesn't grow forever."""
    while not stop.is_set():
        try:
            cutoff = now_ms() - (cfg.ttl_seconds * 1000)
            await r.zremrangebyscore(cfg.sessions_zset, 0, cutoff)
        except Exception:
            logging.exception("prune_task error")
        await sleep_or_stop(stop, 60)


async def ai_rebroadcast_task(
    r: redis.Redis,
    hub: Hub,
    node_cache: NodeCache,
    stop: asyncio.Event,
) -> None:
    """
    Subscribe to SvxLink-AI's pub/sub channel (svx:ai:events) and
    re-broadcast each session_updated event as a brief talking burst:

        node_upsert (ghost, online=True, isTalker=True)
        talk_start  (session, active=True)
        ... linger SVX_AI_LINGER_SECONDS ...
        node_upsert (ghost, online=False, isTalker=False)
        talk_stop   (session, active=False)

    Synthesises the ghost node from the session's own `node` block so the
    portal renders the AI-detected callsign at the operator's coordinates
    (which SvxLink-AI can override via a QRZ lookup) with a live talking
    indicator — without any portal-side changes.
    """
    channel = os.getenv("SVX_AI_CHANNEL", "svx:ai:events")
    linger  = env_float("SVX_AI_LINGER_SECONDS", 1.5)

    def _to_float(v):
        try:
            return float(v) if v not in (None, "") else None
        except Exception:
            return None

    while not stop.is_set():
        try:
            pubsub = r.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(channel)
            logging.info("AI rebroadcast subscribed to %s (linger=%.1fs)",
                         channel, linger)
            async for msg in pubsub.listen():
                if stop.is_set():
                    break
                if not msg or msg.get("type") != "message":
                    continue
                try:
                    raw = msg["data"]
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    payload = json.loads(raw)
                except Exception:
                    logging.warning("ai_rebroadcast: bad JSON payload")
                    continue
                if payload.get("type") != "session_updated":
                    continue
                session = payload.get("session") or {}
                callsign = session.get("callsign")
                if not callsign:
                    continue

                # If this callsign is a real reflector node tracked by
                # poll_task, skip — broadcasting a ghost would clobber the
                # real node's state in the portal and poll_task won't
                # re-emit until its own cache diff fires.
                if await node_cache.has(callsign):
                    logging.debug(
                        "ai_rebroadcast: skipping %s (matches real node)",
                        callsign,
                    )
                    continue

                src_node = session.get("node") or {}
                qth      = src_node.get("qth") or {}
                try:
                    tg = int(src_node.get("tg") or 0)
                except Exception:
                    tg = 0

                ghost_on = {
                    "callsign":     callsign,
                    "online":       True,
                    "isTalker":     True,
                    "tg":           tg,
                    "monitoredTGs": [tg] if tg else [],
                    "location":     (src_node.get("nodeLocation") or "AI-detected"),
                    "lat":          _to_float(qth.get("lat")),
                    "lon":          _to_float(qth.get("long")),
                }
                ghost_off = dict(ghost_on)
                ghost_off["online"]   = False
                ghost_off["isTalker"] = False

                start_evt = dict(session); start_evt["active"] = True
                stop_evt  = dict(session); stop_evt["active"]  = False

                await hub.broadcast({"type": "node_upsert", "node": ghost_on})
                await hub.broadcast({"type": "talk_start", "session": start_evt})
                try:
                    await asyncio.wait_for(stop.wait(), timeout=linger)
                    break
                except asyncio.TimeoutError:
                    pass
                await hub.broadcast({"type": "node_upsert", "node": ghost_off})
                await hub.broadcast({"type": "talk_stop", "session": stop_evt})
        except Exception as e:
            logging.warning("ai_rebroadcast: %s (reconnect in 2s)", e)
            try:
                await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass



def diff_nodes(prev: Dict[str, Dict[str, Any]], new: Dict[str, Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
    """Return list of (callsign, node_state) that changed."""
    changed: List[Tuple[str, Dict[str, Any]]] = []
    keys = set(prev.keys()) | set(new.keys())
    for cs in keys:
        if prev.get(cs) != new.get(cs):
            if cs in new:
                changed.append((cs, new[cs]))
    return changed


async def load_nodes_from_redis(r: redis.Redis, cfg: Config) -> Dict[str, Dict[str, Any]]:
    """
    Load the persisted node roster from Redis. Returns all known callsigns
    forced to offline — the next successful poll will re-confirm whichever
    are actually online.
    """
    raw = await r.hgetall(cfg.nodes_key)
    out: Dict[str, Dict[str, Any]] = {}
    for cs, val in raw.items():
        try:
            st = json.loads(val)
        except Exception:
            continue
        if not isinstance(st, dict):
            continue
        st["online"] = False
        st["isTalker"] = False
        st["tg"] = 0
        out[str(cs).upper()] = st
    return out


async def save_nodes_to_redis(
    r: redis.Redis,
    cfg: Config,
    changed: List[Tuple[str, Dict[str, Any]]],
) -> None:
    if not changed:
        return
    pipe = r.pipeline()
    for cs, st in changed:
        pipe.hset(cfg.nodes_key, cs, json.dumps(st, ensure_ascii=False))
    await pipe.execute()


async def close_active_sessions_due_to_stale(
    r: redis.Redis,
    cfg: Config,
    hub: Hub,
    prev_talkers: Set[str],
    active_session_key: Dict[str, str],
) -> Tuple[Set[str], Dict[str, str]]:
    """
    If the status feed is stale/unreachable, close any active talk sessions so the UI doesn't
    remain stuck on "Now".
    """
    if not prev_talkers:
        return set(), active_session_key

    end_ms = now_ms()
    for callsign in sorted(prev_talkers):
        session_key = active_session_key.pop(callsign, "")
        if session_key:
            raw = await r.get(session_key)
            if raw:
                try:
                    record = json.loads(raw)
                except Exception:
                    record = {"callsign": callsign}
            else:
                record = {"callsign": callsign}

            record.update(
                {
                    "end_ms": end_ms,
                    "end": iso_from_ms(end_ms),
                    "active": False,
                    "reason": "status_unreachable",
                }
            )
            await r.set(session_key, json.dumps(record, ensure_ascii=False), ex=cfg.ttl_seconds)
        else:
            record = {
                "callsign": callsign,
                "start_ms": None,
                "start": None,
                "end_ms": end_ms,
                "end": iso_from_ms(end_ms),
                "active": False,
                "reason": "status_unreachable",
            }

        await r.srem(cfg.active_set, callsign)
        await hub.broadcast({"type": "talk_stop", "session": record})

    return set(), active_session_key


async def mark_all_nodes_offline(
    r: redis.Redis,
    cfg: Config,
    hub: Hub,
    node_cache: NodeCache,
    prev_nodes: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Set all nodes to offline/isTalker False and broadcast + persist diffs.
    """
    new_nodes = dict(prev_nodes)
    for cs, st in list(new_nodes.items()):
        if st.get("online") is True or st.get("isTalker") is True or st.get("tg", 0) != 0:
            off = dict(st)
            off["online"] = False
            off["isTalker"] = False
            off["tg"] = 0
            new_nodes[cs] = off

    changes = diff_nodes(prev_nodes, new_nodes)
    for _cs, st in changes:
        await hub.broadcast({"type": "node_upsert", "node": st})

    await node_cache.replace(new_nodes)
    await save_nodes_to_redis(r, cfg, changes)
    return new_nodes


async def poll_task(
    r: redis.Redis,
    cfg: Config,
    hub: Hub,
    stop: asyncio.Event,
    node_cache: NodeCache,
) -> None:
    """
    Poll SvxReflector status endpoint and detect:
    - talk start/stop
    - online/offline
    - node state changes (tg, monitoredTGs, coords, location, isTalker)

    Robustness:
    - If status endpoint is unreachable, we use backoff + rate-limited logs.
    - If unreachable for cfg.status_stale_after_s, mark all nodes offline and close active talk sessions.
    """
    log = logging.getLogger("svx_talker_ws")

    prev_talkers: Set[str] = set()
    active_session_key: Dict[str, str] = {}

    # Restore the node roster from Redis so we remember offline nodes across
    # restarts. Everything is forced offline; the first successful poll will
    # flip the currently-connected ones back to online.
    prev_nodes: Dict[str, Dict[str, Any]] = await load_nodes_from_redis(r, cfg)
    if prev_nodes:
        log.info("Restored %d nodes from Redis (all marked offline).", len(prev_nodes))
        await node_cache.replace(prev_nodes)

    # monotonic timestamps for outage logic
    last_success_m = time.monotonic()
    failure_count = 0
    stale_applied = False
    last_fail_log_m = 0.0

    timeout = aiohttp.ClientTimeout(total=5)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not stop.is_set():
            loop_start = time.monotonic()

            try:
                async with session.get(cfg.status_url) as resp:
                    resp.raise_for_status()
                    payload = await resp.json(content_type=None)

                # success
                if failure_count > 0:
                    log.info("Status endpoint recovered after %d failures.", failure_count)
                failure_count = 0
                stale_applied = False
                last_success_m = time.monotonic()

                nodes = payload.get("nodes") or {}
                if not isinstance(nodes, dict):
                    nodes = {}

                current_online: Dict[str, Dict[str, Any]] = {}
                talker_raw: Dict[str, Dict[str, Any]] = {}

                for cs, node in nodes.items():
                    if not isinstance(node, dict):
                        continue
                    if node.get("hidden") is True:
                        continue

                    callsign = str(cs).upper()
                    st = make_node_state(callsign, node, online=True)
                    current_online[callsign] = st
                    if st["isTalker"]:
                        talker_raw[callsign] = node

                now_talkers = {cs for cs, st in current_online.items() if st["isTalker"]}
                started = now_talkers - prev_talkers
                stopped = prev_talkers - now_talkers

                # Talk start
                for callsign in sorted(started):
                    start_ms = now_ms()
                    session_key = f"{cfg.session_prefix}:{start_ms}:{callsign}"
                    record = {
                        "callsign": callsign,
                        "start_ms": start_ms,
                        "start": iso_from_ms(start_ms),
                        "end_ms": None,
                        "end": None,
                        "active": True,
                        "node": summarize_node(talker_raw.get(callsign, {})),
                    }

                    pipe = r.pipeline()
                    pipe.set(session_key, json.dumps(record, ensure_ascii=False), ex=cfg.ttl_seconds)
                    pipe.zadd(cfg.sessions_zset, {session_key: start_ms})
                    pipe.sadd(cfg.active_set, callsign)
                    cutoff = start_ms - (cfg.ttl_seconds * 1000)
                    pipe.zremrangebyscore(cfg.sessions_zset, 0, cutoff)
                    await pipe.execute()

                    active_session_key[callsign] = session_key
                    await hub.broadcast({"type": "talk_start", "session": record})

                # Talk stop
                for callsign in sorted(stopped):
                    end_ms = now_ms()
                    session_key = active_session_key.pop(callsign, "")

                    if session_key:
                        raw = await r.get(session_key)
                        if raw:
                            try:
                                record = json.loads(raw)
                            except Exception:
                                record = {"callsign": callsign}
                        else:
                            record = {"callsign": callsign}

                        record.update({"end_ms": end_ms, "end": iso_from_ms(end_ms), "active": False})
                        await r.set(session_key, json.dumps(record, ensure_ascii=False), ex=cfg.ttl_seconds)
                    else:
                        record = {
                            "callsign": callsign,
                            "start_ms": None,
                            "start": None,
                            "end_ms": end_ms,
                            "end": iso_from_ms(end_ms),
                            "active": False,
                        }

                    await r.srem(cfg.active_set, callsign)
                    await hub.broadcast({"type": "talk_stop", "session": record})

                prev_talkers = now_talkers

                # Online/offline + node changes
                prev_online_keys = {cs for cs, st in prev_nodes.items() if st.get("online") is True}
                current_online_keys = set(current_online.keys())
                went_offline = prev_online_keys - current_online_keys

                new_nodes = dict(prev_nodes)

                # update online nodes
                for cs, st in current_online.items():
                    new_nodes[cs] = st

                # mark offline nodes (keep last known coords/location/monitoredTGs for UI)
                for cs in went_offline:
                    prev = new_nodes.get(cs, {"callsign": cs})
                    off = dict(prev)
                    off["online"] = False
                    off["isTalker"] = False
                    off["tg"] = 0
                    new_nodes[cs] = off

                # broadcast diffs only + persist them
                changes = diff_nodes(prev_nodes, new_nodes)
                for _cs, st in changes:
                    await hub.broadcast({"type": "node_upsert", "node": st})

                prev_nodes = new_nodes
                await node_cache.replace(prev_nodes)
                await save_nodes_to_redis(r, cfg, changes)

                # sleep to maintain configured poll interval
                elapsed = time.monotonic() - loop_start
                await sleep_or_stop(stop, max(0.0, cfg.poll_interval_s - elapsed))
                continue

            except (
                aiohttp.ClientConnectorError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientResponseError,
                aiohttp.ClientPayloadError,
                aiohttp.ServerTimeoutError,
                asyncio.TimeoutError,
                ConnectionRefusedError,
                OSError,
            ) as e:
                # status endpoint unreachable (expected sometimes during restart)
                failure_count += 1

                now_m = time.monotonic()
                if (now_m - last_fail_log_m) >= cfg.poll_failure_log_every_s:
                    log.warning("Status poll failed (%s).", e)
                    last_fail_log_m = now_m

                # If stale for too long, push offline state once + close active talk sessions
                stale_for = now_m - last_success_m
                if (stale_for >= cfg.status_stale_after_s) and not stale_applied:
                    log.warning(
                        "Status feed unreachable for %.1fs: marking nodes offline and closing active sessions.",
                        stale_for,
                    )
                    prev_talkers, active_session_key = await close_active_sessions_due_to_stale(
                        r, cfg, hub, prev_talkers, active_session_key
                    )
                    prev_nodes = await mark_all_nodes_offline(r, cfg, hub, node_cache, prev_nodes)
                    stale_applied = True

                # Backoff delay
                backoff = cfg.poll_interval_s * (2 ** min(failure_count, 6))
                delay = min(cfg.poll_failure_max_sleep_s, max(cfg.poll_interval_s, backoff))
                await sleep_or_stop(stop, delay)
                continue

            except Exception:
                # Unexpected error: keep traceback but don't spin too fast
                failure_count += 1
                logging.exception("poll_task unexpected error")
                await sleep_or_stop(stop, min(cfg.poll_failure_max_sleep_s, 2.0))
                continue


async def main() -> None:
    cfg = Config()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logging.info("STATUS_URL=%s", cfg.status_url)
    logging.info("REDIS_URL=%s", cfg.redis_url)
    logging.info("POLL_INTERVAL=%.3fs", cfg.poll_interval_s)
    logging.info("WS=ws://%s:%d", cfg.ws_host, cfg.ws_port)

    stop = asyncio.Event()

    def request_stop(signame: str) -> None:
        logging.info("Received %s, shutting down...", signame)
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: request_stop(s.name))
        except NotImplementedError:
            pass

    r = redis.from_url(cfg.redis_url, decode_responses=True)
    await r.ping()

    hub = Hub()
    node_cache = NodeCache()

    tasks: List[asyncio.Task] = []

    reporter_task = install_ws_noise_suppression(stop)
    if reporter_task:
        tasks.append(reporter_task)

    tasks.extend(
        [
            asyncio.create_task(poll_task(r, cfg, hub, stop, node_cache), name="poll_task"),
            asyncio.create_task(prune_task(r, cfg, stop), name="prune_task"),
            asyncio.create_task(ai_rebroadcast_task(r, hub, node_cache, stop), name="ai_rebroadcast_task"),
        ]
    )

    # Entry point compatible with BOTH:
    # - old websockets: handler(ws, path)
    # - new websockets: handler(connection)
    async def ws_entrypoint(*args: Any) -> None:
        if len(args) == 1:
            ws = args[0]
            path = ""
        else:
            ws = args[0]
            path = args[1] if len(args) > 1 else ""
        await ws_handler(ws, str(path or ""), hub, r, cfg, node_cache)

    async with websockets.serve(
        ws_entrypoint,
        host=cfg.ws_host,
        port=cfg.ws_port,
        ping_interval=20,
        ping_timeout=20,
        max_size=2_000_000,
    ):
        logging.info("WebSocket server running.")
        await stop.wait()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    try:
        await r.aclose()  # redis>=5
    except AttributeError:
        await r.close()  # older redis-py

    logging.info("Stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
