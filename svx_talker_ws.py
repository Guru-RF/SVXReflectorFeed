#!/usr/bin/env python3
"""
svx_talker_ws.py

Polls SvxReflector status endpoint, detects when a node starts/stops talking,
stores each talk "session" in Redis with a 24h TTL, and serves BOTH:

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
- svx:session:<start_ms>:<callsign>  -> JSON session record, EX 86400
- svx:sessions                       -> ZSET index (score=start_ms, member=session_key)
- svx:active                         -> SET of callsigns currently talking

Env vars:
- SVX_STATUS_URL     (default http://127.0.0.1:8080/status)
- SVX_POLL_INTERVAL  (default 0.5 seconds)
- REDIS_URL          (default redis://127.0.0.1:6379/0)
- SVX_TTL_SECONDS    (default 86400)
- SVX_SNAPSHOT_LIMIT (default 500)
- SVX_WS_HOST        (default 127.0.0.1)
- SVX_WS_PORT        (default 8765)
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
    """
    Reduced subset for sessions.
    """
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
    """
    Stable node state for the UI. Avoid fields that change every poll, otherwise
    you'd broadcast constantly.
    """
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

    redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    ttl_seconds: int = env_int("SVX_TTL_SECONDS", 24 * 60 * 60)  # 24h
    snapshot_limit: int = env_int("SVX_SNAPSHOT_LIMIT", 500)

    ws_host: str = os.getenv("SVX_WS_HOST", "127.0.0.1")
    ws_port: int = env_int("SVX_WS_PORT", 8765)

    # Redis namespaces
    session_prefix: str = os.getenv("SVX_SESSION_PREFIX", "svx:session")
    sessions_zset: str = os.getenv("SVX_SESSIONS_ZSET", "svx:sessions")
    active_set: str = os.getenv("SVX_ACTIVE_SET", "svx:active")


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
        await asyncio.sleep(60)


def diff_nodes(prev: Dict[str, Dict[str, Any]], new: Dict[str, Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Return list of (callsign, node_state) that changed.
    """
    changed: List[Tuple[str, Dict[str, Any]]] = []
    keys = set(prev.keys()) | set(new.keys())
    for cs in keys:
        if prev.get(cs) != new.get(cs):
            if cs in new:
                changed.append((cs, new[cs]))
    return changed


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
    """
    prev_talkers: Set[str] = set()
    active_session_key: Dict[str, str] = {}

    # callsign -> node_state (includes offline nodes we've seen before)
    prev_nodes: Dict[str, Dict[str, Any]] = {}

    timeout = aiohttp.ClientTimeout(total=5)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not stop.is_set():
            try:
                async with session.get(cfg.status_url) as resp:
                    resp.raise_for_status()
                    payload = await resp.json(content_type=None)

                nodes = payload.get("nodes") or {}
                if not isinstance(nodes, dict):
                    nodes = {}

                # Build online set and talkers
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
                        # If we restarted mid-talk, we may not know start_ms. Still emit stop.
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

                # broadcast diffs only
                for cs, st in diff_nodes(prev_nodes, new_nodes):
                    await hub.broadcast({"type": "node_upsert", "node": st})

                prev_nodes = new_nodes
                await node_cache.replace(prev_nodes)

            except Exception:
                logging.exception("poll_task error")

            await asyncio.sleep(cfg.poll_interval_s)


async def main() -> None:
    cfg = Config()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logging.info("STATUS_URL=%s", cfg.status_url)
    logging.info("REDIS_URL=%s", cfg.redis_url)
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

    tasks = [
        asyncio.create_task(poll_task(r, cfg, hub, stop, node_cache), name="poll_task"),
        asyncio.create_task(prune_task(r, cfg, stop), name="prune_task"),
    ]

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
        await r.close()   # older redis-py

    logging.info("Stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
