# SVXReflectorFeed

Polls a local SvxReflector `/status` endpoint, tracks talk sessions in Redis
(TTL by default 24h), and serves live node state + talk start/stop events
over a WebSocket. Optionally re-broadcasts SvxLink-AI sessions (callsigns
detected by the AI bridge) as short live talking bursts on the same WS.

## Requirements

- Debian / Ubuntu (instructions assume `apt`)
- Python 3.11+
- Redis 6+
- Nginx + Let's Encrypt (only if you want TLS / public access)

## Install

```bash
# System packages
sudo apt update
sudo apt install -y redis-server python3.11-venv
sudo systemctl enable --now redis-server

# Place the repo at /opt/svx-talker-ws
sudo mv /path/to/SVXReflectorFeed /opt/svx-talker-ws
cd /opt/svx-talker-ws

# Python venv + deps
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install aiohttp redis websockets
```

Test-run in the foreground:

```bash
source .venv/bin/activate
python3 svx_talker_ws.py
```

## systemd service

Create `/etc/systemd/system/svx-talker-ws.service`:

```ini
[Unit]
Description=SvxReflector Talker WebSocket + Redis TTL bridge
After=network.target redis-server.service
Wants=redis-server.service

[Service]
Type=simple
WorkingDirectory=/opt/svx-talker-ws
Environment=SVX_STATUS_URL=http://127.0.0.1:8080/status
Environment=SVX_POLL_INTERVAL=0.5
Environment=SVX_TTL_SECONDS=86400
Environment=REDIS_URL=redis://127.0.0.1:6379/0
Environment=SVX_WS_HOST=127.0.0.1
Environment=SVX_WS_PORT=8765
Environment=SVX_STATUS_STALE_AFTER=3
Environment=SVX_POLL_FAILURE_MAX_SLEEP=5
Environment=SVX_POLL_FAILURE_LOG_EVERY=30
Environment=WS_NOISE_REPORT_INTERVAL=60
# SvxLink-AI bridge (optional)
Environment=SVX_AI_CHANNEL=svx:ai:events
Environment=SVX_AI_LINGER_SECONDS=1.5
ExecStart=/opt/svx-talker-ws/.venv/bin/python /opt/svx-talker-ws/svx_talker_ws.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

Enable + tail:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now svx-talker-ws
sudo journalctl -u svx-talker-ws -f
```

## Nginx + TLS (optional, for public `wss://`)

```bash
sudo apt install -y nginx certbot python3-certbot-nginx
sudo mkdir -p /var/www/letsencrypt/.well-known/acme-challenge
sudo chown -R www-data:www-data /var/www/letsencrypt
```

Start with a bootstrap HTTP-only config at `/etc/nginx/sites-available/svx-ws`
so certbot can complete the ACME challenge:

```nginx
server {
    listen 80;
    listen [::]:80;
    server_name ws.example.com;

    location ^~ /.well-known/acme-challenge/ {
        root /var/www/letsencrypt;
        default_type "text/plain";
    }

    location / {
        return 200 "OK (HTTP). SSL not enabled yet.\n";
        add_header Content-Type text/plain;
    }
}
```

```bash
sudo ln -sf /etc/nginx/sites-available/svx-ws /etc/nginx/sites-enabled/svx-ws
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx

sudo certbot certonly --webroot -w /var/www/letsencrypt \
    -d ws.example.com \
    --email you@example.com --agree-tos --no-eff-email
```

Replace the config with the WebSocket reverse-proxy version:

```nginx
# HTTP: ACME + redirect to HTTPS
server {
    listen 80;
    listen [::]:80;
    server_name ws.example.com;

    location ^~ /.well-known/acme-challenge/ {
        root /var/www/letsencrypt;
        default_type "text/plain";
    }

    location / {
        return 301 https://$host$request_uri;
    }
}

# HTTPS: WebSocket reverse proxy
server {
    listen 443 ssl;
    listen [::]:443 ssl;
    server_name ws.example.com;

    ssl_certificate     /etc/letsencrypt/live/ws.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/ws.example.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    # WS connections are long-lived
    proxy_read_timeout  3600s;
    proxy_send_timeout  3600s;

    location / {
        proxy_pass http://127.0.0.1:8765;

        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_buffering off;
    }
}
```

```bash
sudo nginx -t && sudo systemctl reload nginx
```

## Testing

From a Mac:

```bash
brew install websocat
websocat wss://ws.example.com
```

On connect, the server sends a `snapshot` message; thereafter you'll receive
`node_upsert`, `talk_start`, `talk_stop`, and `node_remove` events live. A
`node_remove` (`{"type":"node_remove","callsign":"ON0XXX"}`) is sent when a node
has been offline longer than `SVX_OFFLINE_PRUNE_AFTER`; clients should drop that
callsign from their local roster.

A node without a real position (QTH lat/long `0.0`, e.g. an EchoLink bridge)
gets `lat`/`lon` `null`, so clients don't place it at 0,0.

## Bridged talkers

A bridge node (EchoLink, D-STAR, ...) tells the reflector which station it is
relaying before each over, and SvxReflector reports it as `talkerAlias` on that
node in `/status`. While such a node talks with an alias, the feed reports the
talker as the pseudo-node `<node>/<alias>`, e.g. `ECHOLINK/ON6URE`, instead of
the bridge itself. Every alias gets its own node state, sessions and last-heard,
so ON6URE and ON4ABC via EchoLink are separate entries. The pseudo-node and its
sessions carry `"via": "ECHOLINK"` and `"alias": "ON6URE"`.

- The pseudo-node is online only while it talks, and is pruned like any other
  offline node.
- It takes the position and location of the reflector node named like the
  alias (`ON6URE`) when that node has one; otherwise its `lat`/`lon` are `null`.
- The bridge node itself does not show `isTalker` while one of its relayed
  stations talks.
- Without an alias (or on reflector builds without `talkerAlias`) nothing
  changes. A blocked alias (`talkerAliasBlocked`) never talks, so it never shows
  up.

## Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `SVX_STATUS_URL` | `http://127.0.0.1:8080/status` | SvxReflector status endpoint |
| `SVX_POLL_INTERVAL` | `0.5` | Poll interval (s); try `0.1` or `0.05` for short PTTs |
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis connection |
| `SVX_TTL_SECONDS` | `86400` | Session retention (s) |
| `SVX_SNAPSHOT_LIMIT` | `500` | Max sessions in connect snapshot |
| `SVX_OFFLINE_PRUNE_AFTER` | `86400` | Drop nodes offline for at least this many seconds (`0` keeps them forever) |
| `SVX_WS_HOST` | `127.0.0.1` | WS bind host |
| `SVX_WS_PORT` | `8765` | WS bind port |
| `SVX_STATUS_STALE_AFTER` | `3.0` | Mark nodes offline after this many seconds without a successful poll |
| `SVX_POLL_FAILURE_MAX_SLEEP` | `5.0` | Max backoff between failed polls |
| `SVX_POLL_FAILURE_LOG_EVERY` | `30.0` | Rate-limit for poll-failure warnings |
| `WS_NOISE_REPORT_INTERVAL` | `60` | Periodic summary of suppressed non-WS HTTP hits (`0` disables) |
| `SVX_AI_CHANNEL` | `svx:ai:events` | Pub/sub channel for SvxLink-AI sessions |
| `SVX_AI_LINGER_SECONDS` | `1.5` | How long an AI-detected callsign stays "talking" on the WS |

## Maintenance

Nodes that stay offline longer than `SVX_OFFLINE_PRUNE_AFTER` (default 24h) are
dropped automatically, emitting a `node_remove` event. To clear offline nodes on
demand — e.g. a backlog from before the auto-prune existed — use the helper
(run it with the same interpreter as the service so `redis` is available):

```bash
# Dry run (safe): report what would be removed
/opt/svx-talker-ws/.venv/bin/python remove_offline_nodes.py

# Remove all offline nodes
/opt/svx-talker-ws/.venv/bin/python remove_offline_nodes.py --apply

# Remove only nodes offline for at least 24h (7d, 30m, 3600 also accepted)
/opt/svx-talker-ws/.venv/bin/python remove_offline_nodes.py --older-than 24h --apply
```

It honours `REDIS_URL` and `SVX_NODES_KEY`. Since the running feed caches the
roster in memory, restart it afterwards so live clients stop seeing the removed
nodes: `sudo systemctl restart svx-talker-ws`.

## License

[MIT](LICENSE) © Joeri Van Dooren
