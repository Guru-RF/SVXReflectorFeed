sudo apt update
sudo apt install -y redis-server python3.11-venv
sudo systemctl enable --now redis-server

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install aiohttp redis websockets

source .venv/bin/activate
python3 svx_talker_ws.py

mv this repo to /opt/svx-talker-ws/

/etc/systemd/system/svx-talker-ws.service

[Unit]
Description=SvxReflector Talker WebSocket + Redis TTL bridge
After=network.target redis-server.service
Wants=redis-server.service

[Service]
Type=simple
WorkingDirectory=/opt/svx-talker-ws
Environment=SVX_POLL_INTERVAL=0.05
Environment=WS_NOISE_REPORT_INTERVAL=60
Environment=SVX_STATUS_STALE_AFTER=3
Environment=SVX_POLL_FAILURE_MAX_SLEEP=5
Environment=SVX_POLL_FAILURE_LOG_EVERY=30
Environment=SVX_STATUS_URL=http://127.0.0.1:8080/status
Environment=SVX_POLL_INTERVAL=0.5
Environment=SVX_TTL_SECONDS=86400
Environment=REDIS_URL=redis://127.0.0.1:6379/0
Environment=SVX_WS_HOST=127.0.0.1
Environment=SVX_WS_PORT=8765
ExecStart=/opt/svx-talker-ws/.venv/bin/python /opt/svx-talker-ws/svx_talker_ws.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target

sudo systemctl daemon-reload
sudo systemctl enable --now svx-talker-ws
sudo journalctl -u svx-talker-ws -f


sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx

sudo mkdir -p /var/www/letsencrypt/.well-known/acme-challenge
sudo chown -R www-data:www-data /var/www/letsencrypt

vi /etc/nginx/sites-available/svx-ws

server {
    listen 80;
    listen [::]:80;
    server_name ws.example.com;

    # ACME challenge (Let's Encrypt)
    location ^~ /.well-known/acme-challenge/ {
        root /var/www/letsencrypt;
        default_type "text/plain";
    }

    # For now, return something simple (we'll change to redirect after cert exists)
    location / {
        return 200 "OK (HTTP). SSL not enabled yet.\n";
        add_header Content-Type text/plain;
    }
}

sudo ln -sf /etc/nginx/sites-available/svx-ws /etc/nginx/sites-enabled/svx-ws
sudo rm -f /etc/nginx/sites-enabled/default   # optional, avoids conflicts
sudo nginx -t
sudo systemctl reload nginx

sudo certbot certonly \
  --webroot -w /var/www/letsencrypt \
  -d ws.example.com \
  --email you@example.com --agree-tos --no-eff-email

(replace nginx config)
vi /etc/nginx/sites-available/svx-ws

# --- HTTP: only for ACME + redirect everything else to HTTPS ---
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

# --- HTTPS: WebSocket reverse proxy ---
server {
    listen 443 ssl;
    listen [::]:443 ssl;
    server_name ws.example.com;

    ssl_certificate     /etc/letsencrypt/live/ws.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/ws.example.com/privkey.pem;

    # Reasonable modern TLS defaults
    ssl_protocols TLSv1.2 TLSv1.3;

    # WebSocket connections are long-lived; prevent idle timeouts
    proxy_read_timeout  3600s;
    proxy_send_timeout  3600s;

    location / {
        proxy_pass http://127.0.0.1:8765;

        # WebSocket upgrade (required)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Usual proxy headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Lower latency for WS
        proxy_buffering off;
    }
}


sudo nginx -t
sudo systemctl reload nginx

testing (on mac)
brew install websocat
websocat wss://<url>

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
ExecStart=/opt/svx-talker-ws/.venv/bin/python /opt/svx-talker-ws/svx_talker_ws.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target

sudo journalctl -u svx-talker-ws -f
