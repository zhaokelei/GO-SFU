# YSP SFU - Selective Forwarding Unit

[中文](README.md) | English

A standard SFU (Selective Forwarding Unit) built with Go + pion/webrtc v3.

All audio/video media traffic is relayed through the server; browsers never establish direct P2P connections.

## Features

- **Pure RTP packet forwarding**: no decoding, no encoding, no transcoding — RTP packets are copied and forwarded to subscribers
- **NACK retransmission**: a sliding window caches recent RTP packets and retransmits lost ones on NACK requests
- **PLI keyframe requests**: subscriber PLIs are passed through to the publisher, fixing corruption / green frames after packet loss
- **Bidirectional RTCP passthrough**: SenderReport / ReceiverReport are forwarded both ways to keep audio and video in sync
- **Sequence number validation**: drops duplicate and out-of-order packets to avoid artifacts
- **Jitter buffer**: smooths RTP arrival time differences to reduce stutter and audio glitches
- **Malformed packet filtering**: detects and drops RTP packets with an invalid version or abnormal length
- **SSRC isolation**: audio/video SSRCs are isolated within a session to avoid stream mixing
- **NAT traversal**: supports `--public-ip` (NAT1To1IPs), built-in STUN, optional TURN
- **Connection state monitoring**: ICE / PeerConnection state changes automatically trigger resource cleanup
- **WebSocket heartbeat**: periodic ping/pong with timeout disconnection
- **Automatic resource cleanup**: on session teardown all senders/receivers are closed and caches are cleared, preventing goroutine leaks
- **Ordered event dispatch**: room events are processed sequentially through a single goroutine queue, avoiding concurrency races
- **Single EXE deployment**: the front-end page is embedded via `go:embed`; static build with no external dependencies
- **Non-blocking logging**: async logging (console + file, each with its own buffered queue) so a blocked output never stalls the SFU
- **Console freeze protection**: on Windows the console "QuickEdit Mode" is disabled at startup, so clicking the black window can no longer suspend the process

## Project structure

```
ysp/
├── main.go            # Entry point: CLI flags, HTTP server, WebRTC init
├── room.go            # Room management: session lifecycle, ordered event dispatch, stats
├── session.go         # Session management: PeerConnection wrapper, track pub/sub, RTCP passthrough
├── signaling.go       # WebSocket signaling: message handling, heartbeat, message filtering
├── rtp_buffer.go      # RTP handling: sliding window cache, sequence check, jitter buffer, SSRC map
├── logger.go          # Leveled logging (INFO/WARN/ERROR), async non-blocking, console + file
├── console_windows.go # Windows: disable console QuickEdit at startup to prevent freezes
├── console_other.go   # Non-Windows: no-op implementation for cross-platform builds
├── index.html         # Front-end test page (embedded in the binary)
├── go.mod
├── README.md          # Chinese documentation
└── README.en.md       # English documentation
```

## Build

### 1. Initialize the module (first time)

```bash
go mod init ysp-sfu
```

### 2. Fetch dependencies

```bash
go mod tidy
```

### 3. Static build → single EXE

**Windows (building on Windows):**

```powershell
$env:CGO_ENABLED=0
$env:GOOS="windows"
$env:GOARCH="amd64"
go build -o sfu.exe .
```

**Cross-compiling a Windows EXE from Linux:**

```bash
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o sfu.exe .
```

The output `sfu.exe` is a standalone file — no HTML, DLL, or other external files required. Copy it to any Windows machine and double-click to run.

## Run

### Command-line flags

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `:2033` | Listen address |
| `--public-ip` | (required) | Public IP of the server, used for NAT1To1IPs |
| `--stun` | `stun:stun.chat.bilibili.com:3478,stun:stun.cloudflare.com:3478,stun:stun.miwifi.com:3478` | STUN servers (comma-separated) |
| `--turn-url` | empty | TURN URL (optional) |
| `--turn-user` | empty | TURN username (optional) |
| `--turn-pass` | empty | TURN password (optional) |
| `--jitter-buffer-ms` | `50` | Jitter buffer duration (ms) |
| `--nack-cache-size` | `512` | Number of packets in the NACK cache |
| `--max-sessions` | `100` | Max sessions per room |
| `--log-level` | `INFO` | Log level (INFO/WARN/ERROR) |
| `--log-file` | `sfu.log` | Log file path (empty = console only) |

### Local testing

```bash
sfu.exe --public-ip 127.0.0.1
```

Open `http://127.0.0.1:2033` in a browser, enter a room ID and a session ID, then click connect.

### Public deployment

```bash
sfu.exe --public-ip <your-public-ip>
```

With a TURN relay as fallback:

```bash
sfu.exe --public-ip <public-ip> --turn-url turn:<turn-server>:3478 --turn-user <username> --turn-pass <password>
```

## Troubleshooting: cannot connect after clicking the console window / call freezes

**Symptom**: after clicking the cmd / PowerShell window on the server, the front end keeps failing to connect; pressing Enter in the window makes the connection succeed instantly. During a call, clicking the black window freezes the video within about two seconds.

**Cause**: the Windows console enables "QuickEdit Mode" by default. Clicking inside the console enters text-selection mode, and any write to the console from the process is blocked by the OS. Since the SFU originally logged synchronously to the console, a stalled log write would freeze the whole process (signaling unresponsive, media relaying stopped). Pressing Enter clears the selection and unblocks writes — hence the connection succeeds "the moment you press Enter".

**Fixes built in** (this version):

1. On startup the program calls the Windows API to disable console "QuickEdit Mode", eliminating the freeze at its source;
2. Logging is now **async and non-blocking**, and is also written to the file given by `--log-file` (default `sfu.log`). Even if the console freezes, signaling and media relaying are unaffected and logs are still written to disk completely.

**Manual workaround** (for older versions): right-click the cmd title bar → Properties → Options → uncheck "QuickEdit Mode"; or start with redirection, e.g. `sfu.exe --public-ip <IP> > run.log 2>&1`.

## HTTPS deployment (Nginx reverse proxy)

Browsers (especially iOS Safari / Android Chrome) only allow camera and microphone access in a **secure context**, so mobile testing requires HTTPS. The recommended setup: **Nginx handles HTTPS (TLS termination) while the SFU backend stays plain HTTP** — no certificate handling inside the program.

### Architecture

```
Mobile / desktop browser
    │  https://sfu.example.com      ← page + signaling (TCP 443)
    ▼
Nginx (TLS termination)
    │  http://127.0.0.1:2033       ← reverse proxy, incl. WebSocket upgrade
    ▼
SFU backend (HTTP + WS)
    ▲
    │  WebRTC media (UDP, bypasses Nginx)
    └────────────────────────────  browser connects directly to the server over UDP
```

> **Key point**: Nginx only proxies HTTP / WebSocket (TCP). **WebRTC media is UDP and does not pass through Nginx.**
> Therefore:
> 1. `--public-ip` must still be set to a server address the browser can reach;
> 2. the UDP media port range must still be exposed publicly (see "Firewall notes").

### 1. Prepare certificates

**Option A: public domain (recommended, the certificate is trusted out of the box)**

```bash
# Ubuntu / Debian
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d sfu.example.com
```

**Option B: LAN only / no domain (self-signed)**

```bash
sudo mkdir -p /etc/nginx/ssl
openssl req -x509 -newkey rsa:2048 -nodes -days 825 \
  -keyout /etc/nginx/ssl/key.pem \
  -out /etc/nginx/ssl/cert.pem \
  -subj "/CN=192.168.1.100" \
  -addext "subjectAltName=IP:192.168.1.100,IP:127.0.0.1,DNS:localhost"
```

Replace `192.168.1.100` with the server's actual IP. If mkcert is installed, you can generate and trust it in one step:

```bash
mkcert -install
mkcert 192.168.1.100 localhost 127.0.0.1
```

### 2. Configure Nginx

First add this `map` to the `http { }` block (to detect WebSocket upgrades correctly):

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
```

Then create `/etc/nginx/conf.d/sfu.conf`:

```nginx
server {
    listen 443 ssl;
    http2 on;                      # nginx < 1.25.1: use "listen 443 ssl http2;"
    server_name sfu.example.com;   # for LAN self-signed setups, use the IP, e.g. 192.168.1.100

    ssl_certificate     /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_session_cache   shared:SSL:10m;

    location / {
        proxy_pass http://127.0.0.1:2033;
        proxy_http_version 1.1;

        # Required for WebSocket upgrade
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection $connection_upgrade;

        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Signaling is a long-lived connection; raise the timeouts or it will reconnect frequently
        proxy_read_timeout    3600s;
        proxy_send_timeout    3600s;
        proxy_connect_timeout 10s;

        # Disable response buffering to reduce signaling latency
        proxy_buffering off;
    }
}

# Redirect HTTP to HTTPS automatically
server {
    listen 80;
    server_name sfu.example.com;
    return 301 https://$host$request_uri;
}
```

Test and reload:

```bash
sudo nginx -t && sudo nginx -s reload
```

### 2.1 BT Panel (BaoTa) configuration

BT Panel generates the site's main config automatically (`listen` / `server_name` / `root` / SSL / HTTP redirect, etc.).
**Do not replace it wholesale.** Just paste the block below anywhere inside `server { }` in the site's config file
(e.g. after `#SSL-INFO-END`), save, and click "Reload config" in the panel:

```nginx
    # ==== YSP SFU backend reverse proxy: page + signaling WebSocket ====
    location / {
        proxy_pass http://127.0.0.1:2033;
        proxy_http_version 1.1;

        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Required for WebSocket upgrade: without these two lines the /ws handshake fails (backend logs "upgrade token not found")
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Signaling is a long-lived connection; raise the timeouts or it will reconnect frequently
        proxy_read_timeout    3600s;
        proxy_send_timeout    3600s;
        proxy_connect_timeout 10s;

        # Disable response buffering to reduce signaling latency
        proxy_buffering off;
    }
```

> **Notes**:
> 1. The front-end page is embedded in `sfu.exe`, so a single `location /` forwards **both the page and `/ws` signaling**
>    to the backend — no need to place files under `wwwroot`.
> 2. If you only want to proxy signaling and serve the page from a BT static directory, change `location /` to `location /ws`.
> 3. Keep BT's own `#PHP-INFO` / `#REWRITE` / `#redirect` / `#static-cache` includes at their panel defaults.
> 4. If the panel's `static_cache` defines a `location` with `expires` for `\.(js|css|png|jpg...)`,
>    it takes precedence over `location /` and will intercept those requests — comment out that include if needed.

### 3. Start the backend

Nginx already terminates TLS, so keep the backend on plain HTTP:

```bash
sfu.exe --addr 127.0.0.1:2033 --public-ip 192.168.1.100
```

- `--addr 127.0.0.1:2033`: only the local Nginx can reach it, preventing direct connections that bypass HTTPS (safer)
- `--public-ip`: an address the browser can actually reach (LAN IP for LAN, public IP for the internet)

### 4. Mobile access

Open `https://192.168.1.100` (or your domain) in a phone browser:

- **Domain + Let's Encrypt**: the certificate is trusted and works immediately.
- **Self-signed certificate**: you must install and **trust** it first, otherwise the browser disables camera/microphone:
  - **iOS**: transfer `cert.pem` to the phone → Settings → Profile Downloaded → Install → then Settings → General → About → Certificate Trust Settings → enable full trust
  - **Android**: Settings → Security → Encryption & credentials → Install a certificate → CA certificate
  - **Windows / macOS**: double-click the certificate → install it into "Trusted Root Certification Authorities"

> Tapping "Continue" on the certificate warning page alone is **not enough**: the browser will not treat it as a secure context and `getUserMedia` will still be rejected.

### 5. Verify

- The page opens, the address bar shows a padlock and no warning
- The front-end log shows `connecting wss://<IP>/ws` and then "signaling connected"
- No `Mixed Content` errors in the browser console (the front end picks `ws` / `wss` based on the page protocol)
- Camera and microphone can be accessed normally

## Firewall notes

### Ports that must be open

| Port | Protocol | Purpose |
|------|----------|---------|
| 443 | TCP | HTTPS page + WSS signaling (via Nginx, publicly exposed) |
| 80 | TCP | Automatic HTTP → HTTPS redirect (optional) |
| 2033 | TCP | Backend HTTP + WebSocket (under the Nginx setup, better to bind 127.0.0.1 only) |
| 49152-65535 | UDP | WebRTC media (ICE/DTLS/SRTP), **must be publicly exposed** |

### Example (Linux iptables)

```bash
# Allow signaling and the web page
iptables -A INPUT -p tcp --dport 2033 -j ACCEPT

# Allow the WebRTC media UDP port range
iptables -A INPUT -p udp --dport 49152:65535 -j ACCEPT
```

### Windows Firewall

Add inbound rules in "Windows Defender Firewall" for:
- TCP 2033
- UDP 49152-65535

## Cross-border deployment notes

1. **Public IP is mandatory**: `--public-ip` is required, otherwise browsers cannot reach the server.
2. **STUN server choice**: on cross-border networks, prefer a STUN server close to your server, or self-host STUN/TURN.
3. **TURN fallback**: cross-border links have high packet loss; configure a TURN server as a relay fallback to guarantee connectivity.
4. **Jitter buffer tuning**: cross-border links are jittery; increase `--jitter-buffer-ms` (e.g. 100-200 ms) to trade latency for smoothness.
5. **NACK cache**: with heavy cross-border loss, increase `--nack-cache-size` (e.g. 50-100) to improve retransmission success.
6. **Cloud security groups**: make sure the TCP/UDP ports above are allowed in your cloud provider's security group.
7. **Bandwidth planning**: in SFU mode, server egress = subscribers × publisher bitrate; plan bandwidth in advance.

## Signaling protocol

All signaling goes over WebSocket, with messages in JSON:

```json
{ "type": "message-type", "data": { ... } }
```

### Client → Server

| type | Description | data |
|------|-------------|------|
| `join` | Join a room | `{ roomId, sessionId, name }` (`sessionId` is the unique connection id, `name` is the display nickname and may repeat) |
| `offer` | Push an SDP Offer | `{ sdp }` |
| `answer` | Reply with an SDP Answer | `{ sdp }` |
| `ice_candidate` | Trickle ICE candidate | `{ candidate, sdpMid, sdpMLineIndex }` |
| `subscribe` | Subscribe to a track | `{ publisherId, trackId }` |
| `unsubscribe` | Unsubscribe from a track | `{ publisherId, trackId }` |
| `list_tracks` | Get the room's track list | - |
| `ping` | Heartbeat | - |

### Server → Client

| type | Description |
|------|-------------|
| `join_success` | Joined successfully |
| `sessions_list` | Existing sessions in the room (each with `sessionId` and `name`) |
| `tracks_list` | Room track list |
| `track_published` | A new track was published |
| `track_unpublished` | A track was unpublished |
| `session_join` | A session joined (with `sessionId` and `name`) |
| `session_leave` | A session left (with `sessionId` and `name`) |
| `offer` | Server-initiated renegotiation Offer |
| `answer` | Server Answer |
| `ice_candidate` | Server ICE candidate |
| `error` | Error notification |
| `pong` | Heartbeat reply |

## Design notes

- **No Peer struct**: the business layer only has Room and Session, using pion's `webrtc.PeerConnection` directly as the browser↔server WebRTC connection.
- **Pure packet forwarding**: after sequence validation and filtering, received RTP packets are copied to every subscriber's `TrackLocalStaticRTP`.
- **RTCP passthrough**: subscriber PLI/NACK is passed to the publisher browser via the publisher's `PeerConnection.WriteRTCP`; publisher SR/RR is handled automatically by pion interceptors.
- **Resource cleanup**: when a session closes, a `context.CancelFunc` signals all packet-reading goroutines to exit, the PeerConnection is closed to release all senders/receivers, and the RTP cache is cleared.

## Limitations

- No simulcast, SVC, or transcoding
- No recording
- For internal testing; no JWT authentication
