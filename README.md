<p align="center">
  <img src="docs/assets/tailcat.png" alt="Tailcat" width="96">
</p>

<h1 align="center">Tailcat WebUI</h1>

<p align="center">A multi-user Tailcat control plane and responsive web console.</p>

<p align="center">
  <a href="README_ZH.md">简体中文</a> ·
  <a href="https://github.com/ca-x/tailcat-webui/actions/workflows/ci.yml">CI</a> ·
  <a href="docs/openapi.yaml">OpenAPI</a>
</p>

Tailcat WebUI turns [Tailcat](https://github.com/tailscale/tailcat) into a
long-running, OIDC-authenticated application. Each user can operate multiple
independent Tailcat servers and clients. Every durable resource belongs to its
OIDC owner. Remote HTTP, SSE, and WebSocket resources are published below
stable subroutes on an isolated public origin.

## Screenshots

### Server management · desktop light theme

![Tailcat server management](docs/screenshots/server-desktop-light.png)

### Network overview · mobile dark theme in Simplified Chinese

<p align="center">
  <img src="docs/screenshots/mobile-dashboard-dark-zh.png" alt="Tailcat mobile dashboard in dark mode and Simplified Chinese" width="390">
</p>

Both images are captured from the running embedded application, not mockups.

### Diagnostics · desktop light theme

<p align="center">
  <img src="docs/screenshots/diagnostics-desktop-light.png" alt="Successful Tailcat peer diagnostics in the desktop light theme" width="960">
</p>

Source PNG: 1440 × 900.

### Secure transfers · mobile dark theme in Simplified Chinese

<p align="center">
  <img src="docs/screenshots/transfers-mobile-dark-zh.png" alt="Completed verified Tailcat file transfer in the mobile dark theme and Simplified Chinese" width="390">
</p>

Source PNG: 390 × 844. The screenshots contain demo names and operation
summaries only. Connection tokens and one-time share codes were dismissed
before capture.

## Capabilities

| Tailcat capability | WebUI implementation |
| --- | --- |
| Pipe stdin/stdout | Authenticated binary WebSocket TCP tunnel |
| Expose local TCP ports | Per-server port mappings with deployment target policy |
| Auth-free SSH server | Available only in explicit loopback demo mode; production uses TCP forwarding to a hardened SSH daemon |
| Ping and direct-path detection | Client ping with direct, DERP, or peer-relay status |
| SOCKS-style arbitrary TCP dialing | Browser TCP tunnel accepts `host:port` through the selected Tailcat client |
| Exit node | Per-server option, constrained by allowed destination CIDRs |
| Parse and resolve tokens | Built-in token tools and API endpoints |
| Ephemeral and saved keys | Per-resource choice; saved private keys are AES-256-GCM encrypted |
| Client allowlist | Named public keys; first enable/revocation safely stops a live server to apply fail-closed policy |
| DNS tokens | Resolves `tailcat=tc…` TXT records when creating clients |
| Custom DERP | Region ID/code, custom host list, or alternate DERP map URL |
| Multiple instances | Independent server/client runtimes in one process and per user |
| Port-aware target policy | Deployment rules accept CIDR or exact domain targets with explicit port ranges |
| Network diagnostics | Owner-scoped ping and bounded duplex throughput history over reserved TCP port `41640` |
| Secure file transfer | Browser-staged, resumable BLAKE3-verified transfers over reserved TCP port `41641` |

Additional product features:

- OIDC authorization-code flow with state, nonce, PKCE, server-side sessions,
  HTTP-only cookies, and owner-scoped queries.
- Public or owner-only `/r/{slug}/*` routes with streaming and WebSocket support.
- React 19 + Ant Design 6 interface using framework components for navigation,
  forms, drawers, dialogs, confirmations, tables, and notifications.
- English and Simplified Chinese; light, dark, and system appearance.
- Pure-Go SQLite through Ent and `github.com/lib-x/entsqlite`; no CGO.
- One embedded binary plus Linux amd64/arm64 container images.

## Stock Tailcat compatibility

Standard Tailcat connection tokens remain compatible in both directions. A
stock client can connect to a WebUI-managed server with
`tailcat <token> <port>`. The stock `tailcat ping`, `tailcat socks`, and
`tailcat ssh` behaviors, including the server allowlist key check, remain the
upstream Tailcat behaviors. A WebUI-managed client can also use a token from a
stock Tailcat server.

The WebUI diagnostics-history protocol on TCP `41640` and staged BLAKE3
transfer protocol on TCP `41641` require WebUI-aware peers. The stock CLI has
its own ping command, but it cannot participate in either application
protocol. Exit-node rules in this project are WebUI policy controls; this
project does not claim compatibility with original CLI exit routing.

## Operations and limits

Diagnostics always target the selected Tailcat client. The peer service is
fixed to TCP `41640`, so the API cannot choose an arbitrary speed-test host.
Each run is limited to 5 seconds and 32 MiB in each direction. An owner may run
two diagnostics at once, with at most one per client. The database keeps the
newest 100 summaries for up to 30 days and does not store peer IP addresses or
progress samples.

Transfers use browser-selected files only. The sender stages bytes under its
data directory, finalizes an immutable share, and receives a capability code
that is shown once. Rotating the code revokes the previous value. The receiver
saves an encrypted code only when a job needs restart or resume. TCP `41641`
serves fixed manifest and range operations, not filesystem browsing.
The drawer and background progress card can cancel an active browser upload.
Closing the drawer or leaving the page also aborts the current request, while
files already staged remain available for retry.

Compiled transfer ceilings are 512 MiB per file, 1 GiB per outgoing share or
incoming job, 2 GiB of staged bytes per owner, 1,000 files per share, and 4,096
retained files per owner. Fixed owner-wide metadata caps allow 128 retained
outgoing shares and 128 retained incoming jobs; pending creations count toward
admission. A job uses exactly four range workers, with at most two active jobs
per owner. Shares and jobs default to a 24-hour lifetime; operators may tighten
it from 1 second up to the 24-hour ceiling. A service-owned scheduler enforces
expiry continuously and retries failed cleanup. BLAKE3 manifests use 8 MiB
blocks, and every completed file receives a final whole-file hash check.

Transfer storage uses owner/share/job rooted directories and random disk names.
Virtual paths are normalized relative paths, never host filesystem paths.
The storage layer rejects absolute paths, dot segments, controls, symlinks,
Windows reparse escapes, unsafe hard links, and root replacement. Staged files
are private, fsynced, atomically published, and removed through owner-scoped
deletion. SQLite and Tailcat WebUI remain pure Go with `CGO_ENABLED=0`.
Deleting a Tailcat server or client first cancels and removes its dependent
shares or jobs and staged bytes. If cleanup fails, the parent row remains so
the deletion can be retried.

## Quick start

Requirements: Go 1.27.0, Node.js 26, and pnpm 11.3.

```sh
git clone https://github.com/ca-x/tailcat-webui.git
cd tailcat-webui
cd web && pnpm install --frozen-lockfile --ignore-scripts && cd ..
make build
```

Create an OIDC client whose callback is:

```text
https://tailcat.example.com/api/v1/auth/callback
```

Then configure and run:

```sh
export TAILCAT_WEBUI_ADDR=:8080
export TAILCAT_WEBUI_BASE_URL=https://tailcat.example.com
export TAILCAT_WEBUI_PUBLISH_BASE_URL=https://publish.tailcat.example.com
export TAILCAT_WEBUI_DATA_DIR=./data
export TAILCAT_WEBUI_MASTER_KEY="$(openssl rand -base64 32)"
export TAILCAT_WEBUI_OIDC_ISSUER=https://id.example.com
export TAILCAT_WEBUI_OIDC_CLIENT_ID=tailcat-webui
export TAILCAT_WEBUI_OIDC_CLIENT_SECRET=replace-me
./bin/tailcat-webui
```

`TAILCAT_WEBUI_MASTER_KEY` must remain stable. It encrypts remote connection
tokens and saved Tailcat private identities; losing it makes those records
unrecoverable.

For a loopback-only evaluation without an identity provider:

```sh
TAILCAT_WEBUI_DEMO_MODE=true make dev
```

Demo mode refuses non-loopback base URLs and listen addresses. `make dev`
supplies a public, development-only master key; never reuse it outside demo.

## Docker

```sh
docker run --rm -p 8080:8080 \
  -v tailcat-data:/data \
  -e TAILCAT_WEBUI_BASE_URL=https://tailcat.example.com \
  -e TAILCAT_WEBUI_PUBLISH_BASE_URL=https://publish.tailcat.example.com \
  -e TAILCAT_WEBUI_MASTER_KEY="$TAILCAT_WEBUI_MASTER_KEY" \
  -e TAILCAT_WEBUI_OIDC_ISSUER=https://id.example.com \
  -e TAILCAT_WEBUI_OIDC_CLIENT_ID=tailcat-webui \
  -e TAILCAT_WEBUI_OIDC_CLIENT_SECRET="$OIDC_CLIENT_SECRET" \
  ghcr.io/ca-x/tailcat-webui:latest
```

Terminate TLS at a trusted reverse proxy and keep
`TAILCAT_WEBUI_BASE_URL=https://…`; this enables Secure session cookies and
HSTS. Configure wildcard DNS/TLS for `*.publish.tailcat.example.com`, route it
and the management hostname to the same listener, and preserve the original
`Host` header. Every published route receives its own immutable-ID subdomain;
this isolates public scripts and private route cookies from other tenants.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `TAILCAT_WEBUI_ADDR` | `127.0.0.1:8080` | HTTP listen address |
| `TAILCAT_WEBUI_BASE_URL` | `http://localhost:8080` | Browser-visible canonical URL |
| `TAILCAT_WEBUI_PUBLISH_BASE_URL` | required outside demo | Separate origin for published resources |
| `TAILCAT_WEBUI_DATA_DIR` | `./data` | SQLite and runtime data directory |
| `TAILCAT_WEBUI_MASTER_KEY` | required outside demo | Base64-encoded 32-byte key for tokens and saved identities |
| `TAILCAT_WEBUI_OIDC_ISSUER` | empty | OIDC discovery issuer |
| `TAILCAT_WEBUI_OIDC_CLIENT_ID` | empty | OIDC client ID |
| `TAILCAT_WEBUI_OIDC_CLIENT_SECRET` | empty | OIDC client secret |
| `TAILCAT_WEBUI_OIDC_SCOPES` | `openid,profile,email` | Requested scopes |
| `TAILCAT_WEBUI_ALLOWED_MAPPING_TARGETS` | loopback CIDRs | Host targets allowed for explicit port mappings |
| `TAILCAT_WEBUI_ALLOWED_EXIT_TARGETS` | empty | Destination CIDRs an exit-node may reach; domain rules are rejected |
| `TAILCAT_WEBUI_TRUSTED_PROXIES` | empty | Proxy CIDRs trusted for `X-Forwarded-For` rate-limit identity |
| `TAILCAT_WEBUI_ALLOWED_DERP_HOSTS` | empty | Extra HTTPS DERP map/relay hosts users may select |
| `TAILCAT_WEBUI_TRANSFER_MAX_FILE_BYTES` | `512MiB` | Per-file staging limit; may only tighten the compiled ceiling |
| `TAILCAT_WEBUI_TRANSFER_MAX_SHARE_BYTES` | `1GiB` | Total bytes in one outgoing share |
| `TAILCAT_WEBUI_TRANSFER_MAX_JOB_BYTES` | `1GiB` | Total bytes in one incoming job |
| `TAILCAT_WEBUI_TRANSFER_MAX_OWNER_BYTES` | `2GiB` | Total staged bytes for one owner |
| `TAILCAT_WEBUI_TRANSFER_MAX_FILES_PER_SHARE` | `1000` | File count in one share/job; range `1..1000` |
| `TAILCAT_WEBUI_TRANSFER_WORKERS` | `4` | Range workers; must be exactly `4` |
| `TAILCAT_WEBUI_TRANSFER_MAX_JOBS_PER_OWNER` | `2` | Concurrent receive jobs; range `1..2` |
| `TAILCAT_WEBUI_TRANSFER_EXPIRY` | `24h` | Share/job lifetime; range `1s..24h` |
| `TAILCAT_WEBUI_TRANSFER_RETENTION` | `24h` | Compatibility lifetime name; must equal expiry |
| `TAILCAT_WEBUI_TRANSFER_UPLOAD_TIMEOUT` | `30m` | Browser upload read deadline; range `1s..1h` |
| `TAILCAT_WEBUI_DEMO_MODE` | `false` | Loopback-only development login |
| `TAILCAT_WEBUI_DEMO_UNSAFE_SSH` | `false` | Enable Tailcat's in-process shell only in loopback demo mode |

Mapping-target values are comma-separated. Each rule is `CIDR`, `CIDR@port`,
`CIDR@start-end`, `domain@port`, or `domain@start-end`. A bare CIDR allows every
port for compatibility; domains require an exact or ranged port clause. Exit
targets accept only the three CIDR forms because Tailcat exit forwarding
supplies numeric destinations. A domain in
`TAILCAT_WEBUI_ALLOWED_EXIT_TARGETS` is rejected at startup. The `@` separator
keeps IPv6 CIDRs unambiguous. Mapping and exit deployment rules set the maximum
authority. Owner-scoped exit rules can only narrow that maximum, and an empty
exit rule set denies all exit traffic.

SQLite uses foreign keys, WAL, `synchronous=NORMAL`, a five-second busy
timeout, mmap, and a bounded connection pool. Shared-cache mode is deliberately
not used because it serializes WAL readers.

## Development

```sh
make generate    # regenerate Ent code
make lint        # Go vet + frontend ESLint
make test        # Go race tests + Vitest
make build       # build web assets and embedded pure-Go binary
make verify      # core build, test, generated-asset, and secret-pattern gate
```

The full release gate also runs actionlint, dependency and vulnerability
audits, five cross-builds, archive inspection, and a local Docker build when a
container engine is available.

GitHub releases build the amd64 and arm64 container images concurrently on
native `ubuntu-24.04` and `ubuntu-24.04-arm` runners, then merge their immutable
digests into one multi-architecture manifest without QEMU emulation.

The Go API is split into focused auth, Tailcat runtime, publishing, and HTTP
packages. Direct upstream Tailcat imports are isolated under `internal/tailnet`
because Tailcat does not promise API or wire-format stability.

## Security notes

- Public routes are explicit; new routes default to owner-only.
- Published resources use a separate origin so untrusted remote HTML cannot
  execute with the control-plane origin or register its service worker.
- Every durable lookup includes the authenticated owner ID.
- Diagnostics and transfer shares, files, jobs, and downloads are owner scoped.
- Management cookies are stripped before requests reach published resources.
- Saved node private keys never appear in API responses or logs.
- Transfer capability hashes use constant-time comparison; resumable remote
  codes are AES-256-GCM encrypted with owner/job associated data.
- Uploads require an exact length and configured body/deadline limits;
  completed downloads accept one bounded byte range and use private no-store
  responses.
- Local mappings resolve and pin DNS before dialing to prevent rebinding.
- Exit-node destinations are checked against deployment CIDRs.
- The upstream public Tailcat DERP service is rate-limited and has no SLA;
  production operators should configure their own relay fleet.

See [docs/security.md](docs/security.md) for the threat model.

## License and upstream

Tailcat WebUI is licensed under AGPL-3.0-only. Tailcat and its logo are
BSD-3-Clause software © Tailscale Inc. and contributors. See [NOTICE.md](NOTICE.md).
This project is independent and is not endorsed by Tailscale Inc.
