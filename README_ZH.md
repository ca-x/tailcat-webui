<p align="center">
  <img src="docs/assets/tailcat.png" alt="Tailcat" width="96">
</p>

<h1 align="center">Tailcat WebUI</h1>

<p align="center">支持多用户、多实例和移动端的 Tailcat 控制台。</p>

<p align="center"><a href="README.md">English</a> · <a href="docs/openapi.yaml">OpenAPI</a></p>

Tailcat WebUI 将 [Tailcat](https://github.com/tailscale/tailcat) 封装成长期运行、
使用 OIDC 登录的 Web 应用。每位用户都可以创建多个相互独立的 Tailcat 服务端和
客户端。每项持久化资源都归属于对应的 OIDC 用户。远端 HTTP、SSE 和 WebSocket
资源通过独立的公开来源和稳定子路径发布。

## 实际项目截图

### 桌面端浅色主题 · 服务端管理

![Tailcat 服务端管理](docs/screenshots/server-desktop-light.png)

### 移动端深色主题 · 中文网络概览

<p align="center">
  <img src="docs/screenshots/mobile-dashboard-dark-zh.png" alt="Tailcat 中文移动端深色概览" width="390">
</p>

以上截图均由实际运行的内嵌应用生成，不是设计稿。

### 桌面端浅色主题 · 网络诊断

<p align="center">
  <img src="docs/screenshots/diagnostics-desktop-light.png" alt="桌面端浅色主题中的 Tailcat 网络诊断成功记录" width="960">
</p>

原始 PNG 尺寸：1440 × 900。

### 移动端深色主题 · 中文安全文件传输

<p align="center">
  <img src="docs/screenshots/transfers-mobile-dark-zh.png" alt="移动端深色主题中的 Tailcat 已完成文件传输" width="390">
</p>

原始 PNG 尺寸：390 × 844。截图只包含演示名称和操作摘要。连接令牌与一次性共享码
均在截图前关闭或清空。

## 功能

- 同一进程、同一用户均可运行多个独立服务端和多个客户端。
- 临时身份、加密保存的稳定身份、客户端公钥白名单。
- TCP 端口转发、免认证 SSH 服务端、受目标网段策略限制的出口节点。
- Ping、直连/DERP/Peer Relay 路径判断、令牌解析和完整令牌解析。
- DNS `tailcat=tc…` TXT 记录、自定义 DERP 主机或 DERP Map。
- 认证 WebSocket TCP 隧道，对应 netcat 管道和 SOCKS 任意 TCP 访问场景。
- `/r/{slug}/*` 发布 HTTP、SSE 与 WebSocket，可选择仅本人或公开访问。
- OIDC 授权码流程、PKCE、nonce、state、服务端会话和多用户数据隔离。
- React 19 + Ant Design 6；所有表单、抽屉、弹窗、确认和反馈均使用框架组件。
- 简体中文/英文；浅色、深色、跟随系统三种外观。
- Go 1.27、Ent、默认纯 Go SQLite，无 CGO 单文件部署。
- 端口感知的目标规则，以及每个服务端独立收窄的出口规则。
- 通过固定 TCP `41640` 提供有界网络诊断与用户级历史记录。
- 通过固定 TCP `41641` 提供浏览器暂存、可恢复并由 BLAKE3 校验的文件传输。

## 与原版 Tailcat 兼容性

标准 Tailcat 连接令牌可以双向兼容。原版客户端可使用
`tailcat <token> <port>` 连接 WebUI 管理的服务端。原版的 `tailcat ping`、
`tailcat socks`、`tailcat ssh` 和服务端白名单公钥检查仍按上游 Tailcat 的行为
工作。WebUI 管理的客户端也可以使用原版 Tailcat 服务端生成的令牌。

TCP `41640` 上的 WebUI 诊断历史协议和 TCP `41641` 上的分块传输协议要求两端都
支持 WebUI 协议。原版 CLI 有自己的 Ping 命令，但不能参与这两个应用层协议。
本项目的出口规则属于 WebUI 策略控制，不承诺兼容原版 CLI 的出口路由方式。

## 诊断与文件传输

网络诊断只能访问用户选定的 Tailcat 客户端，服务端口固定为 TCP `41640`，API
不能指定任意测速主机。每次运行最多 5 秒，每个方向最多传输 32 MiB。每位用户
最多同时运行 2 项诊断，同一客户端最多运行 1 项。数据库只保留最近 100 条、
最长 30 天的运行摘要，不保存对端 IP 和逐次进度采样。

文件传输只接受浏览器选取的文件。发送方先在数据目录内暂存文件，再完成不可变
共享，并获得只显示一次的共享码。轮换共享码后，旧码立即失效。接收任务仅为重启
和恢复而保存远端共享码，并使用现有主密钥加密。TCP `41641` 只提供固定的清单和
范围读取操作，不提供主机文件系统浏览。上传期间，抽屉和后台进度卡都提供取消操作；
关闭抽屉或离开页面也会中止当前请求。已经暂存成功的文件不会删除，可直接继续重试。

内置上限为单文件 512 MiB、每个发送共享或接收任务 1 GiB、每位用户暂存总量
2 GiB、每个共享 1,000 个文件，以及每位用户最多保留 4,096 个文件。每位用户
还最多保留 128 个发送共享和 128 个接收任务，创建中的对象也计入限制。每项任务
固定使用 4 个范围读取工作线程，每位用户最多同时运行 2 项任务。共享和任务默认
24 小时后过期，运维方可以在 1 秒到 24 小时的范围内收紧生命周期。进程内调度器
会持续执行到期清理，清理失败会重试，无需等待 API 访问或重启。BLAKE3 清单按
8 MiB 分块，文件完成前还会执行一次整文件哈希校验。

传输存储按用户、共享和任务建立有根目录，并使用随机磁盘文件名。虚拟路径必须是
规范化相对路径，不能作为主机路径使用。存储层拒绝绝对路径、点路径段、控制字符、
符号链接、Windows 重解析点逃逸、不安全硬链接和根目录替换。暂存文件保持私有，
写入后执行同步和原子发布，删除也始终限定在所属用户范围内。SQLite 和应用二进制
继续使用纯 Go 实现，构建时设置 `CGO_ENABLED=0`。删除 Tailcat 服务端或客户端时，
系统会先取消并移除关联的共享、接收任务和暂存数据；如果清理失败，则保留父记录以便重试。

## 快速开始

需要 Go 1.27.0、Node.js 26 和 pnpm 11.3。

```sh
git clone https://github.com/ca-x/tailcat-webui.git
cd tailcat-webui
cd web && pnpm install --frozen-lockfile --ignore-scripts && cd ..
make build
```

在 OIDC 服务中配置回调地址：

```text
https://tailcat.example.com/api/v1/auth/callback
```

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

`TAILCAT_WEBUI_MASTER_KEY` 必须长期保持不变；它用于加密远端连接令牌和已保存的
Tailcat 私钥，丢失后无法恢复这些记录。

仅在本机试用时可启用演示模式：

```sh
TAILCAT_WEBUI_DEMO_MODE=true make dev
```

演示模式会拒绝非回环地址，不能用于线上部署。

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

建议由可信反向代理终止 TLS，并保持 `TAILCAT_WEBUI_BASE_URL` 为 HTTPS，应用
会据此启用 Secure Cookie 和 HSTS。发布域名需要配置通配 DNS/TLS（例如
`*.publish.tailcat.example.com`）；每条路由使用独立子域名，隔离不同租户的脚本和 Cookie。

## 配置

| 变量 | 默认值 | 用途 |
| --- | --- | --- |
| `TAILCAT_WEBUI_ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `TAILCAT_WEBUI_BASE_URL` | `http://localhost:8080` | 浏览器访问的管理来源 |
| `TAILCAT_WEBUI_PUBLISH_BASE_URL` | 演示模式外必填 | 与管理来源隔离的发布来源 |
| `TAILCAT_WEBUI_DATA_DIR` | `./data` | SQLite 与运行数据目录 |
| `TAILCAT_WEBUI_MASTER_KEY` | 演示模式外必填 | 以 Base64 编码的 32 字节令牌和身份加密密钥 |
| `TAILCAT_WEBUI_OIDC_ISSUER` | 空 | OIDC Discovery 发行方 |
| `TAILCAT_WEBUI_OIDC_CLIENT_ID` | 空 | OIDC 客户端 ID |
| `TAILCAT_WEBUI_OIDC_CLIENT_SECRET` | 空 | OIDC 客户端密钥 |
| `TAILCAT_WEBUI_OIDC_SCOPES` | `openid,profile,email` | 请求的 OIDC Scope |
| `TAILCAT_WEBUI_ALLOWED_MAPPING_TARGETS` | 回环 CIDR | 显式端口映射允许访问的主机和端口 |
| `TAILCAT_WEBUI_ALLOWED_EXIT_TARGETS` | 空 | 出口节点允许访问的 CIDR 上限，不接受域名规则 |
| `TAILCAT_WEBUI_TRUSTED_PROXIES` | 空 | 可为限流身份提供 `X-Forwarded-For` 的代理 CIDR |
| `TAILCAT_WEBUI_ALLOWED_DERP_HOSTS` | 空 | 用户可选择的额外 HTTPS DERP Map 或中继主机 |
| `TAILCAT_WEBUI_TRANSFER_MAX_FILE_BYTES` | `512MiB` | 单文件暂存上限，只能收紧内置上限 |
| `TAILCAT_WEBUI_TRANSFER_MAX_SHARE_BYTES` | `1GiB` | 单个发送共享的总字节数 |
| `TAILCAT_WEBUI_TRANSFER_MAX_JOB_BYTES` | `1GiB` | 单个接收任务的总字节数 |
| `TAILCAT_WEBUI_TRANSFER_MAX_OWNER_BYTES` | `2GiB` | 每位用户的暂存总量 |
| `TAILCAT_WEBUI_TRANSFER_MAX_FILES_PER_SHARE` | `1000` | 单个共享或任务的文件数，范围为 `1..1000` |
| `TAILCAT_WEBUI_TRANSFER_WORKERS` | `4` | 范围读取工作线程，必须等于 `4` |
| `TAILCAT_WEBUI_TRANSFER_MAX_JOBS_PER_OWNER` | `2` | 每位用户的并发接收任务数，范围为 `1..2` |
| `TAILCAT_WEBUI_TRANSFER_EXPIRY` | `24h` | 共享和任务生命周期，范围为 `1s..24h` |
| `TAILCAT_WEBUI_TRANSFER_RETENTION` | `24h` | 兼容配置名，必须与生命周期相同 |
| `TAILCAT_WEBUI_TRANSFER_UPLOAD_TIMEOUT` | `30m` | 浏览器上传读取超时，范围为 `1s..1h` |
| `TAILCAT_WEBUI_DEMO_MODE` | `false` | 仅限回环地址的演示登录 |
| `TAILCAT_WEBUI_DEMO_UNSAFE_SSH` | `false` | 仅在回环演示模式启用 Tailcat 进程内 Shell |

映射目标规则使用逗号分隔。每条规则可写成 `CIDR`、`CIDR@port`、
`CIDR@start-end`、`domain@port` 或 `domain@start-end`。兼容旧配置的裸 CIDR
表示允许全部端口，域名规则必须指定单一端口或端口范围。出口目标只接受前三种
CIDR 形式，因为 Tailcat 出口转发只提供数值地址；如果
`TAILCAT_WEBUI_ALLOWED_EXIT_TARGETS` 包含域名，应用会在启动时拒绝配置。端口子句
使用 `@` 分隔，因此 IPv6 CIDR 不会产生歧义。映射和出口规则定义部署级上限。
用户级出口规则只能继续收紧该上限，空规则集会拒绝全部出口流量。

## 开发验证

```sh
make generate
make lint
make test
make build
make verify
```

`make verify` 是本地核心门禁，覆盖构建、测试、生成文件一致性与高置信度密钥模式
扫描。完整发布门禁还会分别运行 actionlint、依赖与漏洞审计、五目标交叉编译、
归档检查，并在主机具备容器引擎时执行本地 Docker 构建。

GitHub Release 会在原生 `ubuntu-24.04` 与 `ubuntu-24.04-arm` runner 上并行构建
amd64/arm64 容器镜像，再按不可变 digest 合并为一个多架构 manifest，不再使用
QEMU 模拟构建。

SQLite 默认启用外键、WAL、`synchronous=NORMAL`、5 秒 busy timeout、mmap 和
有界连接池。出于 WAL 并发读性能考虑，不启用 shared-cache。

公开路由默认只允许所属用户访问，并通过独立来源隔离不可信远端脚本。诊断、共享、
文件、任务和下载查询均带用户范围。共享码在数据库中只保留 SHA-256 哈希并使用
常量时间比较；用于恢复的远端共享码使用 AES-256-GCM 和用户、任务关联数据加密。
上传必须提供准确长度，并受请求体和超时限制。已完成下载只接受单一字节范围，返回
私有且禁止缓存的响应。详细设计见 [docs/security.md](docs/security.md)。

Tailcat 本身不承诺 Go API、CLI 或线协议稳定性，因此项目固定上游 revision，并把
直接依赖隔离在 `internal/tailnet`。

## 许可证与上游说明

Tailcat WebUI 使用 AGPL-3.0-only。Tailcat 及其猫图标由 Tailscale Inc. 与贡献者
以 BSD-3-Clause 发布，详见 [NOTICE.md](NOTICE.md)。本项目是独立社区项目，未获
Tailscale Inc. 背书。
