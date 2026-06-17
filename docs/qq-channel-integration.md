# QQ 渠道接入技术方案

> 依据 `agents/architecture-technical-agent.md` 规范输出：架构设计 + 数据库设计 + 接口设计。不含业务逻辑实现代码，含可执行 DDL/接入点说明。

---

## 1. 概述

### 1.1 背景

GoClaw 网关已支持 Telegram、Discord、Slack、WhatsApp、Feishu/Lark、Zalo、Facebook、Pancake、Bitrix24 等渠道，统一通过 `internal/channels` 抽象层接入 Agent 运行时。目前**未接入 QQ**。

QQ 在国内 IM 市场覆盖率极高，但接入存在生态特殊性：

- 腾讯官方的 QQ 机器人 API（QQ 频道机器人 / QQ 群机器人）需企业资质审核、能力受限，且无法覆盖个人会话场景。
- 主流社区方案是 **OneBot 11 协议**（NapCat / Lagrange.Core / LLOneBot / go-cqhttp 等协议实现端），通过反向 WebSocket 上报事件、通过 JSON API 下发动作。这是国产 IM 场景下事实标准。

### 1.2 目标

- 将 QQ 作为**一种新渠道类型（`qq`）**接入 GoClaw，复用现有渠道抽象层，不引入新子系统。
- 支持 **私聊（private）** 与 **群聊（group）** 双场景。
- 支持文本、图片、语音、文件等富媒体收发。
- 支持现有策略体系：配对（pairing）、白名单（allowlist）、@提及门控、多租户、加密凭据、限流。
- 对接方式与现有渠道一致：DB 实例化（`channel_instances`） + 工厂注册 + 健康监控 + 热重载。

### 1.3 范围

| 范围内 | 范围外 |
|--------|--------|
| OneBot 11 反向/正向 WebSocket 客户端 | 协议实现端（NapCat/Lagrange）本身的部署 |
| 事件 → InboundMessage 映射 | QQ 官方开放平台机器人 API（企业资质通道） |
| OutboundMessage → OneBot send_msg | 频道（guild/channel）场景（一期仅 private/group） |
| 媒体收发（base64/URL） | TTS 语音合成（复用现有 `audio.Manager`，非本方案新增） |
| 策略、配对、限流、加密、健康、热重载 | 协议端账号风控/养号 |

### 1.4 现状关键事实（已对代码核实）

- 渠道统一接口：`internal/channels/channel.go` 的 `Channel` interface（`Name/Type/Start/Stop/Send/IsRunning/IsAllowed`），及一组可选扩展接口（`StreamingChannel`、`WebhookChannel`、`ReactionChannel`、`ChannelDestroyer`、`BlockReplyChannel`、`GroupMemberProvider`、`PendingCompactable`）。实现嵌入 `BaseChannel`。
- 工厂注册：`internal/channels/instance_loader.go` 的 `ChannelFactory` 类型；在 `cmd/gateway.go:530-551` 通过 `instanceLoader.RegisterFactory(...)` 注册各渠道。
- DB 表：`channel_instances`（`migrations/000001_init_schema.up.sql:505`），字段含 `name / display_name / channel_type / agent_id(FK agents) / credentials(BYTEA 加密) / config(JSONB) / enabled`。SQLite 镜像在 `internal/store/sqlitestore/`。
- 类型校验（创建实例时白名单）：两处 `isValidChannelType` —— WS 方法 `internal/gateway/methods/channel_instances.go:330`、HTTP 处理器 `internal/http/channel_instances.go:703`。
- 媒体能力登记：`internal/channels/capabilities.go` 的 `mediaCapableTypes`。
- 版本门控：`internal/edition/edition.go`，`Lite.AllowsChannels()` 返回 `false`（Lite 桌面版禁用渠道），`MaxChannels` 仅列 telegram/discord。QQ 属 Standard 版特性。
- 消息总线契约：`internal/bus/types.go` 的 `InboundMessage` / `OutboundMessage`。
- 依赖现状（`go.mod`）：`gorilla/websocket`、`mymmrac/telego`、`bwmarrin/discordgo`、`slack-go/slack`、`go.mau.fi/whatsmeow`。OneBot 11 协议**无需新增重度依赖**，基于 `gorilla/websocket` 自研轻量客户端即可。

---

## 2. 系统架构

### 2.1 架构图

```
 ┌────────────────────────────────────────────────────────────────────┐
 │                         OneBot 11 协议层                            │
 │                                                                    │
 │   ┌─────────────────┐        反向 WebSocket (Reverse WS)           │
 │   │  协议实现端      │ ───────► /v1/onebot/qq/{instance} ─────────┐ │
 │   │ (NapCat /        │           (实现端主动连入, 推荐)            │ │
 │   │  Lagrange /      │                                         │ │
 │   │  LLOneBot)       │ ◄─────── 正向 WebSocket (Forward WS) ────┐ │ │
 │   └─────────────────┘          (gateway 主动连 ws://host:port)   │ │ │
 │            │                                                   │ │ │
 └────────────┼───────────────────────────────────────────────────┼─┼─┘
              │ 事件(JSON): message/notice/meta_event              │ │ │
              ▼  动作(JSON): send_msg/get_group_member_list/...    │ │ │
 ┌─────────────────────────────────────────────────────────────────┘ │ │
 │  internal/channels/qq/        (本方案新增)                        │ │ │
 │  ┌──────────────┐   ┌─────────────────┐   ┌──────────────────┐   │ │ │
 │  │  channel.go  │──►│  onebot/client  │◄──┤  WebhookChannel  │◄──┘ │
 │  │  (生命周期)   │   │  (WS + 收发)     │   │  (反向WS挂载)    │     │
 │  └──────┬───────┘   └────────┬────────┘   └──────────────────┘     │
 │         │ embeds             │ ChannelFactory                        │
 │         ▼                    ▼                                       │
 │  ┌──────────────┐   ┌─────────────────┐   ┌──────────────────┐     │
 │  │ BaseChannel  │   │   handle.go     │   │    send.go       │     │
 │  │ (策略/配对/   │   │  事件→Inbound   │   │ Outbound→send_msg│     │
 │  │  健康/租户)   │   │   Message       │   │  + media.go      │     │
 │  └──────────────┘   └─────────────────┘   └──────────────────┘     │
 └─────────────┬─────────────────────────────────────────────────────┘
               │ bus.PublishInbound / bus.OutboundMessage
               ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │        MessageBus  ──►  Agent 运行时 (pipeline / sessions /        │
 │                          scheduler / tools)                       │
 └──────────────────────────────────────────────────────────────────┘
```

### 2.2 模块划分

| 模块 | 路径 | 职责 |
|------|------|------|
| QQ Channel 主体 | `internal/channels/qq/channel.go` | 实现 `channels.Channel`；管理 WS 生命周期、运行状态、策略装配 |
| OneBot 客户端 | `internal/channels/qq/onebot/client.go` | WebSocket 连接管理、事件分发、动作调用（`echo` 请求-响应匹配） |
| OneBot 类型 | `internal/channels/qq/onebot/types.go` | 事件 / 消息段 / API 请求响应的 Go 类型映射 |
| 入站处理 | `internal/channels/qq/handle.go` | OneBot 事件 → `bus.InboundMessage`，DM/Group 策略校验，@提及解析 |
| 出站处理 | `internal/channels/qq/send.go` | `bus.OutboundMessage` → OneBot `send_msg`，分段、CQ/数组格式、占位消息编辑 |
| 媒体 | `internal/channels/qq/media.go` | 图片/语音/文件收发（base64 / file-URL），大小上限 |
| 工厂 | `internal/channels/qq/factory.go` | `channels.ChannelFactory` 实现：`credentials + config` → `config.QQConfig` → `New` |

### 2.3 模块职责

- **`channel.go`**：嵌入 `channels.BaseChannel`（继承策略/配对/健康/租户/ContactCollector）。`Start()` 建立连接（正向 WS 连接实现端；反向 WS 则注册 WebhookHandler 并等待实现端连入）。`Stop()` 关闭所有 WS 连接。`Send()` 处理占位消息编辑、媒体、NO_REPLY、分段。实现 `BlockReplyEnabled()`（继承 gateway 默认）。
- **`onebot/client.go`**：单实例对单 `self_id`。维护 WS 读循环，按 `post_type` 分发事件；动作调用通过自增 `echo` 字段 + `map[string]chan ApiResponse` 实现请求-响应配对（带超时）。处理 `meta_event`（heartbeat/lifecycle）用于存活探活 → 健康状态。
- **`handle.go`**：`message.private` → `peer_kind=direct`；`message.group` → `peer_kind=group`，解析 `at` 段判断是否 @机器人（配合 `require_mention`），剥离 @机器人文本。`sender.user_id` 作为 `SenderID`，群以 `group_id` 作为 `ChatID`，私聊以 `user_id` 作为 `ChatID`。
- **`send.go`**：占位更新（撤回旧消息 + 重发，QQ 无原生"编辑"，用"撤回+重发"或"先发占位文本再撤回重发"近似流式）。媒体走 `send_msg` 的 image/record/file 段。长度超限按 `channels.ChunkMarkdown` 分段（QQ 单条约 4500 字符上限）。

### 2.4 接入链路（注册点清单，全部 `file:line` 可定位）

| # | 文件 | 改动 | 性质 |
|---|------|------|------|
| 1 | `internal/channels/channel.go:73` | 新增 `TypeQQ = "qq"` 常量 | 必改 |
| 2 | `internal/channels/capabilities.go:22` | `mediaCapableTypes` 增 `TypeQQ: true` | 必改（支持媒体） |
| 3 | `internal/channels/qq/*` | 新建渠道包（7 个文件） | 新增 |
| 4 | `cmd/gateway.go` (~540 行附近) | `instanceLoader.RegisterFactory(channels.TypeQQ, qq.Factory)` | 必改 |
| 5 | `internal/gateway/methods/channel_instances.go:330` | `isValidChannelType` 增 `"qq"` | 必改 |
| 6 | `internal/http/channel_instances.go:703` | `isValidChannelType` 增 `"qq"` | 必改 |
| 7 | `internal/config/config_channels.go` | ~~`ChannelsConfig` 增 `QQ QQConfig`~~ **不需要**：QQ 是纯 DB 实例渠道（同 Bitrix24/Pancake/Facebook，均无 `ChannelsConfig` 字段）。`config.QQConfig` 仅作为 `New()` 参数类型存在（定义见 4.5），但**不挂到 `ChannelsConfig`** | 无需改 |
| 8 | `internal/edition/edition.go` | QQ 默认 Standard 版可用，Lite 禁用（`AllowsChannels()` 已为 false） | 无需改 |
| 9 | `internal/i18n/keys.go` + 3 catalog | 渠道显示名 / 错误文案 i18n key | 必改 |
| 10a | `ui/web/src/constants/channels.ts` | `CHANNEL_TYPES` 增 `{ value: "qq", label: "QQ" }` | 必改 |
| 10b | `ui/web/src/pages/contacts/contacts-page.tsx:25` | 此文件有**独立硬编码**的 `CHANNEL_TYPES` 与 `PERM_CHANNELS`（与 `constants/channels.ts` 不同步），需同步增 `"qq"` | 必改 |
| 10c | `ui/web` 渠道表单/向导/图标 | `channel-wizard-registry`、`channel-schemas` 增 qq 凭据/配置 schema + 图标 | 必改 |
| **+** | `internal/channels/qq/channel.go` | 实现 `SetPendingHistoryTenantID(uuid.UUID)`（持久化历史必需，见 5.1） | 必改 |

> 与 Bitrix24 的 `WebhookChannel`/`ChannelDestroyer` 不同，QQ 协议端账号是用户自托管，`Stop()` 已能完成所有清理，**无需实现 `ChannelDestroyer`**（参考 channel.go:156-159 的设计说明）。

---

## 3. 技术选型

| 层级 | 技术 / 方案 | 版本 / 规格 | 说明 |
|------|------------|-------------|------|
| 协议 | OneBot 11 | v11 | 事实标准；事件上报 + API 动作；支持反向/正向 WS、HTTP、Webhook 多种通信，本方案用 WS |
| 传输 | gorilla/websocket | 已在 `go.mod` | 与现有渠道一致，无需新增依赖 |
| 连接模式 | 反向 WS（默认）+ 正向 WS（可选） | — | 反向 WS 内网穿透友好，复用 `WebhookChannel` 接口挂载到主 mux，避免额外端口（与 Feishu webhook 同思路） |
| 消息格式 | OneBot 数组消息段（`message[].type/data`） | — | 比 CQ 码字符串更安全，避免注入；媒体用 `base64://` 或 `http(s)://` URL |
| 编码 | JSON | encoding/json | OneBot 原生 JSON |
| 加密 | AES-256-GCM | 现有 `internal/crypto` | 凭据（`access_token`）落库加密 |
| ORM | 无 | database/sql + pgx/v5 + sqlx | 与全项目一致，不引入 ORM |
| 库选型 | 自研轻量 OneBot WS 客户端 | — | 社区 Go OneBot 库维护质量参差且耦合度高；协议本身简单（WS+JSON），自研可控、与现有 channel 模式贴合 |

**协议实现端推荐**（用户侧部署，非本仓库交付）：NapCat（NTQQ，活跃）、Lagrange.Core（C#，跨平台）、LLOneBot。方案与之通过 OneBot 11 标准解耦，实现端可替换。

---

## 4. 数据库设计

### 4.1 ER / 表关系

```
        agents (FK, 应用层维护)
           │ agent_id
           ▼
   channel_instances
   ┌─────────────────────────────┐
   │ id (uuid v7) PK             │
   │ tenant_id                   │──── multi-tenant
   │ name (uniq)                 │
   │ display_name                │
   │ channel_type = 'qq'         │
   │ agent_id                    │
   │ credentials (BYTEA, 加密)    │
   │ config (JSONB)              │
   │ enabled                     │
   └─────────────────────────────┘
```

### 4.2 关键结论：**复用 `channel_instances`，无需新建表**

QQ 是 `channel_instances.channel_type` 的一个新枚举值，**完全复用现有表结构与索引**。现有表已满足所有需求：

| 既有列 | QQ 用途 |
|--------|---------|
| `channel_type` | 存 `"qq"` |
| `credentials` (BYTEA, AES-256-GCM) | 存 `access_token`（OneBot 鉴权令牌） |
| `config` (JSONB) | 存非机密策略与连接配置（见 4.4） |
| `agent_id` (FK agents) | 路由到目标 Agent |
| `tenant_id` | 多租户隔离 |
| `enabled` | 启停 |

### 4.3 DDL（**仅说明，不执行**——表已存在）

> 依据本项目「双 DB 迁移规范」（CLAUDE.md），QQ 接入**无需任何迁移脚本**：`channel_type` 是 `VARCHAR(50)` 而非 DB 级枚举，新增值不需要改 schema。`RequiredSchemaVersion`（PG）/ `SchemaVersion`（SQLite）**均不需要 bump**。

供参考的现有定义（`migrations/000001_init_schema.up.sql:505`）：

```sql
CREATE TABLE channel_instances (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    name            VARCHAR(100) NOT NULL UNIQUE,
    display_name    VARCHAR(255) DEFAULT '',
    channel_type    VARCHAR(50) NOT NULL,           -- QQ 复用，存 'qq'
    agent_id        UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    credentials     BYTEA,                          -- AES-256-GCM 加密
    config          JSONB DEFAULT '{}',
    enabled         BOOLEAN DEFAULT true,
    created_by      VARCHAR(255) DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_channel_instances_type  ON channel_instances(channel_type);  -- 已覆盖按类型查询
CREATE INDEX idx_channel_instances_agent ON channel_instances(agent_id);      -- 已覆盖按 Agent 查询
```

SQLite 侧（`internal/store/sqlitestore/schema.sql`）同结构，亦无需改动。

> 索引遵循本项目规范：`channel_type`、`agent_id` 已建必要普通索引；无需为 QQ 额外建索引。
> 注：`channel_instances` 是系统启动期/管理面高频表，沿用既有表即继承既有索引与写入路径，不引入新索引开销。

### 4.4 `config` / `credentials` JSON 契约（本方案的核心数据约定）

**`credentials`**（加密 JSONB，经 `crypto` 解密后结构）：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `access_token` | string | 推荐 | OneBot 鉴权令牌（`Authorization: Bearer` / `?access_token=`）；留空则不鉴权 |
| `self_id` | string | 推荐 | 协议端登录 QQ 号，用于过滤自身消息、@提及检测 |

**`config`**（非机密 JSONB）：

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `direction` | string | `"reverse"` | `"reverse"`（实现端连入）/ `"forward"`（gateway 连出） |
| `endpoint` | string | — | forward 模式的 `ws://host:port`；reverse 模式留空 |
| `reverse_path` | string | `/v1/onebot/qq` | reverse 模式 WS 挂载**前缀**；实际挂载路径 = `{reverse_path}/{instance_name}`（如 `/v1/onebot/qq/qq-main`），与 `channel_instances.name` 一致，OneBot 实现端 WS 地址须匹配此路径 |
| `dm_policy` | string | `"pairing"` | 同其它渠道：`pairing`/`open`/`allowlist`/`disabled` |
| `group_policy` | string | `"pairing"` | 同上 |
| `allow_from` | []string | `[]` | 白名单 QQ 号 / 群号 |
| `require_mention` | bool | `true` | 群内是否需 @机器人 |
| `history_limit` | int | `50` | 群待处理消息上限（接 `PendingHistory`） |
| `block_reply` | *bool | `nil` | 覆盖 gateway block_reply（nil=继承） |
| `media_max_bytes` | int64 | `20971520` | 媒体下载上限（默认 20MB） |
| `chunk_limit` | int | `4500` | 单条消息分片长度 |

#### 4.5 对应 Go 结构体（编译必需，方案原缺）

`config`（非机密 JSONB）由工厂解析。参照 Discord / Bitrix24 模式：工厂内定义本地 `qqInstanceConfig`（从 DB JSONB 反序列化），再构造 `config.QQConfig` 作为 `New()` 参数类型。`config.QQConfig` **仅作 `New()` 参数类型，不挂到 `ChannelsConfig`**（QQ 是纯 DB 实例渠道，同 Bitrix24/Pancake/Facebook，`config_channels.go` 的 `ChannelsConfig` 里没有它们）：

```go
// QQConfig configures a QQ (OneBot 11) channel instance. Non-secret only;
// access_token / self_id live in credentials (AES-256-GCM encrypted).
type QQConfig struct {
    Direction      string   `json:"direction,omitempty"`       // "reverse" (default) | "forward"
    Endpoint       string   `json:"endpoint,omitempty"`        // forward mode ws://host:port
    ReversePath    string   `json:"reverse_path,omitempty"`    // reverse WS mount prefix, default "/v1/onebot/qq"
    DMPolicy       string   `json:"dm_policy,omitempty"`       // "pairing" (default) | "open" | "allowlist" | "disabled"
    GroupPolicy    string   `json:"group_policy,omitempty"`    // "pairing" (default) | "open" | "allowlist" | "disabled"
    AllowFrom      []string `json:"allow_from,omitempty"`      // QQ号/群号白名单
    RequireMention *bool    `json:"require_mention,omitempty"` // 群@提及门控 (默认 true)
    HistoryLimit   int      `json:"history_limit,omitempty"`   // 群待处理消息上限 (默认 50)
    BlockReply     *bool    `json:"block_reply,omitempty"`     // 覆盖 gateway block_reply (nil=继承)
    MediaMaxBytes  int64    `json:"media_max_bytes,omitempty"` // 媒体下载上限 (默认 20MB)
    ChunkLimit     int      `json:"chunk_limit,omitempty"`     // 单条消息分片长度 (默认 4500)
}
```

工厂（`internal/channels/qq/factory.go`）配套解析结构，字段镜像 `QQConfig`（参照 `discord/factory.go:20` 的 `discordInstanceConfig`、`bitrix24/factory.go:30` 的 `bitrixInstanceConfig`）：

```go
// qqInstanceConfig mirrors QQConfig; parsed from channel_instances.config JSONB.
type qqInstanceConfig struct {
    Direction      string   `json:"direction,omitempty"`
    Endpoint       string   `json:"endpoint,omitempty"`
    ReversePath    string   `json:"reverse_path,omitempty"`
    DMPolicy       string   `json:"dm_policy,omitempty"`
    GroupPolicy    string   `json:"group_policy,omitempty"`
    AllowFrom      []string `json:"allow_from,omitempty"`
    RequireMention *bool    `json:"require_mention,omitempty"`
    HistoryLimit   int      `json:"history_limit,omitempty"`
    BlockReply     *bool    `json:"block_reply,omitempty"`
    MediaMaxBytes  int64    `json:"media_max_bytes,omitempty"`
    ChunkLimit     int      `json:"chunk_limit,omitempty"`
}
```

> `GroupPolicy="pairing"` 运行时被 `BaseChannel.CheckGroupPolicy()` 支持（`channel.go:371`），但 `GroupPolicy` 常量块（`channel.go:66-70`）未定义 `GroupPolicyPairing` 常量——编码时勿遗漏该分支。

---

## 5. 接口设计

### 5.1 内部接口（Go，对工程实现约束）

**渠道接口实现清单**（`internal/channels/qq/channel.go` 的 `Channel` 必须满足）：

| 接口 | 实现 | 说明 |
|------|------|------|
| `channels.Channel` | ✅ 必须实现 | `Name/Type/Start/Stop/Send/IsRunning/IsAllowed` |
| `channels.WebhookChannel` | ✅ reverse 模式 | `WebhookHandler() (path, http.Handler)` —— 挂载到主 mux，接受实现端 WS 升级 |
| `channels.BlockReplyChannel` | ✅ | `BlockReplyEnabled() *bool` |
| `channels.GroupMemberProvider` | 可选 | `ListGroupMembers(chatID)` → `get_group_member_list` |
| `channels.ReactionChannel` | 可选 | QQ 无原生 reaction，可用表情回应近似（二期） |
| `channels.StreamingChannel` | 可选 | 借"撤回+重发"近似流式（二期） |
| `channels.PendingCompactable` | ✅ | `SetPendingCompaction(*CompactionConfig)` 群消息压缩 |
| `channels.ChannelDestroyer` | ❌ 不实现 | 协议端账号用户自托管，`Stop()` 足够 |
| 隐式（instance_loader 鸭子断言） | ✅ 必须实现 | `SetPendingHistoryTenantID(uuid.UUID)`：`instance_loader.go:281-283` 把实例 `tenant_id` 注入持久化历史。**漏实现 → DB 写入用 `uuid.Nil` → 跨租户串数据**（多租户隔离破坏） |

> **PendingHistory 持久化决策（必须明确，方案原缺）**：QQ 群聊需跨重启保留待处理消息历史，采用 `NewPersistentHistory()`（`internal/channels/history.go:90`，需 `store.PendingMessageStore`）。**不要**用 `NewPendingHistory()`（纯内存，重启即丢）。工厂签名参照 Feishu / ZaloPersonal 的 `FactoryWithPendingStore(pendingStore)` 模式注入 `pgStores.PendingMessages`；正因选了持久化，上一行的 `SetPendingHistoryTenantID` 才是**强制**项而非可选。

**工厂签名**（与 Discord/Telegram 一致）：

```
func Factory(name string, creds json.RawMessage, cfg json.RawMessage,
    msgBus *bus.MessageBus, pairingSvc store.PairingStore) (channels.Channel, error)
```

**消息总线契约**（不新增字段，复用现有）：

| 方向 | 类型 | QQ 映射 |
|------|------|---------|
| 入站 | `bus.InboundMessage` | `Channel`=实例名; `SenderID`=QQ号; `ChatID`=群号(群)/QQ号(私聊); `PeerKind`=`group`/`direct`; `Media`=[图片/语音]; `Metadata`={message_id, group_id, user_id, nickname} |
| 出站 | `bus.OutboundMessage` | `ChatID` 同上; `Content`=纯文本/Markdown; `Media`=[图片/语音/文件]; `Metadata`={reply_to, placeholder_key} |

### 5.2 OneBot 11 协议接口（外部，与协议实现端交互）

**通信规范**：协议 = WebSocket（ws/wss）；格式 = JSON；编码 = UTF-8；鉴权 = `access_token`。

#### 5.2.1 入站事件（实现端 → gateway）

| 事件 | 触发 | 关键字段 | GoClaw 处理 |
|------|------|----------|-------------|
| `message.private` | 私聊消息 | `user_id`, `message[]`, `raw_message`, `sender{nickname}` | → `InboundMessage`(direct) |
| `message.group` | 群消息 | `group_id`, `user_id`, `message[]`, 含 `at` 段 | → `InboundMessage`(group)，解析 @ |
| `notice.group_upload` | 群文件 | `group_id`, `file{name,url}` | → 媒体下载（二期） |
| `meta_event.heartbeat` | 心跳 | `time`, `self_id` | → 健康探活 |
| `meta_event.lifecycle` | 连接生命周期 | `sub_type=lifecycle/enable` | → MarkHealthy / 重连 |

入站事件示例（群消息）：

```json
{
  "post_type": "message",
  "message_type": "group",
  "time": 1700000000,
  "self_id": 10001,
  "sub_type": "normal",
  "group_id": 123456789,
  "user_id": 88888888,
  "message": [
    {"type": "at",    "data": {"qq": "10001"}},
    {"type": "text",  "data": {"text": " 你好"}}
  ],
  "raw_message": "[CQ:at,qq=10001] 你好",
  "sender": {"user_id": 88888888, "nickname": "张三"}
}
```

#### 5.2.2 出站动作（gateway → 实现端，请求-响应，`echo` 配对）

| 动作 | 参数 | 用途 |
|------|------|------|
| `send_private_msg` | `user_id`, `message[]` | 私聊回复 |
| `send_group_msg` | `group_id`, `message[]` | 群回复 |
| `delete_msg` | `message_id` | 撤回占位消息（流式近似） |
| `get_group_member_list` | `group_id` | `GroupMemberProvider` |
| `get_login_info` | — | `Start()` 校验连接 |
| `set_msg_emoji_like` | `message_id`, `emoji` | reaction（二期） |

出站动作示例：

```json
{
  "action": "send_group_msg",
  "params": {
    "group_id": 123456789,
    "message": [{"type": "text", "data": {"text": "回复内容"}}]
  },
  "echo": "req-0001"
}
```

响应示例：

```json
{"status": "ok", "retcode": 0, "data": {"message_id": 999}, "echo": "req-0001"}
```

#### 5.2.3 错误码（OneBot `retcode`）

| retcode | 含义 | GoClaw 处理 |
|---------|------|-------------|
| 0 | 成功 | 正常 |
| 1 | 失败（一般） | `slog.Warn`，降级为文本；计入健康 `Degraded` |
| 2 | 失败（协议端异常，如掉登录） | `MarkFailed` + 重连退避 |

### 5.3 HTTP/WS 管理接口（复用现有，无新增端点）

QQ 实例的 CRUD 复用现有端点，仅需放开 `isValidChannelType` 白名单（接入点 #5、#6）：

| 接口 | 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|------|
| 列出实例 | GET | `/v1/channels/instances` | auth | 返回含 `channel_type=qq` |
| 创建实例 | POST | `/v1/channels/instances` | admin | `channel_type:"qq"` |
| 查询实例 | GET | `/v1/channels/instances/{id}` | auth | — |
| 更新实例 | PUT | `/v1/channels/instances/{id}` | admin | — |
| 删除实例 | DELETE | `/v1/channels/instances/{id}` | admin | — |

**创建实例请求示例**（POST `/v1/channels/instances`，复用现有 schema）：

```json
{
  "name": "qq-main",
  "display_name": "QQ 主号",
  "channel_type": "qq",
  "agent_id": "0190xxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "enabled": true,
  "credentials": {"access_token": "tok_xxx", "self_id": "10001"},
  "config": {
    "direction": "reverse",
    "reverse_path": "/v1/onebot/qq",
    "dm_policy": "pairing",
    "group_policy": "pairing",
    "require_mention": true,
    "history_limit": 50
  }
}
```

> `credentials` 经现有 store 层 AES-256-GCM 加密后落库，永不回传 API（`json:"-"`）。

---

## 6. 安全设计

### 6.1 认证授权

| 维度 | 措施 |
|------|------|
| 渠道实例管理 | 复用现有 RBAC：创建/更新/删除需 `adminAuth`（`requireTenantAdmin`），租户隔离（SQL `WHERE tenant_id=$N`） |
| OneBot 协议鉴权 | `access_token` 经 `credentials` 加密存储。**实现点**：在 `Upgrader.Upgrade()` 把 HTTP→WS 升级前，用 `r.Header.Get("Authorization")`（取 `Bearer <token>`）或 `r.URL.Query().Get("access_token")` 校验；不匹配 → 写 HTTP 401 + 拒绝升级 + `slog.Warn("security.*")`。`access_token` 留空（仅内网部署）则跳过校验 |
| 反向 WS 路径 | 每实例独立挂载路径 `{reverse_path}/{instance_name}`（见 4.4），避免单一路径承载多账号；路径不公开 |
| 租户隔离 | `BaseChannel.SetTenantID` 透传；`InboundMessage.TenantID` 一路下传至 session/memory，确保跨租户不串 |

### 6.2 数据安全

| 维度 | 措施 |
|------|------|
| 凭据加密 | AES-256-GCM（`internal/crypto`），`credentials` 列 `json:"-"` 不回传 |
| @注入防护 | OneBot 数组消息段天然隔离 CQ 码注入；出站文本中 CQ 码做转义（`&#91;`/`&#93;`/`&amp;`）防伪造消息段 |
| SSRF | 媒体 `http(s)://` URL 下载走现有 SSRF 防护；base64 内联优先 |
| 媒体大小 | `media_max_bytes` 硬上限（默认 20MB），超限丢弃 + 告警 |
| 限流 | 复用 `ratelimit.go` + `quota.go`，按 sender/group 计数 |
| 失败关闭 | 配对查询失败 → `PolicyDeny`（fail-closed，参考 channel.go:343） |
| 撤回滥用 | 流式近似用的 `delete_msg` 仅作用于本实例已发消息（message_id 白名单），不撤回用户消息 |

### 6.3 风控注意（非代码层，需文档告知部署者）

> 以下属协议端/账号侧责任，与 gateway 解耦，但**必须写入部署文档告知使用者**：

1. **NTQQ 方案本质是"外挂"**：NapCat/Lagrange/LLOneBot 从腾讯角度属被打击目标，存在**封号风险**。
2. **使用小号/专用号**：避免主号被封造成不可逆损失。
3. **服务器地理位置**：海外服务器更易触发风控，建议国内部署。
4. **PC 端协议冲突**：三个实现端均占用 PC 端协议，登录期间不可同时登录桌面 QQ。
5. **30 天重登限制**：协议端登录态每 30 天需重新登录一次。
6. **长期趋势**：腾讯对第三方接入打击力度持续加强，有条件可引导迁移至 Telegram/Discord 等开放平台。
7. **网络部署**：协议实现端与 gateway 同机或内网部署，反向 WS 不暴露公网；公网必须走 `wss://` + 强 `access_token`。

---

## 7. 实施阶段（建议顺序，供排期参考）

| 阶段 | 产物 | 验收 |
|------|------|------|
| P1 协议骨架 | `onebot/{client,types}.go`；能连/读/写、echo 配对、heartbeat 探活 | 与 NapCat 打通：收发文本 |
| P2 渠道适配 | `qq/{channel,factory,handle,send}.go`；注册工厂；放开两处白名单 | 实例化 + 私聊/群聊文本闭环 |
| P3 策略与媒体 | 配对/白名单/@提及/限流；`media.go` 图片语音收发 | 多租户 + 群策略 + 媒体闭环 |
| P4 体验增强 | `WebhookChannel` 反向 WS 挂载；`PendingCompactable`；流式近似；i18n + Web UI | UI 可配置；健康面板可见；热重载可用 |
| P5 测试 | 单元（types/转义/分段）+ 集成（fake OneBot 服务端）+ chaos（断连重连） | `go test -race ./internal/channels/qq/...` 通过 |

> 测试遵循项目规范：单元 + 集成 + chaos；**不写**吞吐/延迟/内存基准测试（CLAUDE.md 明确要求）。

---

## 附录 A：与现有渠道的对照

| 维度 | Discord | Feishu | **QQ（本方案）** |
|------|---------|--------|------------------|
| 协议 | Gateway WS | WS/Webhook | **OneBot 11 WS** |
| 凭据 | Bot Token | AppID/AppSecret | **access_token + self_id** |
| 连接方向 | 出（gateway→Discord） | 双（websocket 模式出，webhook 模式入） | **双向（reverse 入 / forward 出）** |
| Destroyer | 否 | 否 | **否** |
| WebhookChannel | 否 | 是（webhook 模式） | **是（reverse 模式）** |
| 新建表 | 否 | 否 | **否（复用 channel_instances）** |

## 附录 B：决策记录

- **为何不接腾讯官方 QQ 机器人 API**：需企业资质、审核周期长、能力受限（无法覆盖个人会话），与 GoClaw「个人/团队 Agent 网关」定位不符。OneBot 11 生态成熟、可替换实现端、零资质门槛。
- **为何自研 OneBot 客户端而非用三方库**：协议简单（WS+JSON），三方库维护质量与耦合度不可控，自研与现有 channel 模式（gorilla/websocket + BaseChannel）零摩擦。
- **为何复用 `channel_instances` 而不新建表**：QQ 是渠道*类型*而非子系统，所有结构需求（凭据加密/配置 JSON/多租户/启停）现有表均已覆盖，建新表违反「最小改动」与索引规范。
