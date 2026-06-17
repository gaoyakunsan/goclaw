# QQ 渠道接入方案 Review 与优化建议

> 对 `docs/qq-channel-integration.md` 的技术评审，含代码级问题、方案对比、优化建议。

---

## 一、代码级问题

### 🔴 严重

**1. 缺少 `SetPendingHistoryTenantID` 接口实现**

方案 5.1 节列出了 QQ 需实现的接口清单，但遗漏了 `SetPendingHistoryTenantID(uuid.UUID)`。`InstanceLoader` 在此处传递 tenantID：

`instance_loader.go:280-282`：

```go
if ph, ok := ch.(interface{ SetPendingHistoryTenantID(uuid.UUID) }); ok {
    ph.SetPendingHistoryTenantID(inst.TenantID)
}
```

所有使用 `PendingHistory`（持久化群历史）的渠道（Discord、Telegram、Feishu、Slack、Zalo Personal）都实现了此方法。QQ 若需持久化群消息历史，**必须实现**。如果用 `NewPendingHistory()`（纯内存），可不实现，但方案需明确说明选择哪种。

**2. `QQConfig` 结构体未定义**

方案 4.4 节定义了 config JSONB 契约（`direction`/`endpoint`/`reverse_path`/`dm_policy`/`group_policy` 等），接入点 #7 提出在 `config_channels.go:15` 的 `ChannelsConfig` 中增加 `QQ QQConfig` 字段，但**全文未给出 `QQConfig` Go 结构体定义**。工厂函数需将 `json.RawMessage` 解析为该结构体，缺失则无法编译。

建议补充定义：

```go
// QQConfig configures a QQ (OneBot 11) channel instance via config JSONB.
// Non-secret fields only; secrets go into credentials (AES-256-GCM encrypted).
type QQConfig struct {
    Direction      string   `json:"direction,omitempty"`       // "reverse" (default) or "forward"
    Endpoint       string   `json:"endpoint,omitempty"`        // forward mode ws://host:port
    ReversePath    string   `json:"reverse_path,omitempty"`    // reverse WS mount path, default "/v1/onebot/qq"
    DMPolicy       string   `json:"dm_policy,omitempty"`       // "pairing" (default), "open", "allowlist", "disabled"
    GroupPolicy    string   `json:"group_policy,omitempty"`    // "open" (default), "pairing", "allowlist", "disabled"
    AllowFrom      []string `json:"allow_from,omitempty"`      // QQ号/群号白名单
    RequireMention *bool    `json:"require_mention,omitempty"` // 群@提及门控 (默认 true)
    HistoryLimit   int      `json:"history_limit,omitempty"`   // 群待处理消息上限 (默认 50)
    BlockReply     *bool    `json:"block_reply,omitempty"`     // 覆盖 gateway block_reply (nil=继承)
    MediaMaxBytes  int64    `json:"media_max_bytes,omitempty"` // 媒体下载上限 (默认 20MB)
    ChunkLimit     int      `json:"chunk_limit,omitempty"`     // 单条消息分片长度 (默认 4500)
}
```

同时在 `ChannelsConfig` 中增加：

```go
type ChannelsConfig struct {
    // ... existing fields ...
    QQ QQConfig `json:"qq"`
    // ...
}
```

---

### 🟡 中等

**3. 反向 WS 多实例路由细节不够明确**

方案说 `reverse_path` 默认 `/v1/onebot/qq`、多实例时按 name 区分。`WebhookHandler()` 返回 `(path, http.Handler)`，需明确路由机制：

- 多实例时路径应带区分后缀，如 `/v1/onebot/qq/{instance_name}`
- OneBot 实现端（NapCat/Lagrange）需相应配置 `ws://gateway:port/v1/onebot/qq/{name}`
- 路径中的 `{name}` 需与 `channel_instances.name` 一致，`WebhookHandler()` 按 `reverse_path + "/" + name` 拼接

建议在 4.4 节 `reverse_path` 说明中补充：

> 多实例时，实际挂载路径为 `{reverse_path}/{instance_name}`（如 `/v1/onebot/qq/qq-main`），与 `channel_instances.name` 对应。OneBot 实现端的 WS 连接地址需与之匹配。

**4. 接入点遗漏 `ui/web/src/constants/channels.ts` 的显式改动**

两个 `isValidChannelType` 函数的注释（`channel_instances.go:325-329` 和 `channel_instances.go:700-702`）都明确要求同步 `CHANNEL_TYPES`。接入点 #10 说"UI 适配"太宽泛，应显式列出 `channels.ts` 新增条目：

```ts
{ value: "qq", label: "QQ" },
```

否则 WS 驱动的 UI 拒绝 QQ 但 HTTP API 接受 QQ，不一致会导致线上 bug。

**5. GroupPolicy 常量与 config 契约默认值不一致**

方案配置中 `group_policy` 默认 `"pairing"`，但 `channel.go:67-70` 的 `GroupPolicy` 常量只有 `open/allowlist/disabled`，缺少 `pairing`。虽然运行时 `CheckGroupPolicy()` 代码路径支持 `"pairing"`（第 371 行），但新开发者按常量列表编码会遗漏。建议在配置说明中标注"运行时支持但未定义常量"。

---

### 🟢 建议

**6. 流式近似与 `StreamingChannel` 的关系需澄清**

QQ 无原生消息编辑能力，方案中 `send.go` 描述的"撤回+重发"与 Telegram/Slack 的原地编辑流式模式不同。`StreamingChannel.CreateStream()` 返回 `ChannelStream` 依赖编辑原消息——QQ 只能用 delete+resend。建议在方案中明确：QQ 的 `StreamingChannel` 实现（二期）将是自定义模式，每次"编辑"=delete 原消息 + send 新消息，体验和速率均与 Telegram 不同。

**7. 反向 WS handler 的 `access_token` 校验点需明确**

方案说反相 WS "WS 握手校验 `Authorization: Bearer` / `?access_token=`"（第 382 行），但未明确实现方式。gorilla/websocket 的 `Upgrader.Upgrade()` 在 HTTP→WS 升级前可拦截请求做鉴权，应在 handler 中用 `r.Header.Get("Authorization")` 或 `r.URL.Query().Get("access_token")` 校验，失败返回 HTTP 401 并拒绝升级。建议在 6.1 节补充此实现细节。

**8. `ContactCollector` 扩展点未提及**

`BaseChannel.contactCollector` 提供自动联系人收集能力，Telegram/Discord/Feishu 等均利用。QQ 有 `sender.nickname` 字段可填充，建议在 `handle.go` 说明中加入此能力。

**9. `PendingHistory` 持久化策略未明确**

QQ 群聊需要 `PendingHistory` 做消息历史累积/压缩。方案未说明是用 `NewPendingHistory()`（纯内存，重启丢失）还是 `NewPersistentHistory()`（DB 持久化）。建议明确：生产环境用持久化版本（需 `PendingMessageStore`），工厂创建时依赖注入。

---

## 二、方案选型验证

经全网搜索验证，当前（2025-2026）所有 QQ 接入方案对比如下：

| 方案 | 群聊 | 私聊 | AI 接入 | 稳定性 | 合规性 |
|------|------|------|---------|--------|--------|
| **QQ 官方开放平台 API** | ⚠️ 沙箱≤20人 | ✅ | 🔴 **禁止** | ✅ 稳定 | ✅ 官方 |
| **OneBot 11 + NTQQ 无头**（本方案） | ✅ | ✅ | ✅ 自由 | 🟡 偶有风控 | ⚠️ 灰色地带 |
| **NTQQ 协议直接逆向**（Windigo 等） | 理论支持 | 理论支持 | ✅ | 🔴 极差 | 🔴 违规 |
| **QQ 频道机器人** | ❌ 仅频道 | ❌ | ⚠️ 受限 | ✅ | ✅ |

### 官方 API 不可行的原因

1. **禁止 AI 接入**：官方明确不允许个人开发者接入 AI/AIGC 功能，需企业认证 + 模型备案。GoClaw 核心是 AI Agent 网关，与官方定位直接冲突
2. **主动消息限制**：每月仅 4 条（群+私合计），Agent 对话场景不可用
3. **无法获取用户真实 QQ 号/群号**：平台隐藏用户身份信息
4. **WebSocket 即将废弃**：官方推荐 Webhook，增加部署复杂度（HTTPS+域名+ICP 备案）
5. **内容审查严格**：不允许链接，严重限制 Agent 工具调用结果展示

### 协议直接逆向不可行的原因

go-cqhttp（Go 语言，基于 MiraiGo 逆向 Android QQ 协议）已停更，原因是腾讯不断更新加密方案导致逆向项目无法追赶。Windigo 等项目同理——维护成本不可持续，生产环境不可用。

### OneBot v11 vs v12 的取舍

| 维度 | v11 | v12 |
|------|-----|-----|
| QQ 生态实现端 | ✅ NapCat/Lagrange/LLOneBot 全部支持 | ❌ 几乎无 |
| 消息格式 | 数组消息段 + CQ 码双模 | 纯 JSON |
| 成熟度 | 🔥 极高 | 🟡 早期 |

**选 v11 正确**：v12 虽然规范更严格，但在 QQ 生态中无可用实现端。

---

## 三、实现端推荐排序

本方案（OneBot 11）与实现端解耦，用户可自选。建议文档中按以下优先级推荐：

| 优先级 | 实现端 | 理由 |
|--------|--------|------|
| **首选** | [Lagrange.Core](https://github.com/KonataDev/Lagrange.Core) | C# 自包含、资源最低、无需 QQ 客户端，适合服务器部署 |
| **备选** | [NapCatQQ](https://github.com/NapNeko/NapCatQQ) | 社区最活跃、文档最全，但需 Electron 运行环境 |
| **桌面** | [LLOneBot](https://github.com/LLOneBot/LLOneBot) | 需完整 NTQQ 客户端 + LiteLoaderQQNT 插件，适合有 GUI 的 Windows 桌面 |

---

## 四、风险提示（应写入部署文档）

1. **所有 NTQQ 方案本质是"外挂"**：从腾讯角度属于被打击目标，存在封号风险
2. **使用小号/专用号**：避免主号被封造成不可逆损失
3. **服务器地理位置**：海外服务器更容易触发风控，建议国内部署
4. **PC 端协议冲突**：NapCat/Lagrange/LLOneBot 均占用 PC 端协议，不可同时登录桌面 QQ
5. **30 天重登限制**：每 30 天需重新登录一次
6. **长期趋势**：腾讯对第三方接入的打击力度持续加强，如有条件可引导用户迁移至 Telegram/Discord 等开放平台

---

## 五、接入点清单修正

| # | 原方案 | 修正/补充 |
|---|--------|----------|
| 1 | `channel.go:73` 新增 `TypeQQ` 常量 | 确认行号准确（在现有常量块末尾追加） |
| 2 | `capabilities.go:22` 增 QQ | ✅ 行号准确 |
| 3 | `internal/channels/qq/*` 新建包 | 建议增加 `qq/config.go` 存放 `QQConfig` 解析 |
| 4 | `cmd/gateway.go` 注册工厂 | 行号范围 `530-551` 准确，追加在最后 |
| 5 | WS `isValidChannelType` 增 `"qq"` | 行号 `330` 准确 |
| 6 | HTTP `isValidChannelType` 增 `"qq"` | 行号 `703` 准确 |
| 7 | `config_channels.go` 增 `QQ QQConfig` | 需先定义 `QQConfig` 结构体（见问题 #2） |
| 8 | `edition.go` | ✅ 无需改（`AllowsChannels()` 已为 false，`MaxChannels` 不含 qq 即为 0=无限制） |
| 9 | i18n keys + 3 catalogs | ✅ 必改 |
| 10 | UI 适配 | **拆分为**：`channels.ts` 增 `{ value: "qq", label: "QQ" }` + 渠道表单 + 图标 |
| **NEW** | — | 需实现 `SetPendingHistoryTenantID(uuid.UUID)`（见问题 #1） |
| **NEW** | — | 需在部署文档中加入风控风险提示（见第四部分） |
