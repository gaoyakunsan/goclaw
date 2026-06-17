package qq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/qq/onebot"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/safego"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// upgrader is the per-connection HTTP→WebSocket upgrader for reverse mode.
// CheckOrigin is permissive: cross-origin doesn't apply to a server-side WS
// upgrade, and the access_token gate is the real auth boundary.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(_ *http.Request) bool { return true },
}

// reconnectBackoff caps the delay between forward-mode reconnect attempts.
const reconnectBackoff = 5 * time.Second

// Channel connects to a QQ account via the OneBot 11 protocol.
//
// Concurrency model (mirrors Bitrix24/Feishu):
//   - BaseChannel locks its own running/health/allowlist state.
//   - cfg / accessToken / selfID / pendingStore are write-once at construction.
//   - client is created in Start() and read afterwards; guarded by clientMu so
//     Stop() sees a coherent snapshot even if it races a failed Start().
type Channel struct {
	*channels.BaseChannel

	cfg         config.QQConfig
	accessToken string
	selfID      string
	pendingStore store.PendingMessageStore

	clientMu sync.Mutex
	client   *onebot.Client

	// peerKind remembers group vs private for each outbound ChatID, since
	// bus.OutboundMessage carries no PeerKind. Populated on inbound dispatch.
	// Keyed by ChatID (group_id for groups, user_id for private).
	peerKind sync.Map // string → "group" | "private"

	// dedup guards against duplicate event delivery within a short window.
	dedup sync.Map // int64 message_id → time.Time

	// Stop coordination.
	stopOnce sync.Once
	stopCh   chan struct{}

	// Forward-mode reconnect loop ownership.
	fwdMu     sync.Mutex
	fwdCancel context.CancelFunc
	fwdWG     sync.WaitGroup

	// Reverse-mode webhook wiring. whBuilt ensures the per-instance handler
	// is constructed exactly once (WebhookHandler may be queried by the
	// manager before Start completes).
	whMu      sync.Mutex
	whBuilt   bool
	whHandler http.Handler
}

// New constructs a QQ channel. pendingStore is optional (nil = RAM-only
// pending history). The caller (factory) wires pairing/tenant/agent-id via
// BaseChannel setters; New only owns channel-internal state.
func New(cfg config.QQConfig, creds qqCreds, msgBus *bus.MessageBus, pairingSvc store.PairingStore, pendingStore store.PendingMessageStore) (*Channel, error) {
	base := channels.NewBaseChannel(channels.TypeQQ, msgBus, cfg.AllowFrom)
	base.SetPairingService(pairingSvc)
	base.SetGroupHistory(channels.MakeHistory(channels.TypeQQ, pendingStore, base.TenantID()))
	base.ValidatePolicy(cfg.DMPolicy, cfg.GroupPolicy)

	historyLimit := cfg.HistoryLimit
	if historyLimit == 0 {
		historyLimit = channels.DefaultGroupHistoryLimit
	}
	base.SetHistoryLimit(historyLimit)
	requireMention := true
	if cfg.RequireMention != nil {
		requireMention = *cfg.RequireMention
	}
	base.SetRequireMention(requireMention)

	c := &Channel{
		BaseChannel:  base,
		cfg:          cfg,
		accessToken:  creds.AccessToken,
		selfID:       creds.SelfID,
		pendingStore: pendingStore,
		stopCh:       make(chan struct{}),
	}
	return c, nil
}

// Type always returns the platform type, independent of the instance name.
func (c *Channel) Type() string { return channels.TypeQQ }

// BlockReplyEnabled returns the per-channel block_reply override (nil = inherit).
func (c *Channel) BlockReplyEnabled() *bool { return c.cfg.BlockReply }

// SetPendingCompaction wires LLM-based auto-compaction into the group history.
func (c *Channel) SetPendingCompaction(cfg *channels.CompactionConfig) {
	if gh := c.GroupHistory(); gh != nil {
		gh.SetCompactionConfig(cfg)
	}
}

// SetPendingHistoryTenantID propagates the instance tenant_id into the pending
// history so DB writes are tenant-scoped. MUST be implemented: the
// InstanceLoader calls this via duck-typing after construction, and the
// factory creates the PendingHistory before the loader's SetTenantID runs —
// without this the history would write under uuid.Nil and cross tenant rows.
func (c *Channel) SetPendingHistoryTenantID(id uuid.UUID) {
	if gh := c.GroupHistory(); gh != nil {
		gh.SetTenantID(id)
	}
}

// Start brings the channel online.
//
// Reverse mode: builds the OneBot client, mounts nothing here (the manager
// mounts the WebhookHandler on the main mux), and waits for the protocol
// implementation to dial in. Marks the channel "starting / awaiting connection".
//
// Forward mode: builds the client and launches the reconnect loop that dials
// the configured endpoint. Marks healthy once the first connection succeeds.
func (c *Channel) Start(ctx context.Context) error {
	c.MarkStarting("Connecting to OneBot")

	if err := c.buildClient(); err != nil {
		c.MarkFailed("Setup failed", err.Error(), channels.ChannelFailureKindConfig, false)
		return err
	}

	c.GroupHistory().StartFlusher()
	c.SetRunning(true)

	if c.cfg.Direction == "forward" {
		c.startForwardLoop(ctx)
		// Probe once to surface a fast failure (bad host/credentials) instead
		// of silently retrying. The reconnect loop keeps running regardless.
		if !c.client.IsConnected() {
			c.MarkDegraded("Connecting", "forward WS not yet connected; reconnecting",
				channels.ChannelFailureKindNetwork, true)
		}
	} else {
		// Reverse: healthy the moment the implementation connects (handled in
		// the WS upgrade path). Until then we sit at "awaiting connection".
		c.MarkDegraded("Awaiting connection",
			"OneBot implementation should connect to "+c.reverseMountPath(),
			channels.ChannelFailureKindNetwork, true)
	}

	slog.Info("qq channel started",
		"name", c.Name(), "direction", c.cfg.Direction, "self_id", c.selfID)
	return nil
}

// buildClient constructs the OneBot client once with the event handler bound.
func (c *Channel) buildClient() error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	if c.client != nil {
		return nil
	}
	c.client = onebot.NewClient(c.accessToken, c.handleEvent)
	c.client.SetDisconnectHandler(c.onDisconnect)
	return nil
}

// onDisconnect is fired by the OneBot client when a read loop ends.
func (c *Channel) onDisconnect(err error) {
	if err != nil && !isNormalClose(err) {
		slog.Warn("qq onebot connection lost", "name", c.Name(), "error", err)
	}
	if c.cfg.Direction == "reverse" {
		// Reverse: back to waiting for the implementation to reconnect.
		c.MarkDegraded("Awaiting connection", "OneBot implementation disconnected; awaiting reconnect",
			channels.ChannelFailureKindNetwork, true)
	}
	// Forward: the reconnect loop owns re-establishing + re-marks healthy.
}

// startForwardLoop runs the dial/reconnect loop until Stop().
func (c *Channel) startForwardLoop(ctx context.Context) {
	fwdCtx, cancel := context.WithCancel(ctx)
	c.fwdMu.Lock()
	c.fwdCancel = cancel
	c.fwdMu.Unlock()

	c.fwdWG.Add(1)
	go func() {
		defer safego.Recover(nil, "component", "qq_forward", "channel", c.Name())
		defer c.fwdWG.Done()
		c.forwardLoop(fwdCtx)
	}()
}

// forwardLoop dials the endpoint, marks healthy on success, and reconnects on
// drop with a bounded backoff. Exits when ctx (Stop) is cancelled.
func (c *Channel) forwardLoop(ctx context.Context) {
	endpoint := c.cfg.Endpoint
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		if err := c.client.DialForward(ctx, endpoint); err != nil {
			slog.Warn("qq forward dial failed; retrying",
				"name", c.Name(), "endpoint", endpoint, "error", err)
			c.MarkDegraded("Connecting", fmt.Sprintf("dial failed: %v", err),
				channels.ChannelFailureKindNetwork, true)
		} else {
			c.onConnected()
			// Block until the connection drops (the client's read loop runs in
			// the background; IsConnected flips false when it exits).
			c.awaitDisconnect(ctx)
		}

		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-time.After(reconnectBackoff):
		}
	}
}

// awaitDisconnect blocks until the client reports disconnected or ctx/stops.
func (c *Channel) awaitDisconnect(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-t.C:
			if !c.client.IsConnected() {
				return
			}
		}
	}
}

// onConnected is invoked once a fresh connection (either direction) is live.
// Probes login info to confirm the implementation is responsive and stamps
// the bot's self_id if the operator left it blank in credentials.
func (c *Channel) onConnected() {
	c.MarkHealthy("Connected")
	go func() {
		defer safego.Recover(nil, "component", "qq_probe", "channel", c.Name())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := c.client.Call(ctx, onebot.ActionGetLoginInfo, nil)
		if err != nil {
			slog.Debug("qq get_login_info failed", "name", c.Name(), "error", err)
			return
		}
		var info onebot.LoginInfoData
		if err := json.Unmarshal(resp.Data, &info); err == nil && info.UserID != 0 {
			// Stamp self_id from the implementation if the operator didn't.
			c.clientMu.Lock()
			if c.selfID == "" {
				c.selfID = strconv.FormatInt(info.UserID, 10)
				slog.Info("qq self_id resolved from login info", "name", c.Name(), "self_id", c.selfID)
			}
			c.clientMu.Unlock()
		}
	}()
}

// Stop closes the OneBot client, the forward reconnect loop, and the flusher.
// Idempotent.
func (c *Channel) Stop(_ context.Context) error {
	c.stopOnce.Do(func() { close(c.stopCh) })

	c.fwdMu.Lock()
	cancel := c.fwdCancel
	c.fwdMu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.fwdWG.Wait()

	c.clientMu.Lock()
	client := c.client
	c.clientMu.Unlock()
	if client != nil {
		_ = client.Close()
	}

	if gh := c.GroupHistory(); gh != nil {
		gh.StopFlusher()
	}
	c.SetRunning(false)
	c.MarkStopped("")
	slog.Info("qq channel stopped", "name", c.Name())
	return nil
}

// --- Reverse-WS webhook handler ---

// reverseMountPath returns the full per-instance WS path the implementation
// must connect to: {reverse_path}/{instance_name}.
func (c *Channel) reverseMountPath() string {
	base := c.cfg.ReversePath
	if base == "" {
		base = defaultReversePath
	}
	return strings.TrimRight(base, "/") + "/" + c.Name()
}

// WebhookHandler implements channels.WebhookChannel. In reverse mode it returns
// the per-instance WS endpoint; in forward mode it returns ("", nil) since the
// gateway dials out and no inbound HTTP route is needed.
func (c *Channel) WebhookHandler() (string, http.Handler) {
	if c.cfg.Direction == "forward" {
		return "", nil
	}
	c.whMu.Lock()
	defer c.whMu.Unlock()
	if !c.whBuilt {
		c.whHandler = http.HandlerFunc(c.handleReverseWS)
		c.whBuilt = true
	}
	return c.reverseMountPath(), c.whHandler
}

// handleReverseWS authenticates the WS upgrade request (access_token) and hands
// the upgraded connection to the OneBot client. A new connection replaces any
// prior one (reconnect scenario).
func (c *Channel) handleReverseWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !c.checkAccessToken(r) {
		slog.Warn("security.qq_reverse_ws_token_mismatch",
			"channel", c.Name(), "remote", r.RemoteAddr)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already wrote an error response; nothing more to do.
		slog.Debug("qq reverse ws upgrade failed", "channel", c.Name(), "error", err)
		return
	}

	c.clientMu.Lock()
	client := c.client
	c.clientMu.Unlock()
	if client == nil {
		// Channel not started yet (or already stopped); reject the connection.
		_ = conn.Close()
		return
	}
	if err := client.AcceptReverse(conn); err != nil {
		slog.Warn("qq reverse ws accept failed", "channel", c.Name(), "error", err)
		return
	}
	c.onConnected()
	slog.Info("qq reverse ws connection accepted", "channel", c.Name(), "remote", r.RemoteAddr)
}

// checkAccessToken validates the OneBot bearer token from either the
// Authorization header or ?access_token= query. Returns true when no token is
// configured (internal-only deployments trust the network boundary).
func (c *Channel) checkAccessToken(r *http.Request) bool {
	if c.accessToken == "" {
		return true
	}
	if got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer")); got == c.accessToken {
		return true
	}
	if got := r.URL.Query().Get("access_token"); got == c.accessToken {
		return true
	}
	return false
}

// Send delivers an outbound message via OneBot send_msg. See send.go.
func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	return c.sendMessage(ctx, msg)
}

// ListGroupMembers implements channels.GroupMemberProvider via
// get_group_member_list.
func (c *Channel) ListGroupMembers(ctx context.Context, chatID string) ([]channels.GroupMember, error) {
	c.clientMu.Lock()
	client := c.client
	c.clientMu.Unlock()
	if client == nil {
		return nil, errors.New("qq channel not connected")
	}
	resp, err := client.Call(ctx, onebot.ActionGetGroupMemberList, map[string]any{
		"group_id": chatID,
	})
	if err != nil {
		return nil, err
	}
	var members []onebot.GroupMember
	if err := json.Unmarshal(resp.Data, &members); err != nil {
		return nil, fmt.Errorf("qq: decode group member list: %w", err)
	}
	out := make([]channels.GroupMember, 0, len(members))
	for _, m := range members {
		name := m.Card
		if name == "" {
			name = m.Nickname
		}
		out = append(out, channels.GroupMember{
			MemberID: strconv.FormatInt(m.UserID, 10),
			Name:     name,
		})
	}
	return out, nil
}

// Client returns the OneBot client (exported for tests; nil before Start).
func (c *Channel) Client() *onebot.Client {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	return c.client
}

// isNormalClose reports whether a read-loop error is an expected disconnect
// (clean close / normal closure code), so we don't log it as a warning.
func isNormalClose(err error) bool {
	if err == nil {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "normal closure") ||
		strings.Contains(msg, "close 1000") ||
		strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "EOF")
}
