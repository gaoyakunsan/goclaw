package qq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/qq/onebot"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// pairingDebounce bounds how often we send a pairing-reply to the same sender.
const pairingDebounce = 60 * time.Second

// lastHealthUpdate prevents heartbeat events (which arrive every ~30s) from
// spamming the health store. We coalesce to one update per 60s.
const healthUpdateInterval = 60 * time.Second

// handleEvent is the OneBot client's event callback. It runs on the client's
// read goroutine, so all slow work (media download, pairing DB, agent publish)
// is dispatched onto detached contexts / the bus, never blocking the socket.
func (c *Channel) handleEvent(evt onebot.Event) {
	// Ignore the bot's own echoed messages and message_sent events.
	if evt.PostType == onebot.PostTypeSent {
		return
	}

	switch evt.PostType {
	case onebot.PostTypeMessage:
		c.handleMessage(evt)
	case onebot.PostTypeMetaEvent:
		c.handleMetaEvent(evt)
	case onebot.PostTypeNotice:
		// group_upload file events are a phase-2 media path; log for visibility.
		if evt.NoticeType == onebot.NoticeGroupUpload && evt.File != nil {
			slog.Debug("qq group_upload notice (phase 2)",
				"group_id", evt.GroupID, "file", evt.File.Name)
		}
	}
}

// handleMetaEvent reacts to lifecycle/heartbeat events for health + reconnect.
func (c *Channel) handleMetaEvent(evt onebot.Event) {
	switch evt.MetaEventType {
	case onebot.MetaEventLifecycle:
		// sub_type "enable"/"disable" on the connection lifecycle.
		c.MarkHealthy("Connected")
		slog.Info("qq onebot lifecycle", "name", c.Name(), "sub_type", evt.SubType, "self_id", evt.SelfID)
	case onebot.MetaEventHeartbeat:
		// Coalesce: heartbeats are frequent; only refresh health periodically.
		if c.canUpdateHealth() {
			c.MarkHealthy("Connected")
		}
	}
}

// canUpdateHealth rate-limits health writes driven by heartbeat events.
func (c *Channel) canUpdateHealth() bool {
	v, loaded := c.dedup.LoadOrStore("__health__", time.Now())
	if !loaded {
		return true
	}
	last, ok := v.(time.Time)
	if !ok {
		c.dedup.Store("__health__", time.Now())
		return true
	}
	if time.Since(last) >= healthUpdateInterval {
		c.dedup.Store("__health__", time.Now())
		return true
	}
	return false
}

// handleMessage converts a OneBot message event into a bus.InboundMessage and
// applies DM/group policy + mention gating + pending-history accumulation.
func (c *Channel) handleMessage(evt onebot.Event) {
	// Drop the bot's own messages (some implementations echo them as message,
	// not message_sent). self_id comparison is authoritative when available.
	if c.selfID != "" && strconv.FormatInt(evt.UserID, 10) == c.selfID {
		return
	}

	messageID := strconv.FormatInt(evt.MessageID, 10)
	if evt.MessageID != 0 && c.isDuplicate(messageID) {
		return
	}

	isGroup := evt.MessageType == onebot.MessageTypeGroup
	senderID := strconv.FormatInt(evt.UserID, 10)
	chatID := senderID
	peerKind := "direct"
	if isGroup {
		chatID = strconv.FormatInt(evt.GroupID, 10)
		peerKind = "group"
	}
	// Remember peer kind for outbound dispatch (OutboundMessage lacks it).
	c.peerKind.Store(chatID, peerKind)

	// Extract text + detect @bot mention.
	text, mentionedBot := c.extractContent(evt.Message)

	// Resolve media segments → temp files BEFORE the mention gate so non-mentioned
	// group messages still accumulate their attachments in pending history.
	mediaFiles, mediaTags := c.resolveInboundMedia(evt)

	// --- Group policy + mention gating ---
	if isGroup {
		if !c.checkGroupPolicy(c.tenantCtx(), senderID, chatID) {
			return
		}

		requireMention := c.RequireMention()
		if requireMention && !mentionedBot {
			// Not mentioned: record into pending history for later context.
			if gh := c.GroupHistory(); gh != nil && c.HistoryLimit() > 0 {
				gh.Record(chatID, channels.HistoryEntry{
					Sender:    evt.Sender.DisplayName(),
					SenderID:  senderID,
					Body:      text,
					Timestamp: eventTime(evt),
					MessageID: messageID,
				}, c.HistoryLimit())
			}
			// Collect contact even when not mentioned (cached; prevents DB spam).
			if cc := c.ContactCollector(); cc != nil {
				cc.EnsureContact(c.tenantCtx(), c.Type(), c.Name(), senderID, senderID,
					evt.Sender.DisplayName(), "", "group", "user", "", "")
			}
			return
		}
	}

	// --- DM policy ---
	if !isGroup {
		if !c.checkDMPolicy(c.tenantCtx(), senderID, chatID) {
			return
		}
	}

	if text == "" && len(mediaFiles) == 0 {
		text = "[empty message]"
	}

	senderName := evt.Sender.DisplayName()
	content := text

	// Build metadata.
	metadata := map[string]string{
		"message_id":    messageID,
		"user_id":       senderID,
		"nickname":      senderName,
		"display_name":  channels.SanitizeDisplayName(senderName),
		"mentioned_bot": strconv.FormatBool(mentionedBot),
		"platform":      channels.TypeQQ,
		"self_id":       c.selfID,
	}
	if isGroup {
		metadata["group_id"] = chatID
	}

	// Annotate with sender identity + (for groups) pending-history context.
	if senderName != "" {
		annotated := fmt.Sprintf("[From: %s]\n%s", senderName, content)
		if isGroup && c.HistoryLimit() > 0 {
			if gh := c.GroupHistory(); gh != nil {
				content = gh.BuildContext(chatID, annotated, c.HistoryLimit())
			} else {
				content = annotated
			}
		} else {
			content = annotated
		}
	}

	// Prepend media tags so the agent understands attachments.
	if mediaTags != "" {
		if content != "" {
			content = mediaTags + "\n\n" + content
		} else {
			content = mediaTags
		}
	}

	// Collect contact for processed messages (DM + mentioned group).
	if cc := c.ContactCollector(); cc != nil {
		cc.EnsureContact(c.tenantCtx(), c.Type(), c.Name(), senderID, senderID,
			senderName, "", peerKind, "user", "", "")
	}

	c.Bus().PublishInbound(bus.InboundMessage{
		Channel:      c.Name(),
		SenderID:     senderID,
		ChatID:       chatID,
		Content:      content,
		Media:        mediaFiles,
		PeerKind:     peerKind,
		UserID:       senderID,
		AgentID:      c.AgentID(),
		HistoryLimit: c.HistoryLimit(),
		TenantID:     c.TenantID(),
		Metadata:     metadata,
	})

	// Clear the group's pending context now that the bot has replied.
	if isGroup {
		if gh := c.GroupHistory(); gh != nil {
			gh.Clear(chatID)
		}
	}
}

// extractContent pulls plain text out of the message segments and reports
// whether the bot was @-mentioned. The @bot segment (and its trailing space)
// is stripped so the agent sees the user's actual instruction.
func (c *Channel) extractContent(msg onebot.Message) (string, bool) {
	var b strings.Builder
	mentionedBot := false
	for _, seg := range msg {
		switch seg.Type {
		case onebot.SegmentText:
			text := onebot.SegmentTextValue(seg)
			// If the previous segment was an @bot, drop the single leading
			// space OneBot leaves between the mention and the message body.
			if mentionedBot && b.Len() == 0 {
				text = strings.TrimLeft(text, " \t\r\n")
			}
			b.WriteString(text)
		case onebot.SegmentAt:
			qq := onebot.SegmentQQ(seg)
			if qq == c.selfID && c.selfID != "" {
				mentionedBot = true
				continue
			}
			// Mention of someone else — preserve as a readable marker.
			if name, ok := seg.Data["name"].(string); ok && name != "" {
				b.WriteString("@" + name)
			} else if qq != "" {
				b.WriteString("@" + qq)
			}
		case onebot.SegmentFace:
			// Emoji face — represent as a marker so it's not silently dropped.
			if id, ok := seg.Data["id"].(string); ok {
				b.WriteString("[face:" + id + "]")
			}
		case onebot.SegmentReply:
			// Reply target — surfaced separately; ignore in body.
		case onebot.SegmentImage, onebot.SegmentRecord, onebot.SegmentFile:
			// Media handled by resolveInboundMedia; nothing to add to text.
		default:
			// Unknown segment — preserve raw for diagnostics.
			b.WriteString("[" + seg.Type + "]")
		}
	}
	return strings.TrimSpace(b.String()), mentionedBot
}

// checkGroupPolicy applies the group policy, sending a pairing reply when the
// group needs pairing and isn't yet approved. Returns true to accept.
func (c *Channel) checkGroupPolicy(ctx context.Context, senderID, chatID string) bool {
	policy := c.cfg.GroupPolicy
	if policy == "" {
		policy = defaultGroupPolicy
	}
	switch policy {
	case "disabled":
		return false
	case "allowlist":
		return c.IsAllowed(senderID) || c.IsAllowed(chatID)
	case "pairing":
		// Allowlist shortcut: explicit allow_from beats pairing.
		if c.HasAllowList() && (c.IsAllowed(senderID) || c.IsAllowed(chatID)) {
			return true
		}
		if c.IsGroupApproved(chatID) {
			return true
		}
		groupSenderID := "group:" + chatID
		switch c.CheckGroupPolicy(ctx, groupSenderID, chatID, policy) {
		case channels.PolicyAllow:
			return true
		case channels.PolicyNeedsPairing:
			c.sendPairingReply(ctx, groupSenderID, chatID)
			return false
		default:
			return false
		}
	default: // "open"
		return true
	}
}

// checkDMPolicy applies the DM policy, sending a pairing reply when needed.
func (c *Channel) checkDMPolicy(ctx context.Context, senderID, chatID string) bool {
	switch c.CheckDMPolicy(ctx, senderID, c.cfg.DMPolicy) {
	case channels.PolicyAllow:
		return true
	case channels.PolicyNeedsPairing:
		c.sendPairingReply(ctx, senderID, chatID)
		return false
	default:
		slog.Debug("qq DM rejected by policy", "sender_id", senderID, "policy", c.cfg.DMPolicy)
		return false
	}
}

// sendPairingReply generates a pairing code and replies over OneBot so the
// operator can approve the sender/group. Debounced per sender to avoid spam.
func (c *Channel) sendPairingReply(ctx context.Context, senderID, chatID string) {
	ps := c.PairingService()
	if ps == nil {
		return
	}
	if !c.CanSendPairingNotif(senderID, pairingDebounce) {
		return
	}
	code, err := ps.RequestPairing(ctx, senderID, c.Name(), chatID, "default", nil)
	if err != nil {
		slog.Debug("qq pairing request failed", "sender_id", senderID, "error", err)
		return
	}
	reply := fmt.Sprintf(
		"GoClaw: access not configured.\n\nYour QQ: %s\n\nPairing code: %s\n\nAsk the bot owner to approve with:\n  goclaw pairing approve %s",
		senderID, code, code,
	)
	out := bus.OutboundMessage{Channel: c.Name(), ChatID: chatID, Content: reply, TenantID: c.TenantID()}
	if err := c.sendMessage(ctx, out); err != nil {
		slog.Warn("qq pairing reply send failed", "sender_id", senderID, "error", err)
		return
	}
	c.MarkPairingNotifSent(senderID)
	slog.Info("qq pairing reply sent", "sender_id", senderID, "code", code)
}

// tenantCtx returns a background context scoped to this channel's tenant so
// downstream store/pairing calls are tenant-isolated.
func (c *Channel) tenantCtx() context.Context {
	ctx := context.Background()
	if c.TenantID() != uuid.Nil {
		ctx = store.WithTenantID(ctx, c.TenantID())
	}
	return ctx
}

// isDuplicate guards against duplicate event delivery within a short window.
func (c *Channel) isDuplicate(messageID string) bool {
	_, loaded := c.dedup.LoadOrStore(messageID, time.Now())
	if loaded {
		return true
	}
	// Self-evict after the dedup window.
	time.AfterFunc(5*time.Minute, func() { c.dedup.Delete(messageID) })
	return false
}

// eventTime converts the event timestamp (unix seconds) to time.Time, falling
// back to now if absent.
func eventTime(evt onebot.Event) time.Time {
	if evt.Time > 0 {
		return time.Unix(evt.Time, 0)
	}
	return time.Now()
}
