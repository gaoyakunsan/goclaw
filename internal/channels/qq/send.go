package qq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/qq/onebot"
)

// sendMessage maps an OutboundMessage onto OneBot send_private_msg /
// send_group_msg. It handles NO_REPLY suppression, media, chunking, and the
// placeholder-key delete-then-resend approximation of streaming.
func (c *Channel) sendMessage(ctx context.Context, msg bus.OutboundMessage) error {
	c.clientMu.Lock()
	client := c.client
	c.clientMu.Unlock()
	if client == nil {
		return errors.New("qq channel not connected")
	}

	// NO_REPLY / suppressed path: empty content + no media means the caller
	// wants downstream cleanup only. Matches Telegram/Discord/Slack convention.
	if msg.Content == "" && len(msg.Media) == 0 {
		return nil
	}

	// Resolve group vs private from the tracked peer kind. Fall back to private
	// so an unknown ChatID doesn't accidentally broadcast into a group.
	isGroup := c.lookupPeerKind(msg.ChatID) == "group"

	// Placeholder delete: when a placeholder_key is present, delete the prior
	// message before sending the real one (QQ has no native edit; this is the
	// delete+resend streaming approximation).
	if key := msg.Metadata["placeholder_key"]; key != "" {
		if id, err := strconv.ParseInt(key, 10, 64); err == nil && id != 0 {
			if err := client.Send(ctx, onebot.ActionDeleteMsg, onebot.DeleteMsgParams{MessageID: id}); err != nil {
				slog.Debug("qq placeholder delete failed", "key", key, "error", err)
			}
		}
	}

	// Media-first: send each attachment as its own message (OneBot send_msg
	// accepts a single image/record/file segment per call cleanly). Text, if
	// any, follows as one or more text messages.
	for _, media := range msg.Media {
		seg, err := c.buildOutboundMediaSegment(ctx, media)
		if err != nil {
			slog.Warn("qq send media failed", "url", media.URL, "error", err)
			continue
		}
		// Optional caption rides on the first media segment.
		var segs []onebot.MessageSegment
		segs = append(segs, seg)
		if caption := stripMarkdown(media.Caption); caption != "" {
			segs = append(segs, onebot.MessageSegment{
				Type: onebot.SegmentText,
				Data: map[string]any{"text": onebot.EscapeCQText(caption)},
			})
		}
		if err := c.callSend(ctx, client, msg.ChatID, isGroup, segs); err != nil {
			slog.Warn("qq send media call failed", "chat_id", msg.ChatID, "error", err)
		}
	}

	// Send text content. QQ plain-text messages render no Markdown, so strip
	// formatting sigils before chunking. Order: strip → chunk → CQ-escape.
	text := stripMarkdown(msg.Content)
	if text == "" {
		return nil
	}

	chunkLimit := c.cfg.ChunkLimit
	if chunkLimit <= 0 {
		chunkLimit = defaultChunkLimit
	}
	for _, chunk := range channels.ChunkMarkdown(text, chunkLimit) {
		segs := []onebot.MessageSegment{{
			Type: onebot.SegmentText,
			Data: map[string]any{"text": onebot.EscapeCQText(chunk)},
		}}
		// Prefix with a reply segment if the inbound message asked for a reply.
		if replyTo := msg.Metadata["reply_to"]; replyTo != "" {
			if id, err := strconv.ParseInt(replyTo, 10, 64); err == nil && id != 0 {
				segs = append([]onebot.MessageSegment{{
					Type: onebot.SegmentReply,
					Data: map[string]any{"id": id},
				}}, segs...)
			}
		}
		if err := c.callSend(ctx, client, msg.ChatID, isGroup, segs); err != nil {
			return err
		}
	}
	return nil
}

// callSend routes to send_private_msg / send_group_msg based on isGroup and
// classifies failures into channel health states.
func (c *Channel) callSend(ctx context.Context, client *onebot.Client, chatID string, isGroup bool, segs []onebot.MessageSegment) error {
	id, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return fmt.Errorf("qq: invalid chat id %q: %w", chatID, err)
	}

	var action string
	var params any
	if isGroup {
		action = onebot.ActionSendGroupMsg
		// Build params manually so message_type is unambiguous.
		params = map[string]any{"group_id": id, "message": segs}
	} else {
		action = onebot.ActionSendPrivateMsg
		params = map[string]any{"user_id": id, "message": segs}
	}

	resp, err := client.Call(ctx, action, params)
	if err != nil {
		c.classifySendError(err)
		return err
	}
	// retcode handling per design 5.2.3: 0=ok, 1=warn/degraded, 2=fail/reconnect.
	_ = resp
	return nil
}

// classifySendError maps a send failure onto health states so the operator
// sees a degraded/failed channel instead of silently dropped replies.
func (c *Channel) classifySendError(err error) {
	if err == nil {
		return
	}
	var rc *onebot.ErrRetcode
	if errors.As(err, &rc) {
		switch rc.Code {
		case 2:
			// Protocol-end anomaly (e.g. dropped login) — mark failed, reconnect.
			c.MarkFailed("OneBot send failed", err.Error(), channels.ChannelFailureKindNetwork, true)
		default:
			c.MarkDegraded("OneBot send degraded", err.Error(), channels.ChannelFailureKindUnknown, true)
		}
		return
	}
	// Transport-level error (not a retcode) — degraded, retryable.
	c.MarkDegraded("OneBot transport error", err.Error(), channels.ChannelFailureKindNetwork, true)
}

// lookupPeerKind returns the remembered "group"/"private" for a chatID, or
// "" if unknown.
func (c *Channel) lookupPeerKind(chatID string) string {
	v, ok := c.peerKind.Load(chatID)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}
