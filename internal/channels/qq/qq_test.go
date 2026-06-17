package qq

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels/qq/onebot"
	"github.com/nextlevelbuilder/goclaw/internal/config"
)

// newTestChannel builds a Channel wired to a real bus with the given self_id,
// for exercising pure logic (no network). The OneBot client is NOT started.
// The bus's inbound consumer loop is started so registered handlers fire.
func newTestChannel(t *testing.T, selfID string, cfg config.QQConfig) (*Channel, *bus.MessageBus) {
	t.Helper()
	cfg = applyDefaults(cfg)
	mb := bus.New()
	ch, err := New(cfg, qqCreds{SelfID: selfID}, mb, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ch.SetName("qq-test")

	// Start the inbound dispatch loop (normally owned by the gateway consumer).
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for {
			msg, ok := mb.ConsumeInbound(ctx)
			if !ok {
				return
			}
			if h, ok := mb.GetHandler(msg.Channel); ok {
				_ = h(msg)
			}
		}
	}()
	return ch, mb
}

func TestExtractContent_AtBotStripsMention(t *testing.T) {
	ch, _ := newTestChannel(t, "10001", config.QQConfig{})
	msg := onebot.Message{
		{Type: onebot.SegmentAt, Data: map[string]any{"qq": "10001"}},
		{Type: onebot.SegmentText, Data: map[string]any{"text": " 你好"}},
	}
	text, mentioned := ch.extractContent(msg)
	if !mentioned {
		t.Fatal("expected bot to be mentioned")
	}
	if text != "你好" {
		t.Fatalf("expected stripped text '你好', got %q", text)
	}
}

func TestExtractContent_OtherAtPreserved(t *testing.T) {
	ch, _ := newTestChannel(t, "10001", config.QQConfig{})
	msg := onebot.Message{
		{Type: onebot.SegmentAt, Data: map[string]any{"qq": "20002", "name": "Alice"}},
		{Type: onebot.SegmentText, Data: map[string]any{"text": " help"}},
	}
	text, mentioned := ch.extractContent(msg)
	if mentioned {
		t.Fatal("bot should not be mentioned")
	}
	if !strings.Contains(text, "@Alice") {
		t.Fatalf("expected @Alice preserved, got %q", text)
	}
	if !strings.Contains(text, "help") {
		t.Fatalf("expected 'help' preserved, got %q", text)
	}
}

func TestExtractContent_PlainText(t *testing.T) {
	ch, _ := newTestChannel(t, "10001", config.QQConfig{})
	msg := onebot.Message{{Type: onebot.SegmentText, Data: map[string]any{"text": "hello"}}}
	text, mentioned := ch.extractContent(msg)
	if mentioned {
		t.Fatal("plain text should not be a mention")
	}
	if text != "hello" {
		t.Fatalf("got %q", text)
	}
}

func TestFactory_DefaultsAndValidation(t *testing.T) {
	t.Run("applies defaults", func(t *testing.T) {
		mb := bus.New()
		ch, err := Factory("qq-main", json.RawMessage(`{"self_id":"10001"}`), json.RawMessage(`{}`), mb, nil)
		if err != nil {
			t.Fatalf("Factory: %v", err)
		}
		qc := ch.(*Channel)
		if qc.cfg.Direction != "reverse" {
			t.Fatalf("default direction: %q", qc.cfg.Direction)
		}
		if qc.cfg.DMPolicy != "pairing" || qc.cfg.GroupPolicy != "pairing" {
			t.Fatalf("default policies: dm=%s group=%s", qc.cfg.DMPolicy, qc.cfg.GroupPolicy)
		}
		if qc.cfg.ChunkLimit != defaultChunkLimit {
			t.Fatalf("default chunk limit: %d", qc.cfg.ChunkLimit)
		}
		if qc.selfID != "10001" {
			t.Fatalf("selfID: %q", qc.selfID)
		}
	})

	t.Run("forward without endpoint rejected", func(t *testing.T) {
		mb := bus.New()
		_, err := Factory("qq-main", json.RawMessage(`{}`),
			json.RawMessage(`{"direction":"forward"}`), mb, nil)
		if err == nil {
			t.Fatal("expected error for forward without endpoint")
		}
	})

	t.Run("bad config json rejected", func(t *testing.T) {
		mb := bus.New()
		_, err := Factory("qq-main", json.RawMessage(`{}`),
			json.RawMessage(`{bad json`), mb, nil)
		if err == nil {
			t.Fatal("expected error for bad config json")
		}
	})
}

// TestInboundFlow_PrivateText publishes a private message event and asserts
// the bus receives a correctly-mapped InboundMessage (direct peer, content
// annotated with sender).
func TestInboundFlow_PrivateText(t *testing.T) {
	ch, mb := newTestChannel(t, "10001", config.QQConfig{DMPolicy: "open"})

	var got bus.InboundMessage
	var wg sync.WaitGroup
	wg.Add(1)
	mb.RegisterHandler("qq-test", func(m bus.InboundMessage) error {
		got = m
		wg.Done()
		return nil
	})

	ch.handleEvent(onebot.Event{
		PostType:    onebot.PostTypeMessage,
		MessageType: onebot.MessageTypePrivate,
		MessageID:   555,
		UserID:      88888,
		Sender:      onebot.Sender{Nickname: "Bob"},
		Message:     onebot.Message{{Type: onebot.SegmentText, Data: map[string]any{"text": "hi there"}}},
	})

	waitWithTimeout(t, &wg, "inbound message")
	if got.PeerKind != "direct" {
		t.Fatalf("expected direct peer, got %s", got.PeerKind)
	}
	if got.SenderID != "88888" || got.ChatID != "88888" {
		t.Fatalf("sender/chat mismatch: %+v", got)
	}
	if !strings.Contains(got.Content, "hi there") || !strings.Contains(got.Content, "Bob") {
		t.Fatalf("content not annotated with sender/text: %q", got.Content)
	}
	if got.Metadata["message_id"] != "555" {
		t.Fatalf("message_id metadata: %q", got.Metadata["message_id"])
	}
}

// TestInboundFlow_GroupNoMentionRecordsHistory verifies that a non-mentioned
// group message is NOT published (require_mention=true) — the handler returns
// early after recording to pending history.
func TestInboundFlow_GroupNoMentionDropped(t *testing.T) {
	ch, mb := newTestChannel(t, "10001", config.QQConfig{GroupPolicy: "open", RequireMention: boolPtr(true)})

	published := make(chan bus.InboundMessage, 1)
	mb.RegisterHandler("qq-test", func(m bus.InboundMessage) error {
		published <- m
		return nil
	})

	ch.handleEvent(onebot.Event{
		PostType:    onebot.PostTypeMessage,
		MessageType: onebot.MessageTypeGroup,
		GroupID:     4242,
		UserID:      88888,
		Sender:      onebot.Sender{Nickname: "Bob"},
		Message:     onebot.Message{{Type: onebot.SegmentText, Data: map[string]any{"text": "chatter"}}},
	})

	select {
	case <-published:
		t.Fatal("non-mentioned group message should not be published")
	case <-time.After(200 * time.Millisecond):
		// good — dropped (recorded to pending history instead)
	}
}

// TestInboundFlow_GroupMentionPublished verifies a mentioned group message
// flows through with peer=group.
func TestInboundFlow_GroupMentionPublished(t *testing.T) {
	ch, mb := newTestChannel(t, "10001", config.QQConfig{GroupPolicy: "open", RequireMention: boolPtr(true)})

	var got bus.InboundMessage
	var wg sync.WaitGroup
	wg.Add(1)
	mb.RegisterHandler("qq-test", func(m bus.InboundMessage) error {
		got = m
		wg.Done()
		return nil
	})

	ch.handleEvent(onebot.Event{
		PostType:    onebot.PostTypeMessage,
		MessageType: onebot.MessageTypeGroup,
		GroupID:     4242,
		UserID:      88888,
		Sender:      onebot.Sender{Nickname: "Bob"},
		Message: onebot.Message{
			{Type: onebot.SegmentAt, Data: map[string]any{"qq": "10001"}},
			{Type: onebot.SegmentText, Data: map[string]any{"text": " help me"}},
		},
	})

	waitWithTimeout(t, &wg, "mentioned group message")
	if got.PeerKind != "group" || got.ChatID != "4242" {
		t.Fatalf("expected group/4242, got peer=%s chat=%s", got.PeerKind, got.ChatID)
	}
	if !strings.Contains(got.Content, "help me") {
		t.Fatalf("content missing: %q", got.Content)
	}
}

// TestSendMessage_* lives in send_e2e_test.go (wires a Channel to a fake
// OneBot WebSocket implementation).

// --- helpers ---

func boolPtr(b bool) *bool { return &b }

func waitWithTimeout(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}
