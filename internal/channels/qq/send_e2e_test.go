package qq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels/qq/onebot"
	"github.com/nextlevelbuilder/goclaw/internal/config"
)

// fakeOneBot is an in-test OneBot implementation: it upgrades an inbound WS
// (reverse mode) and records every action request the channel sends.
type fakeOneBot struct {
	mu       sync.Mutex
	conn     *websocket.Conn
	requests []onebot.Request
	gotReq   chan onebot.Request
}

func newFakeOneBot() *fakeOneBot {
	return &fakeOneBot{gotReq: make(chan onebot.Request, 16)}
}

func (f *fakeOneBot) handler(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{}
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.conn = c
	f.mu.Unlock()
	defer c.Close()
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		var req onebot.Request
		if json.Unmarshal(data, &req) == nil {
			f.mu.Lock()
			f.requests = append(f.requests, req)
			f.mu.Unlock()
			select {
			case f.gotReq <- req:
			default:
			}
			// Reply ok with a message_id so Call() returns successfully.
			if req.Echo != "" {
				resp := onebot.Response{Echo: req.Echo, Retcode: 0, Status: "ok",
					Data: json.RawMessage(`{"message_id":777}`)}
				out, _ := json.Marshal(resp)
				_ = c.WriteMessage(websocket.TextMessage, out)
			}
		}
	}
}

func (f *fakeOneBot) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		_ = f.conn.Close()
	}
}

// waitForAction drains background requests (e.g. the get_login_info probe) and
// returns the first request matching the given action.
func (f *fakeOneBot) waitForAction(t *testing.T, action string, d time.Duration) onebot.Request {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for action %s", action)
		}
		select {
		case req := <-f.gotReq:
			if req.Action == action {
				return req
			}
			// background request (e.g. probe) — keep draining
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for action %s", action)
		}
	}
}

// waitForAnySend drains until a send_* action arrives, returning it. Returns
// false if none arrives within the window (for NO_REPLY assertions).
func (f *fakeOneBot) waitForAnySend(d time.Duration) (onebot.Request, bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		select {
		case req := <-f.gotReq:
			if strings.HasPrefix(req.Action, "send_") || req.Action == onebot.ActionDeleteMsg {
				return req, true
			}
		case <-time.After(remaining):
			return onebot.Request{}, false
		}
	}
	return onebot.Request{}, false
}

// connectChannelReverse mounts the channel's reverse-WS WebhookHandler on a
// test HTTP server, then dials in as the OneBot implementation would. Returns
// the wired channel + fake + the dialed conn (caller closes on cleanup).
func connectChannelReverse(t *testing.T, cfg config.QQConfig, accessToken string) (*Channel, *fakeOneBot) {
	t.Helper()
	cfg = applyDefaults(cfg)
	cfg.Direction = "reverse"
	mb := bus.New()
	ch, err := New(cfg, qqCreds{SelfID: "10001", AccessToken: accessToken}, mb, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ch.SetName("qq-rev")
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })

	path, handler := ch.WebhookHandler()
	if path == "" || handler == nil {
		t.Fatal("reverse mode must expose a webhook handler")
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	// Dial the full per-instance path as the implementation would.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + path
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	hdr := http.Header{}
	if accessToken != "" {
		hdr.Set("Authorization", "Bearer "+accessToken)
	}
	conn, resp, err := dialer.Dial(wsURL, hdr)
	if err != nil {
		body := ""
		if resp != nil {
			body = resp.Status
		}
		t.Fatalf("fake impl dial %s: %v (status %s)", wsURL, err, body)
	}

	// Wire the dialed conn into a fakeOneBot so it reads requests + replies.
	f := newFakeOneBot()
	f.mu.Lock()
	f.conn = conn
	f.mu.Unlock()
	go f.serveConn(conn)
	t.Cleanup(f.close)
	t.Cleanup(func() { _ = conn.Close() })

	// Wait until the channel's client has accepted the reverse connection.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cli := ch.Client(); cli != nil && cli.IsConnected() {
			return ch, f
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("reverse connection not accepted by channel")
	return ch, f
}

// serveConn reads requests off an already-upgraded conn, records them, and
// replies with ok+message_id (mirrors fakeOneBot.handler without the upgrade).
func (f *fakeOneBot) serveConn(c *websocket.Conn) {
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		var req onebot.Request
		if json.Unmarshal(data, &req) != nil {
			continue
		}
		f.mu.Lock()
		f.requests = append(f.requests, req)
		f.mu.Unlock()
		select {
		case f.gotReq <- req:
		default:
		}
		if req.Echo != "" {
			resp := onebot.Response{Echo: req.Echo, Retcode: 0, Status: "ok",
				Data: json.RawMessage(`{"message_id":777}`)}
			out, _ := json.Marshal(resp)
			_ = c.WriteMessage(websocket.TextMessage, out)
		}
	}
}

// connectChannelForward dials out (forward mode) from the channel to the fake.
func connectChannelForward(t *testing.T, cfg config.QQConfig) (*Channel, *fakeOneBot) {
	t.Helper()
	f := newFakeOneBot()
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(srv.Close)
	t.Cleanup(f.close)

	cfg = applyDefaults(cfg)
	cfg.Direction = "forward"
	cfg.Endpoint = "ws" + strings.TrimPrefix(srv.URL, "http")

	mb := bus.New()
	ch, err := New(cfg, qqCreds{SelfID: "10001"}, mb, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ch.SetName("qq-test")
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })

	// Wait for the forward dial to connect.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cli := ch.Client(); cli != nil && cli.IsConnected() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if cli := ch.Client(); cli == nil || !cli.IsConnected() {
		t.Fatal("forward dial never connected")
	}
	return ch, f
}

func TestSendE2E_PrivateText(t *testing.T) {
	ch, f := connectChannelForward(t, config.QQConfig{})

	// Seed the peer kind so Send knows 88888 is a private chat.
	ch.peerKind.Store("88888", "direct")

	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: "qq-test", ChatID: "88888", Content: "hello world",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	req := f.waitForAction(t, onebot.ActionSendPrivateMsg, 3*time.Second)
	var params map[string]any
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("decode params: %v", err)
	}
	if id, _ := params["user_id"].(float64); id != 88888 {
		t.Fatalf("user_id: %v", params["user_id"])
	}
	msg, _ := params["message"].([]any)
	if len(msg) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(msg))
	}
	seg := msg[0].(map[string]any)
	if seg["type"] != "text" {
		t.Fatalf("segment type: %v", seg["type"])
	}
	data := seg["data"].(map[string]any)
	if data["text"] != "hello world" {
		t.Fatalf("text: %v", data["text"])
	}
}

func TestSendE2E_GroupText(t *testing.T) {
	ch, f := connectChannelForward(t, config.QQConfig{})

	ch.peerKind.Store("4242", "group")

	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: "qq-test", ChatID: "4242", Content: "group reply",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	req := f.waitForAction(t, onebot.ActionSendGroupMsg, 3*time.Second)
	_ = req
}

func TestSendE2E_NoReplySuppressed(t *testing.T) {
	ch, f := connectChannelForward(t, config.QQConfig{})

	// Empty content + no media → NO_REPLY, no send.
	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: "qq-test", ChatID: "88888", Content: "",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if _, sent := f.waitForAnySend(300 * time.Millisecond); sent {
		t.Fatal("NO_REPLY should not produce a send OneBot request")
	}
}

func TestSendE2E_CQEscape(t *testing.T) {
	ch, f := connectChannelForward(t, config.QQConfig{})

	ch.peerKind.Store("88888", "direct")
	// Malicious text that would forge a CQ segment if not escaped.
	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: "qq-test", ChatID: "88888", Content: "[CQ:image,file=http://evil/x.png]",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	req := f.waitForAction(t, onebot.ActionSendPrivateMsg, 3*time.Second)
	var params map[string]any
	_ = json.Unmarshal(req.Params, &params)
	msg := params["message"].([]any)
	seg := msg[0].(map[string]any)
	if seg["type"] != "text" {
		t.Fatalf("must stay a single text segment, got type %v (CQ injection!)", seg["type"])
	}
}

func TestSendE2E_Chunking(t *testing.T) {
	ch, f := connectChannelForward(t, config.QQConfig{ChunkLimit: 10})

	ch.peerKind.Store("88888", "direct")
	long := strings.Repeat("A", 25) // forces ≥3 chunks at limit 10
	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: "qq-test", ChatID: "88888", Content: long,
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Count send_* requests over a short window (draining the probe).
	count := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case req := <-f.gotReq:
			if strings.HasPrefix(req.Action, "send_") {
				count++
			}
		case <-time.After(200 * time.Millisecond):
		}
		if count >= 3 {
			break
		}
	}
	if count < 3 {
		t.Fatalf("expected ≥3 chunked sends, got %d", count)
	}
}

// TestReverseE2E_SendAndReceive exercises the default reverse-WS mode: the
// implementation dials in, the channel accepts, and a Send round-trips through
// send_private_msg. Also confirms the per-instance reverse path is exposed.
func TestReverseE2E_Send(t *testing.T) {
	ch, f := connectChannelReverse(t, config.QQConfig{}, "")

	ch.peerKind.Store("88888", "direct")
	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: "qq-rev", ChatID: "88888", Content: "via reverse",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	req := f.waitForAction(t, onebot.ActionSendPrivateMsg, 3*time.Second)
	var params map[string]any
	_ = json.Unmarshal(req.Params, &params)
	if id, _ := params["user_id"].(float64); id != 88888 {
		t.Fatalf("user_id: %v", params["user_id"])
	}
}

// TestReverseE2E_TokenRejected verifies the access_token gate on the reverse
// WS upgrade: a connection without the correct token is refused with 401.
func TestReverseE2E_TokenRejected(t *testing.T) {
	cfg := config.QQConfig{}
	mb := bus.New()
	ch, err := New(applyDefaults(cfg), qqCreds{SelfID: "10001", AccessToken: "secret"}, mb, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ch.SetName("qq-token")
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })

	path, handler := ch.WebhookHandler()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	// Dial WITHOUT the token → expect non-101 (401) response.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + path
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	_, resp, err := dialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected dial to fail without access token")
	}
	if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

// TestReverseE2E_TokenAccepted verifies a correct token lets the connection through.
func TestReverseE2E_TokenAccepted(t *testing.T) {
	ch, _ := connectChannelReverse(t, config.QQConfig{}, "secret")
	if cli := ch.Client(); cli == nil || !cli.IsConnected() {
		t.Fatal("token-accepted reverse connection not active")
	}
}
