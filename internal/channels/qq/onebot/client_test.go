package onebot

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
)

// fakeImpl is a minimal OneBot implementation: it accepts a reverse-WS
// connection, echoes back action responses keyed on the request echo, and
// records the last action it saw so tests can assert on it.
type fakeImpl struct {
	mu        sync.Mutex
	conn      *websocket.Conn
	lastReq   Request
	replyData json.RawMessage // data payload to return for the next Call
	replyCode int
	gotReq    chan Request
}

func newFakeImpl() *fakeImpl {
	return &fakeImpl{gotReq: make(chan Request, 8)}
}

func (f *fakeImpl) handler(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.conn = c
	f.mu.Unlock()
	defer c.Close()
	for {
		mt, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		if mt != websocket.TextMessage {
			continue
		}
		var req Request
		if err := json.Unmarshal(data, &req); err != nil {
			continue
		}
		f.mu.Lock()
		f.lastReq = req
		rc := f.replyCode
		payload := f.replyData
		f.mu.Unlock()
		select {
		case f.gotReq <- req:
		default:
		}
		// Reply with a response carrying the same echo.
		resp := Response{Echo: req.Echo, Retcode: rc, Data: payload}
		if rc == 0 {
			resp.Status = "ok"
		} else {
			resp.Status = "failed"
		}
		out, _ := json.Marshal(resp)
		_ = c.WriteMessage(websocket.TextMessage, out)
	}
}

func (f *fakeImpl) setReply(code int, data json.RawMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replyCode = code
	f.replyData = data
}

// sendEvent pushes an event frame to the connected client.
func (f *fakeImpl) sendEvent(t *testing.T, evt Event) {
	t.Helper()
	f.mu.Lock()
	c := f.conn
	f.mu.Unlock()
	if c == nil {
		t.Fatal("fake impl not connected")
	}
	out, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if err := c.WriteMessage(websocket.TextMessage, out); err != nil {
		t.Fatalf("write event: %v", err)
	}
}

func (f *fakeImpl) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		_ = f.conn.Close()
	}
}

// dialClient wires a Client to the fake impl via forward dial.
func dialClient(t *testing.T, f *fakeImpl, handler EventHandler) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	cli := NewClient("tok", handler)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.DialForward(ctx, wsURL); err != nil {
		t.Fatalf("dial: %v", err)
	}
	return cli, srv
}

func waitForConn(cli *Client) bool {
	for range 100 {
		if cli.IsConnected() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestClient_CallEchoCorrelation(t *testing.T) {
	f := newFakeImpl()
	t.Cleanup(f.close)
	f.setReply(0, json.RawMessage(`{"message_id":999}`))

	cli, _ := dialClient(t, f, nil)
	t.Cleanup(func() { _ = cli.Close() })
	if !waitForConn(cli) {
		t.Fatal("client never connected")
	}

	// Action call → fake impl echoes with matching echo → response returns.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Call(ctx, ActionSendGroupMsg, map[string]any{"group_id": 123, "message": []MessageSegment{{Type: SegmentText, Data: map[string]any{"text": "hi"}}}})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	var mid MessageIDData
	if err := json.Unmarshal(resp.Data, &mid); err != nil {
		t.Fatalf("decode message_id: %v", err)
	}
	if mid.MessageID != 999 {
		t.Fatalf("expected message_id 999, got %d", mid.MessageID)
	}
}

func TestClient_CallRetcodeError(t *testing.T) {
	f := newFakeImpl()
	t.Cleanup(f.close)
	f.setReply(1, json.RawMessage(`{}`))

	cli, _ := dialClient(t, f, nil)
	t.Cleanup(func() { _ = cli.Close() })
	if !waitForConn(cli) {
		t.Fatal("client never connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Call(ctx, "some_action", nil)
	if err == nil {
		t.Fatal("expected retcode error")
	}
	if !strings.Contains(err.Error(), "retcode=1") {
		t.Fatalf("error should mention retcode: %v", err)
	}
}

func TestClient_EventDispatch(t *testing.T) {
	f := newFakeImpl()
	t.Cleanup(f.close)

	var gotEvent Event
	var wg sync.WaitGroup
	wg.Add(1)
	cli, _ := dialClient(t, f, func(evt Event) { gotEvent = evt; wg.Done() })
	t.Cleanup(func() { _ = cli.Close() })
	if !waitForConn(cli) {
		t.Fatal("client never connected")
	}

	// Push a group message event from the impl side.
	f.sendEvent(t, Event{
		PostType:    PostTypeMessage,
		MessageType: MessageTypeGroup,
		GroupID:     42,
		UserID:      7,
		MessageID:   1,
		Message:     Message{{Type: SegmentText, Data: map[string]any{"text": "hello"}}},
	})

	waitWithTimeout(t, &wg, "event handler")
	if gotEvent.PostType != PostTypeMessage || gotEvent.GroupID != 42 {
		t.Fatalf("unexpected event: %+v", gotEvent)
	}
}

func TestClient_DisconnectHandler(t *testing.T) {
	f := newFakeImpl()
	t.Cleanup(f.close)

	disconnected := make(chan error, 1)
	cli := NewClient("tok", nil)
	cli.SetDisconnectHandler(func(err error) { disconnected <- err })

	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.DialForward(ctx, wsURL); err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	if !waitForConn(cli) {
		t.Fatal("client never connected")
	}

	// Drop the connection from the impl side.
	f.close()

	select {
	case <-disconnected:
		// good
	case <-time.After(3 * time.Second):
		t.Fatal("disconnect handler never fired")
	}
}

func TestClient_AcceptReverse(t *testing.T) {
	// Reverse mode: the fake impl (client of the WS) dials the gateway's
	// handler. Here we flip roles — a test HTTP server acts as the gateway's
	// reverse endpoint, and we feed the upgraded conn into AcceptReverse.
	cli := NewClient("", nil)
	t.Cleanup(func() { _ = cli.Close() })

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if err := cli.AcceptReverse(c); err != nil {
			t.Errorf("accept reverse: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	// Dial as the "implementation" would.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if !waitForConn(cli) {
		t.Fatal("reverse connection not accepted")
	}
}

// waitWithTimeout fails the test if wg doesn't reach zero within a window.
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
