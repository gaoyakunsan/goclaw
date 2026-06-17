package onebot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// EventHandler receives a parsed inbound OneBot event. Implementations must be
// safe to call from the client's read goroutine; slow work should be spawned
// onto its own goroutine so it never blocks the read loop.
type EventHandler func(evt Event)

// dialTimeout bounds the initial forward-WS handshake.
const dialTimeout = 15 * time.Second

// defaultCallTimeout bounds a single action request/response round trip.
const defaultCallTimeout = 30 * time.Second

// writeTimeout is the per-frame write deadline on the underlying WS.
const writeTimeout = 15 * time.Second

// Client is a single-connection OneBot 11 client.
//
// One Client maps to one QQ account (one self_id). It owns the active
// WebSocket connection, the event read loop, and the echo request/response
// correlation table. Both transport directions are supported:
//
//   - Reverse WS: the implementation connects in; the channel's HTTP handler
//     upgrades the request and hands the *websocket.Conn to AcceptReverse.
//   - Forward WS: the gateway dials out to ws://host:port via DialForward.
//
// At most one connection is active at a time; a new connection (reverse
// reconnect, or a forward re-dial) replaces and closes the previous one.
type Client struct {
	accessToken string
	handler     EventHandler

	// Active connection + read-loop lifecycle. connMu guards conn assignment
	// so Call() can check liveness without racing a reconnect. Writes to the
	// socket are serialized by writeMu so concurrent Send/Call never interleave
	// frames (gorilla/websocket does not support concurrent writers).
	connMu  sync.RWMutex
	conn    *websocket.Conn
	writeMu sync.Mutex

	// Echo request/response correlation. echoSeq is monotonic across the
	// client's lifetime so reconnects can't recycle a stale echo.
	echoSeq  int64
	pendingMu sync.Mutex
	pending   map[string]chan Response

	// onDisconnect fires once per dropped connection so the channel can trigger
	// a forward-mode reconnect or just mark health degraded. It receives the
	// error that ended the read loop (nil for a clean Close()).
	onDisconnect func(error)

	// Close coordination.
	closeOnce sync.Once
	closed    chan struct{}
}

// NewClient creates a Client. handler is invoked for every inbound event.
// accessToken is sent on the forward dial (Authorization + query); for reverse
// mode the channel's HTTP handler validates the token before handing the conn
// over, so the client itself does not re-check it.
func NewClient(accessToken string, handler EventHandler) *Client {
	return &Client{
		accessToken: accessToken,
		handler:     handler,
		pending:     make(map[string]chan Response),
		closed:      make(chan struct{}),
	}
}

// SetDisconnectHandler installs a callback invoked when the active connection's
// read loop exits. Must be set before DialForward/AcceptReverse.
func (c *Client) SetDisconnectHandler(fn func(error)) { c.onDisconnect = fn }

// IsConnected reports whether a connection is currently active.
func (c *Client) IsConnected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn != nil
}

// AcceptReverse takes ownership of a reverse-WS connection that the channel's
// HTTP handler already upgraded + authenticated. Any previously active
// connection is closed first so a reconnect cleanly replaces it. The read loop
// runs in the background and returns immediately.
func (c *Client) AcceptReverse(conn *websocket.Conn) error {
	if conn == nil {
		return errors.New("onebot: nil connection")
	}
	c.replaceConn(conn)
	go c.readLoop(conn)
	return nil
}

// DialForward connects out to the implementation's WS server and starts the
// read loop. Returns once the handshake completes; the read loop runs in the
// background. Caller drives reconnects by calling DialForward again on error.
func (c *Client) DialForward(ctx context.Context, rawURL string) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: dialTimeout,
	}
	// Auth: OneBot 11 accepts the token as a Bearer header OR ?access_token=.
	// Send both so we tolerate either server-side expectation.
	header := http.Header{}
	if c.accessToken != "" {
		header.Set("Authorization", "Bearer "+c.accessToken)
	}
	reqURL := rawURL
	if c.accessToken != "" && !containsQuery(rawURL, "access_token") {
		reqURL = appendQuery(rawURL, "access_token", c.accessToken)
	}

	conn, _, err := dialer.DialContext(ctx, reqURL, header)
	if err != nil {
		return fmt.Errorf("onebot: dial %s: %w", rawURL, err)
	}
	c.replaceConn(conn)
	go c.readLoop(conn)
	return nil
}

// replaceConn closes any active connection and installs conn as the new one.
// The previous read loop observes the closed socket and exits on its own.
func (c *Client) replaceConn(conn *websocket.Conn) {
	c.connMu.Lock()
	old := c.conn
	c.conn = conn
	c.connMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// readLoop drains frames from conn until the socket closes or Close() is
// called. Each frame is classified as an API response (has echo) or an event
// (has post_type) and dispatched accordingly. Panics in the event handler are
// isolated so a malformed event can't kill the read loop.
func (c *Client) readLoop(conn *websocket.Conn) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("onebot read loop panic", "panic", fmt.Sprint(rec))
		}
		// Drop the connection from the client so IsConnected flips false.
		c.connMu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.connMu.Unlock()
		_ = conn.Close()
		if c.onDisconnect != nil {
			c.onDisconnect(nil)
		}
	}()

	for {
		select {
		case <-c.closed:
			return
		default:
		}
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			// Normal closure or network drop — surface to the disconnect handler.
			select {
			case <-c.closed:
				return
			default:
			}
			if c.onDisconnect != nil {
				c.onDisconnect(err)
			}
			return
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		c.handleFrame(data)
	}
}

// frameDiscriminator tells a response (echo present) from an event (post_type).
type frameDiscriminator struct {
	Echo     string `json:"echo"`
	PostType string `json:"post_type"`
}

func (c *Client) handleFrame(data []byte) {
	// Some implementations send non-JSON heartbeat pings or empty acks; skip.
	var d frameDiscriminator
	if err := json.Unmarshal(data, &d); err != nil {
		slog.Debug("onebot: dropping non-JSON frame", "err", err)
		return
	}

	if d.Echo != "" {
		// API response — route to the waiting Call().
		var resp Response
		if err := json.Unmarshal(data, &resp); err != nil {
			slog.Warn("onebot: malformed response frame", "echo", d.Echo, "err", err)
			return
		}
		c.deliverResponse(resp)
		return
	}

	if d.PostType == "" {
		// Neither response nor event; ignore (implementation-specific keepalive).
		return
	}

	// Event — parse fully and dispatch.
	var evt Event
	if err := json.Unmarshal(data, &evt); err != nil {
		slog.Warn("onebot: malformed event frame", "post_type", d.PostType, "err", err)
		return
	}
	evt.Raw = append(evt.Raw[:0], data...) // retain raw for diagnostics

	if c.handler != nil {
		// Isolate handler panics from the read loop.
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("onebot event handler panic",
						"post_type", evt.PostType, "panic", fmt.Sprint(rec))
				}
			}()
			c.handler(evt)
		}()
	}
}

// deliverResponse routes a Response to the goroutine waiting on its echo.
func (c *Client) deliverResponse(resp Response) {
	c.pendingMu.Lock()
	ch, ok := c.pending[resp.Echo]
	if ok {
		delete(c.pending, resp.Echo)
	}
	c.pendingMu.Unlock()
	if !ok {
		// Late/duplicate response with no waiter — drop silently.
		return
	}
	// Non-blocking send: the waiter has its own timeout and may have given up.
	select {
	case ch <- resp:
	default:
	}
}

// Call sends an action and blocks until the matching response arrives or ctx
// expires. params is marshaled verbatim; pass nil for parameter-less actions.
// On a non-zero retcode the returned error wraps *ErrRetcode.
func (c *Client) Call(ctx context.Context, action string, params any) (Response, error) {
	conn := c.connSnapshot()
	if conn == nil {
		return Response{}, errors.New("onebot: not connected")
	}

	echo := c.nextEcho()
	rawParams, err := marshalParams(params)
	if err != nil {
		return Response{}, fmt.Errorf("onebot: marshal params: %w", err)
	}
	req := Request{Action: action, Params: rawParams, Echo: echo}

	// Register the waiter BEFORE writing so a fast response can't race us.
	ch := make(chan Response, 1)
	c.pendingMu.Lock()
	c.pending[echo] = ch
	c.pendingMu.Unlock()
	defer c.cancelWaiter(echo)

	if err := c.writeJSON(req); err != nil {
		return Response{}, fmt.Errorf("onebot: write %s: %w", action, err)
	}

	select {
	case resp := <-ch:
		if !resp.OK() {
			return resp, &ErrRetcode{Action: action, Code: resp.Retcode, Word: resp.Status}
		}
		return resp, nil
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case <-time.After(defaultCallTimeout):
		return Response{}, fmt.Errorf("onebot: %s timed out after %s", action, defaultCallTimeout)
	}
}

// Send is the fire-and-forget variant of Call for actions whose response we do
// not need (e.g. delete_msg during streaming approximation). Still uses the
// correlation table so a late error doesn't leak, but returns immediately.
func (c *Client) Send(ctx context.Context, action string, params any) error {
	conn := c.connSnapshot()
	if conn == nil {
		return errors.New("onebot: not connected")
	}
	rawParams, err := marshalParams(params)
	if err != nil {
		return fmt.Errorf("onebot: marshal params: %w", err)
	}
	// No echo → implementation treats it as a one-way call (OneBot 11 allows
	// omitting echo; no response is sent back).
	req := Request{Action: action, Params: rawParams}
	return c.writeJSON(req)
}

// connSnapshot returns the current connection under a read lock (may be nil).
func (c *Client) connSnapshot() *websocket.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

// nextEcho returns a unique correlation id.
func (c *Client) nextEcho() string {
	n := atomic.AddInt64(&c.echoSeq, 1)
	return fmt.Sprintf("goclaw-%d", n)
}

func (c *Client) cancelWaiter(echo string) {
	c.pendingMu.Lock()
	delete(c.pending, echo)
	c.pendingMu.Unlock()
}

// writeJSON serializes a frame to the active connection. writeMu guarantees
// frames don't interleave across concurrent Call/Send.
func (c *Client) writeJSON(v any) error {
	conn := c.connSnapshot()
	if conn == nil {
		return errors.New("onebot: not connected")
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// Re-check liveness under the lock: the conn may have been replaced/closed
	// between the snapshot and now.
	if c.connSnapshot() != conn {
		return errors.New("onebot: connection replaced during write")
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return conn.WriteMessage(websocket.TextMessage, payload)
}

// Close shuts down the client and the active connection. Idempotent. Pending
// Call waiters are released via their context/timeout after the socket closes.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		c.connMu.Lock()
		conn := c.conn
		c.conn = nil
		c.connMu.Unlock()
		if conn != nil {
			err = conn.Close()
		}
	})
	return err
}

// marshalParams marshals params, returning a small JSON object for nil/empty.
func marshalParams(params any) (json.RawMessage, error) {
	if params == nil {
		return json.RawMessage("{}"), nil
	}
	if raw, ok := params.(json.RawMessage); ok {
		if len(raw) == 0 {
			return json.RawMessage("{}"), nil
		}
		return raw, nil
	}
	b, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// --- URL helpers (avoids pulling net/url for two trivial operations) ---

func containsQuery(rawURL, key string) bool {
	idx := indexOfByte(rawURL, '?')
	if idx < 0 {
		return false
	}
	return containsSubstring(rawURL[idx:], key+"=")
}

func appendQuery(rawURL, key, val string) string {
	sep := byte('?')
	if indexOfByte(rawURL, '?') >= 0 {
		sep = '&'
	}
	return fmt.Sprintf("%s%c%s=%s", rawURL, sep, key, val)
}

func indexOfByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func containsSubstring(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
