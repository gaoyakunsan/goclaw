// Package onebot implements a lightweight OneBot 11 client over WebSocket.
//
// OneBot 11 is the de-facto community protocol for QQ bots. Protocol
// implementations (NapCat / Lagrange.Core / LLOneBot / go-cqhttp) connect to
// the gateway and exchange JSON frames: events flow implementation→gateway,
// actions (request/response) flow gateway→implementation.
//
// This package is transport-only: it owns the WebSocket lifecycle, event
// dispatch, and the `echo` request/response correlation. QQ-specific mapping
// to the message bus lives in the parent qq package.
package onebot

import (
	"encoding/json"
	"fmt"
)

// PostType values for the top-level event discriminator.
const (
	PostTypeMessage    = "message"
	PostTypeNotice     = "notice"
	PostTypeRequest    = "request"
	PostTypeMetaEvent  = "meta_event"
	PostTypeSent       = "message_sent" // messages echoed from the bot's own account
)

// MessageType values (message events).
const (
	MessageTypePrivate = "private"
	MessageTypeGroup   = "group"
)

// MetaEventType values (meta_event).
const (
	MetaEventLifecycle = "lifecycle"
	MetaEventHeartbeat = "heartbeat"
)

// NoticeType values we care about.
const (
	NoticeGroupUpload = "group_upload" // group file upload
)

// MessageSegmentType values for message segments.
const (
	SegmentText   = "text"
	SegmentAt     = "at"
	SegmentImage  = "image"
	SegmentReply  = "reply"
	SegmentRecord = "record" // voice
	SegmentFile   = "file"
	SegmentFace   = "face"
)

// MessageSegment is a single segment of a OneBot message.
// `Data` is intentionally map[string]any because segments carry heterogeneous
// payloads (text: {text}; at: {qq}; image: {file,url}; etc.).
type MessageSegment struct {
	Type string            `json:"type"`
	Data map[string]any    `json:"data"`
}

// Message is a OneBot message: an ordered list of segments.
// It implements json.Unmarshaler so the wire form — which may be a JSON array
// of segments OR a CQ-code string — normalizes to []MessageSegment.
type Message []MessageSegment

// UnmarshalJSON accepts both the array form (preferred) and the CQ-code string
// form (legacy). The string form is parsed into segments so downstream code
// never has to branch on representation.
func (m *Message) UnmarshalJSON(b []byte) error {
	// Array form: [{"type":"text","data":{"text":"hi"}}, ...]
	var segs []MessageSegment
	if err := json.Unmarshal(b, &segs); err == nil {
		*m = segs
		return nil
	}
	// String form (CQ code): "[CQ:at,qq=10001] hi"
	var raw string
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("onebot: message must be array or string: %w", err)
	}
	*m = ParseCQCode(raw)
	return nil
}

// Event is a flat representation of a OneBot 11 event.
//
// OneBot events are a tagged union keyed on `post_type`, then a secondary key
// (message_type / notice_type / request_type / meta_event_type). Rather than
// model each variant as a distinct type, we flatten every possible field onto
// one struct with omitempty. Field names do not collide across variants, and
// the flat shape makes the QQ handler's switch-on-post_type straightforward.
//
// `Message` is the only field needing custom decoding (array vs CQ string);
// see Message.UnmarshalJSON.
type Event struct {
	// Common envelope
	PostType string `json:"post_type"`
	Time     int64  `json:"time"`
	SelfID   int64  `json:"self_id"`

	// message / message_sent
	MessageType string  `json:"message_type"`
	SubType     string  `json:"sub_type"`
	MessageID   int64   `json:"message_id"`
	UserID      int64   `json:"user_id"`
	GroupID     int64   `json:"group_id"`
	Message     Message `json:"message"`
	RawMessage  string  `json:"raw_message"`
	Font        int64   `json:"font"`
	Sender      Sender  `json:"sender"`

	// notice
	NoticeType string       `json:"notice_type"`
	File       *NoticeFile  `json:"file,omitempty"`

	// meta_event
	MetaEventType string `json:"meta_event_type"`
	Status        json.RawMessage `json:"status,omitempty"` // heartbeat status object (interval, online, etc.)

	// request (friend/group join) — not handled by QQ channel, retained for completeness
	RequestType string `json:"request_type"`
	Comment     string `json:"comment"`
	Flag        string `json:"flag"`

	// Raw payload retained for diagnostics and fields not modeled above.
	Raw json.RawMessage `json:"-"`
}

// NoticeFile describes a file in a group_upload notice.
type NoticeFile struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	URL   string `json:"url"`
	BusID int64  `json:"busid"`
}

// Sender is the sender sub-object on a message event.
type Sender struct {
	UserID   int64  `json:"user_id"`
	Nickname string `json:"nickname"`
	Card     string `json:"card"` // group card / remark (display name in groups)
	Area     string `json:"area"`
	Level    string `json:"level"`
	Role     string `json:"role"` // owner / admin / member
	Title    string `json:"title"`
}

// DisplayName returns the best available human-readable name for the sender.
// In groups the card (group nickname) takes priority; falls back to nickname.
func (s Sender) DisplayName() string {
	if s.Card != "" {
		return s.Card
	}
	if s.Nickname != "" {
		return s.Nickname
	}
	return ""
}

// --- Action request / response ---

// Request is an outbound action call. `Echo` is set by the client for
// request/response correlation; the implementation echoes it back verbatim.
type Request struct {
	Action string          `json:"action"`
	Params json.RawMessage `json:"params"`
	Echo   string          `json:"echo"`
}

// Response is an inbound action result.
type Response struct {
	Status string          `json:"status"` // "ok" | "async" | "failed"
	Retcode int            `json:"retcode"`
	Data   json.RawMessage `json:"data"`
	Echo   string          `json:"echo"`
}

// OK reports whether the response indicates success (retcode 0).
func (r Response) OK() bool { return r.Retcode == 0 }

// MessageIDData is the data payload of send_private_msg / send_group_msg.
type MessageIDData struct {
	MessageID int64 `json:"message_id"`
}

// LoginInfoData is the data payload of get_login_info.
type LoginInfoData struct {
	UserID   int64  `json:"user_id"`
	Nickname string `json:"nickname"`
}

// GroupMember is a single entry in get_group_member_list data.
type GroupMember struct {
	UserID   int64  `json:"user_id"`
	Nickname string `json:"nickname"`
	Card     string `json:"card"`
	Role     string `json:"role"`
}

// ErrRetcode is returned by Call when the implementation replies with a
// non-zero retcode. Callers can errors.As to inspect the code.
type ErrRetcode struct {
	Action string
	Code   int
	Word   string // status word when present
}

func (e *ErrRetcode) Error() string {
	return fmt.Sprintf("onebot action %q failed: retcode=%d status=%s", e.Action, e.Code, e.Word)
}
