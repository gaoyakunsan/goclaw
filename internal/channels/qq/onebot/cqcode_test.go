package onebot

import (
	"encoding/json"
	"testing"
)

func TestParseCQCode_TextOnly(t *testing.T) {
	msg := ParseCQCode("hello world")
	if len(msg) != 1 || msg[0].Type != SegmentText {
		t.Fatalf("expected single text segment, got %+v", msg)
	}
	if got := msg[0].Data["text"]; got != "hello world" {
		t.Fatalf("text mismatch: %v", got)
	}
}

func TestParseCQCode_AtAndText(t *testing.T) {
	// "[CQ:at,qq=10001] 你好" → at segment + text " 你好"
	msg := ParseCQCode("[CQ:at,qq=10001] 你好")
	if len(msg) != 2 {
		t.Fatalf("expected 2 segments, got %d: %+v", len(msg), msg)
	}
	if msg[0].Type != SegmentAt || msg[0].Data["qq"] != "10001" {
		t.Fatalf("at segment wrong: %+v", msg[0])
	}
	if msg[1].Type != SegmentText || msg[1].Data["text"] != " 你好" {
		t.Fatalf("text segment wrong: %+v", msg[1])
	}
}

func TestParseCQCode_EscapedParams(t *testing.T) {
	// CQ params escape "[" as "&#91;" — round-trip through unescape. Spaces
	// between CQ codes become their own text segments; we only assert on the
	// typed segments + the unescape behavior.
	msg := ParseCQCode("[CQ:reply,id=42] [CQ:at,qq=5] hi [CQ:image,file=a&#91;b,url=http://x]")

	var img, at, reply MessageSegment
	for _, seg := range msg {
		switch seg.Type {
		case "image":
			img = seg
		case "at":
			at = seg
		case "reply":
			reply = seg
		}
	}
	if reply.Data["id"] != "42" {
		t.Fatalf("reply id not parsed: %+v", reply)
	}
	if SegmentQQ(at) != "5" {
		t.Fatalf("at qq not parsed: %+v", at)
	}
	if img.Type != "image" {
		t.Fatalf("expected an image segment in %v", msg)
	}
	if img.Data["file"] != "a[b" {
		t.Fatalf("escaped file not unescaped: %v", img.Data["file"])
	}
}

func TestParseCQCode_MalformedNoClose(t *testing.T) {
	// Missing "]" → rest treated as text, no panic.
	msg := ParseCQCode("[CQ:at,qq=1 hello")
	if len(msg) != 1 || msg[0].Type != SegmentText {
		t.Fatalf("expected single text fallback, got %+v", msg)
	}
}

func TestEscapeCQText_RoundTrip(t *testing.T) {
	cases := []string{
		"plain text",
		"with [brackets] and , commas & ampersand",
		"[CQ:at,qq=10001]",
		"",
	}
	for _, in := range cases {
		escaped := EscapeCQText(in)
		// Text that contains no special chars is returned as-is.
		if in == "plain text" && escaped != in {
			t.Fatalf("plain text should pass through: %q", escaped)
		}
		got := UnescapeCQText(escaped)
		if got != in {
			t.Fatalf("round-trip mismatch for %q: got %q", in, got)
		}
	}
}

func TestEscapeCQText_PreventsInjection(t *testing.T) {
	// User text must not be able to forge a new CQ segment when flattened.
	malicious := "[CQ:image,file=http://evil.com/x.png]"
	escaped := EscapeCQText(malicious)
	if ParseCQCode(escaped)[0].Type == "image" {
		t.Fatalf("injection succeeded: %q parsed to image", escaped)
	}
}

func TestMessage_UnmarshalJSON_Array(t *testing.T) {
	raw := `[{"type":"text","data":{"text":"hi"}},{"type":"at","data":{"qq":"10001"}}]`
	var m Message
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(m) != 2 || m[0].Type != SegmentText || m[1].Type != SegmentAt {
		t.Fatalf("unexpected message: %+v", m)
	}
}

func TestMessage_UnmarshalJSON_CQString(t *testing.T) {
	raw := `"[CQ:at,qq=10001] 你好"`
	var m Message
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("expected 2 segments from CQ string, got %d", len(m))
	}
	if m[0].Type != SegmentAt || SegmentQQ(m[0]) != "10001" {
		t.Fatalf("at segment wrong: %+v", m[0])
	}
}

func TestSegmentTextValue_NonText(t *testing.T) {
	if SegmentTextValue(MessageSegment{Type: SegmentAt}) != "" {
		t.Fatal("at segment should have empty text value")
	}
	seg := MessageSegment{Type: SegmentText, Data: map[string]any{"text": "hello"}}
	if got := SegmentTextValue(seg); got != "hello" {
		t.Fatalf("text value: %q", got)
	}
}

func TestSegmentQQ_NumberAndString(t *testing.T) {
	// JSON numbers unmarshal into map[string]any as float64.
	if got := SegmentQQ(MessageSegment{Data: map[string]any{"qq": float64(10001)}}); got != "10001" {
		t.Fatalf("float qq: %q", got)
	}
	if got := SegmentQQ(MessageSegment{Data: map[string]any{"qq": "10001"}}); got != "10001" {
		t.Fatalf("string qq: %q", got)
	}
	if got := SegmentQQ(MessageSegment{Data: map[string]any{}}); got != "" {
		t.Fatalf("missing qq: %q", got)
	}
}

func TestResponse_OK(t *testing.T) {
	if !(Response{Retcode: 0}).OK() {
		t.Fatal("retcode 0 should be OK")
	}
	if (Response{Retcode: 1}).OK() {
		t.Fatal("retcode 1 should not be OK")
	}
}

func TestErrRetcode_Message(t *testing.T) {
	e := &ErrRetcode{Action: ActionSendGroupMsg, Code: 1, Word: "failed"}
	if e.Error() == "" {
		t.Fatal("error message empty")
	}
}
