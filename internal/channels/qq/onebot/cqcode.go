package onebot

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseCQCode parses a CQ-code string into message segments.
//
// CQ codes look like: "[CQ:at,qq=10001] 你好 [CQ:face,id=14]"
// Plain text outside CQ codes becomes "text" segments. Unknown CQ types are
// preserved as typed segments so no information is lost.
//
// We only parse inbound CQ strings (when an implementation reports the legacy
// string message form). Outbound messages always use the safer array form, so
// there is no encoder here — only EscapeCQText for sanitizing user text inside
// text segments.
func ParseCQCode(s string) Message {
	var segs Message
	var text strings.Builder

	i := 0
	for i < len(s) {
		// Find the next CQ code start.
		idx := strings.Index(s[i:], "[CQ:")
		if idx < 0 {
			text.WriteString(s[i:])
			break
		}
		// Flush text accumulated before the code.
		if idx > 0 {
			text.WriteString(s[i : i+idx])
		}
		start := i + idx + 4 // position after "[CQ:"
		end := strings.Index(s[start:], "]")
		if end < 0 {
			// Malformed (no closing ]); treat the rest as text.
			text.WriteString(s[i+idx:])
			break
		}
		body := s[start : start+end]
		i = start + end + 1

		// Flush any pending text segment before appending the code segment.
		if text.Len() > 0 {
			segs = append(segs, MessageSegment{Type: SegmentText, Data: map[string]any{"text": text.String()}})
			text.Reset()
		}

		parts := strings.SplitN(body, ",", 2)
		cqType := parts[0]
		data := map[string]any{}
		if len(parts) == 2 {
			for kv := range strings.SplitSeq(parts[1], ",") {
				if before, after, ok := strings.Cut(kv, "="); ok {
					k := before
					v := UnescapeCQText(after)
					data[k] = v
				} else {
					// flag-style param with no value
					data[kv] = ""
				}
			}
		}
		// Map common CQ type aliases to canonical segment types.
		switch cqType {
		case "image":
			data["type"] = "cq" // mark origin for callers that need it
		}
		segs = append(segs, MessageSegment{Type: cqType, Data: data})
	}

	if text.Len() > 0 {
		segs = append(segs, MessageSegment{Type: SegmentText, Data: map[string]any{"text": text.String()}})
	}
	return segs
}

// CQ code entities, per OneBot 11 spec. Text payloads (both in CQ-code params
// and outside codes) must be escaped so user content cannot forge new segments.
const (
	cqAmp = "&amp;"
	cqLBr = "&#91;" // [
	cqRBr = "&#93;" // ]
	cqCom = "&#44;" // ,
)

// EscapeCQText escapes plain text so it is safe to embed in a OneBot message.
// Used when serializing outbound text segments in CQ-string form; with the
// array form (which we use) it is still applied defensively so any code path
// that later flattens to a CQ string cannot be injected.
func EscapeCQText(s string) string {
	if !strings.ContainsAny(s, "&[],") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString(cqAmp)
		case '[':
			b.WriteString(cqLBr)
		case ']':
			b.WriteString(cqRBr)
		case ',':
			b.WriteString(cqCom)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// UnescapeCQText reverses EscapeCQText for inbound CQ param values.
func UnescapeCQText(s string) string {
	if !strings.Contains(s, "&") {
		return s
	}
	s = strings.ReplaceAll(s, cqAmp, "&")
	s = strings.ReplaceAll(s, cqLBr, "[")
	s = strings.ReplaceAll(s, cqRBr, "]")
	s = strings.ReplaceAll(s, cqCom, ",")
	return s
}

// SegmentTextValue reads the "text" field of a segment as a string. Returns
// "" for segments without text data or with a non-string value.
func SegmentTextValue(seg MessageSegment) string {
	if seg.Type != SegmentText {
		return ""
	}
	switch v := seg.Data["text"].(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return ""
	}
}

// SegmentQQ reads the "qq" field of an "at" segment as a string (QQ number).
func SegmentQQ(seg MessageSegment) string {
	switch v := seg.Data["qq"].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	}
	return ""
}
