package qq

import (
	"regexp"
	"strings"
)

// stripMarkdown removes Markdown formatting artifacts from LLM output so the
// result reads as clean plain text. QQ plain-text messages render no markup at
// all — raw sigils (**, #, ```, ---) would be shown verbatim to users — so we
// strip them before sending. Mirrors the plain-text handling in the
// Zalo/Pancake channels.
//
// Ordering in the send path matters: stripMarkdown runs FIRST (its regexes
// match Markdown link/emphasis syntax), then ChunkMarkdown splits the cleaned
// text, then EscapeCQText CQ-escapes the chunks. Escaping earlier would break
// the link/inline-code regexes. See send.go sendMessage.
func stripMarkdown(text string) string {
	if text == "" {
		return text
	}

	// 1. Fenced code blocks — keep content, drop the ``` delimiters.
	text = reFencedCode.ReplaceAllString(text, "$1")
	// 2. Inline code backticks.
	text = reInlineCode.ReplaceAllString(text, "$1")
	// 3. Images ![alt](url) — drop entirely.
	text = reImage.ReplaceAllString(text, "")
	// 4. Links [text](url) → "text (url)".
	text = reLink.ReplaceAllString(text, "$1 ($2)")
	// 5. Bold+italic (***text*** / ___text___).
	text = reBoldItalicStar.ReplaceAllString(text, "$1")
	text = reBoldItalicUnder.ReplaceAllString(text, "$1")
	// 6. Bold (**text** / __text__).
	text = reBoldStar.ReplaceAllString(text, "$1")
	text = reBoldUnder.ReplaceAllString(text, "$1")
	// 7. Strikethrough ~~text~~.
	text = reStrikethrough.ReplaceAllString(text, "$1")
	// 8. ATX headers (# text) — keep the text, drop the leading #'s.
	text = reHeader.ReplaceAllString(text, "$1")
	// 9. Horizontal rules (--- / *** / ___ on their own line).
	text = reHorizontalRule.ReplaceAllString(text, "")
	// 10. Blockquotes (> text) — keep the text.
	text = reBlockquote.ReplaceAllString(text, "$1")
	// 11. Bullet markers (- / * / +) → •.
	text = reBullet.ReplaceAllString(text, "${1}• ")
	// Collapse 3+ consecutive newlines down to a single blank line.
	text = reExcessiveNewlines.ReplaceAllString(text, "\n\n")

	return strings.TrimSpace(text)
}

var (
	reFencedCode        = regexp.MustCompile("(?s)```[a-zA-Z0-9]*\\n?(.*?)```")
	reInlineCode        = regexp.MustCompile("`([^`]+)`")
	reImage             = regexp.MustCompile(`!\[[^\]]*\]\([^)]+\)`)
	reLink              = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`)
	reBoldItalicStar    = regexp.MustCompile(`\*{3}(.+?)\*{3}`)
	reBoldItalicUnder   = regexp.MustCompile(`_{3}(.+?)_{3}`)
	reBoldStar          = regexp.MustCompile(`\*{2}(.+?)\*{2}`)
	reBoldUnder         = regexp.MustCompile(`_{2}(.+?)_{2}`)
	reStrikethrough     = regexp.MustCompile(`~~(.+?)~~`)
	reHeader            = regexp.MustCompile(`(?m)^#{1,6}\s+(.+)$`)
	reHorizontalRule    = regexp.MustCompile(`(?m)^[\s]*[-*_]{3,}[\s]*$`)
	reBlockquote        = regexp.MustCompile(`(?m)^>\s?(.*)$`)
	reBullet            = regexp.MustCompile(`(?m)^(\s*)[-*+]\s+`)
	reExcessiveNewlines = regexp.MustCompile(`\n{3,}`)
)
