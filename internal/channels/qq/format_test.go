package qq

import "testing"

func TestStripMarkdown(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"plain text unchanged", "hello world", "hello world"},

		// Bold & italic
		{"bold stars", "this is **bold** text", "this is bold text"},
		{"bold underscores", "this is __bold__ text", "this is bold text"},
		{"bold+italic stars", "***important***", "important"},
		{"strikethrough", "this is ~~deleted~~ text", "this is deleted text"},

		// Code
		{"inline code", "use `fmt.Println` here", "use fmt.Println here"},
		{"fenced code block", "before\n```go\nfmt.Println(\"hi\")\n```\nafter", "before\nfmt.Println(\"hi\")\n\nafter"},
		{"fenced code block no lang", "```\ncode here\n```", "code here"},

		// Links & images
		{"link", "click [here](https://example.com) now", "click here (https://example.com) now"},
		{"image", "see ![alt](https://img.png) below", "see  below"},

		// Headers
		{"h1", "# Title", "Title"},
		{"h3", "### Section", "Section"},
		{"h6", "###### Deep", "Deep"},

		// Horizontal rules
		{"hr dashes", "above\n---\nbelow", "above\n\nbelow"},
		{"hr stars", "above\n***\nbelow", "above\n\nbelow"},

		// Blockquotes
		{"blockquote", "> this is quoted\n> second line", "this is quoted\nsecond line"},

		// Bullets
		{"dash bullet", "- item one\n- item two", "• item one\n• item two"},
		{"star bullet", "* item one\n* item two", "• item one\n• item two"},
		{"indented bullet", "list:\n  - nested item", "list:\n  • nested item"},

		// Excessive newlines
		{"excessive newlines", "a\n\n\n\nb", "a\n\nb"},

		// Real-world regression: the LLM reply shape from the QQ bug report.
		// Bold headings + horizontal rules + emoji + ASCII tree must come out
		// as clean plain text (emoji/tree preserved, sigils gone).
		{
			"qq bug report shape",
			"---\n\n**🧩 系统架构思路**\n\n多智能体动态博弈系统\n├─ 🏦 银行智能体\n├─ 👤 用户智能体\n\n---\n\n**🔑 几个关键点：**\n\n1. **差异化定价维度**\n用户信用分层",
			"🧩 系统架构思路\n\n多智能体动态博弈系统\n├─ 🏦 银行智能体\n├─ 👤 用户智能体\n\n🔑 几个关键点：\n\n1. 差异化定价维度\n用户信用分层",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripMarkdown(tt.in)
			if got != tt.want {
				t.Errorf("stripMarkdown(%q)\n got: %q\nwant: %q", tt.in, got, tt.want)
			}
		})
	}
}
