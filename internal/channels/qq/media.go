package qq

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels/qq/onebot"
)

const (
	base64Scheme = "base64://"
	fileScheme   = "file://"
	urlScheme    = "http"
	downloadTTL  = 5 * time.Minute
)

var mediaHTTPClient = &http.Client{
	Timeout: downloadTTL,
}

// resolveInboundMedia scans a message's media segments (image / record / file),
// downloads each into a temp file, and returns MediaFile entries + a compact
// "[media: ...]" tag string for the agent prompt. Best-effort: a failed
// download is logged and skipped so one bad attachment can't drop a message.
func (c *Channel) resolveInboundMedia(evt onebot.Event) ([]bus.MediaFile, string) {
	var files []bus.MediaFile
	var tags []string
	maxBytes := c.cfg.MediaMaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMediaMaxBytes
	}

	for _, seg := range evt.Message {
		var kind string
		switch seg.Type {
		case onebot.SegmentImage:
			kind = "image"
		case onebot.SegmentRecord:
			kind = "voice"
		case onebot.SegmentFile:
			kind = "file"
		default:
			continue
		}

		path, mime, name, ok := c.downloadSegment(seg, maxBytes)
		if !ok {
			tags = append(tags, "[media: "+kind+" (download failed)]")
			continue
		}
		files = append(files, bus.MediaFile{Path: path, MimeType: mime, Filename: name})
		if kind == "voice" {
			tags = append(tags, "[media: voice message]")
		} else {
			tags = append(tags, fmt.Sprintf("[media: %s]", kind))
		}
	}
	return files, strings.Join(tags, "\n")
}

// downloadSegment fetches a single media segment to a temp file. OneBot
// encodes media as either an http(s) URL, a base64:// inline blob, or a local
// file path; we accept all three.
func (c *Channel) downloadSegment(seg onebot.MessageSegment, maxBytes int64) (path, mime, name string, ok bool) {
	// Name (filename) if provided.
	if n, _ := seg.Data["file"].(string); n != "" {
		name = n
	}
	if n, _ := seg.Data["filename"].(string); n != "" {
		name = n
	}

	// Prefer URL, then base64, then local file.
	if u, _ := seg.Data["url"].(string); u != "" {
		return c.downloadURL(u, maxBytes, name)
	}
	if b64, _ := seg.Data["file"].(string); strings.HasPrefix(b64, base64Scheme) {
		return decodeBase64Media(b64[len(base64Scheme):], maxBytes, name)
	}
	// Some implementations expose the raw bytes via "file" as a local path.
	if p, _ := seg.Data["file"].(string); p != "" && !strings.Contains(p, "://") {
		return copyLocalMedia(p, maxBytes, name)
	}
	return "", "", "", false
}

// downloadURL fetches an http(s) media URL into a bounded temp file.
func (c *Channel) downloadURL(url string, maxBytes int64, name string) (string, string, string, bool) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		slog.Debug("qq media download: bad url", "url", url, "error", err)
		return "", "", "", false
	}
	resp, err := mediaHTTPClient.Do(req)
	if err != nil {
		slog.Debug("qq media download failed", "url", url, "error", err)
		return "", "", "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Debug("qq media download bad status", "url", url, "status", resp.StatusCode)
		return "", "", "", false
	}

	ext := extFor(name, url)
	tmp, err := os.CreateTemp("", "goclaw_qq_*"+ext)
	if err != nil {
		return "", "", "", false
	}
	written, err := io.Copy(tmp, io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", "", "", false
	}
	tmp.Close()
	if written > maxBytes {
		os.Remove(tmp.Name())
		slog.Warn("qq media too large; dropped", "url", url, "size", written, "limit", maxBytes)
		return "", "", "", false
	}
	return tmp.Name(), resp.Header.Get("Content-Type"), orName(name, filepath.Base(tmp.Name())), true
}

// decodeBase64Media writes an inline base64 blob into a temp file.
func decodeBase64Media(b64 string, maxBytes int64, name string) (string, string, string, bool) {
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		slog.Debug("qq media base64 decode failed", "error", err)
		return "", "", "", false
	}
	if int64(len(data)) > maxBytes {
		slog.Warn("qq media too large; dropped", "size", len(data), "limit", maxBytes)
		return "", "", "", false
	}
	ext := extFor(name, "")
	tmp, err := os.CreateTemp("", "goclaw_qq_*"+ext)
	if err != nil {
		return "", "", "", false
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", "", "", false
	}
	tmp.Close()
	return tmp.Name(), mimeForExt(ext), orName(name, filepath.Base(tmp.Name())), true
}

// copyLocalMedia copies a local file path (when the implementation exposes
// one) into a temp file under the size limit.
func copyLocalMedia(srcPath string, maxBytes int64, name string) (string, string, string, bool) {
	src, err := os.Open(srcPath)
	if err != nil {
		return "", "", "", false
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return "", "", "", false
	}
	if info.Size() > maxBytes {
		slog.Warn("qq media too large; dropped", "path", srcPath, "size", info.Size(), "limit", maxBytes)
		return "", "", "", false
	}
	ext := extFor(name, srcPath)
	tmp, err := os.CreateTemp("", "goclaw_qq_*"+ext)
	if err != nil {
		return "", "", "", false
	}
	written, err := io.Copy(tmp, src)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", "", "", false
	}
	tmp.Close()
	return tmp.Name(), mimeForExt(ext), orName(name, filepath.Base(tmp.Name())), written > 0
}

// buildOutboundMediaSegment turns an OutboundMessage media attachment into a
// OneBot segment. Local files are base64-inlined (most reliable across
// implementations); remote URLs are passed through for the implementation to
// fetch.
func (c *Channel) buildOutboundMediaSegment(ctx context.Context, media bus.MediaAttachment) (onebot.MessageSegment, error) {
	url := media.URL
	if url == "" {
		return onebot.MessageSegment{}, errors.New("qq media: empty URL")
	}

	// Remote http(s) URL → pass through; the implementation fetches it.
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		segType := onebot.SegmentImage
		if isAudioMime(media.ContentType) {
			segType = onebot.SegmentRecord
		}
		return onebot.MessageSegment{
			Type: segType,
			Data: map[string]any{"url": url, "file": orName(media.Caption, "media")},
		}, nil
	}

	// Local file path → read + base64-inline.
	path := strings.TrimPrefix(url, fileScheme)
	data, err := os.ReadFile(path)
	if err != nil {
		return onebot.MessageSegment{}, fmt.Errorf("qq media: read %s: %w", path, err)
	}
	maxBytes := c.cfg.MediaMaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMediaMaxBytes
	}
	if int64(len(data)) > maxBytes {
		return onebot.MessageSegment{}, fmt.Errorf("qq media too large: %d bytes (limit %d)", len(data), maxBytes)
	}

	segType := onebot.SegmentImage
	if isAudioMime(media.ContentType) {
		segType = onebot.SegmentRecord
	}
	fname := filepath.Base(path)
	if fname == "" || fname == "." {
		fname = "media"
	}
	// OneBot image/record "file" accepts a "base64://<data>" reference; the
	// filename rides as a separate field so implementations that surface it
	// in notifications have something to show.
	return onebot.MessageSegment{
		Type: segType,
		Data: map[string]any{
			"file":     base64Scheme + base64.StdEncoding.EncodeToString(data),
			"filename": fname,
		},
	}, nil
}

// --- small helpers ---

func extFor(name, src string) string {
	if e := filepath.Ext(name); e != "" {
		return e
	}
	if e := filepath.Ext(src); e != "" {
		return e
	}
	return ".bin"
}

func mimeForExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".mp3":
		return "audio/mpeg"
	case ".m4a":
		return "audio/mp4"
	case ".wav":
		return "audio/wav"
	case ".amr":
		return "audio/amr"
	case ".silk":
		return "audio/silk"
	}
	return "application/octet-stream"
}

func isAudioMime(mime string) bool {
	return strings.HasPrefix(mime, "audio/")
}

func orName(preferred, fallback string) string {
	if preferred != "" {
		return preferred
	}
	return fallback
}
