// Package qq implements the QQ channel via the OneBot 11 protocol.
//
// QQ is a pure DB-instance channel: instances are created through the
// channel_instances HTTP/WS API and instantiated by the InstanceLoader. The
// protocol implementation (NapCat / Lagrange / LLOneBot) runs separately and
// connects to the gateway over OneBot 11 WebSocket (reverse or forward mode).
//
// One Channel maps to one QQ account (one self_id). It embeds
// channels.BaseChannel for policy / pairing / health / tenant plumbing and
// delegates the wire protocol to qq/onebot.Client.
package qq

import "github.com/nextlevelbuilder/goclaw/internal/config"

// qqCreds maps the credentials JSONB from channel_instances.credentials
// (decrypted AES-256-GCM). These are the ONLY secret-bearing fields; the rest
// of the instance config is non-secret and lives in config JSONB.
type qqCreds struct {
	// AccessToken is the OneBot bearer token. Empty = no auth (internal-only
	// deployments). On reverse WS the channel validates it during the HTTP→WS
	// upgrade; on forward WS it is sent as a Bearer header + query param.
	AccessToken string `json:"access_token,omitempty"`
	// SelfID is the QQ number the protocol implementation is logged in as.
	// Used to filter the bot's own echoed messages and to detect @bot mentions.
	SelfID string `json:"self_id,omitempty"`
}

// defaultReversePath is the WS mount prefix for reverse mode. The full
// per-instance path is {reverse_path}/{instance_name} so multiple QQ accounts
// can coexist on the same gateway without path collisions.
const defaultReversePath = "/v1/onebot/qq"

const (
	defaultDMPolicy    = "pairing"
	defaultGroupPolicy = "pairing"
	defaultChunkLimit  = 4500
)

// DefaultMediaMaxBytes caps media downloads/uploads (20 MiB), matching the
// other channels' default.
const DefaultMediaMaxBytes int64 = 20 * 1024 * 1024

// applyDefaults fills in zero-valued knobs with the documented defaults.
// Returns the resulting config.QQConfig. Called from the factory after JSONB
// unmarshal so the rest of the channel never has to branch on empty fields.
func applyDefaults(cfg config.QQConfig) config.QQConfig {
	if cfg.Direction == "" {
		cfg.Direction = "reverse"
	}
	if cfg.DMPolicy == "" {
		cfg.DMPolicy = defaultDMPolicy
	}
	if cfg.GroupPolicy == "" {
		cfg.GroupPolicy = defaultGroupPolicy
	}
	if cfg.ReversePath == "" {
		cfg.ReversePath = defaultReversePath
	}
	if cfg.ChunkLimit <= 0 {
		cfg.ChunkLimit = defaultChunkLimit
	}
	if cfg.MediaMaxBytes <= 0 {
		cfg.MediaMaxBytes = DefaultMediaMaxBytes
	}
	return cfg
}
