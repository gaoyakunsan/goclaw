package qq

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// Factory is the channels.ChannelFactory signature without a pending store.
// QQ group history benefits from DB persistence (survives restarts), so the
// gateway registers FactoryWithPendingStore instead. This bare Factory is kept
// for parity with other channel packages and tests that don't need persistence.
func Factory(name string, creds json.RawMessage, cfg json.RawMessage,
	msgBus *bus.MessageBus, pairingSvc store.PairingStore) (channels.Channel, error) {
	return buildChannel(name, creds, cfg, msgBus, pairingSvc, nil)
}

// FactoryWithPendingStore returns a ChannelFactory closed over a pending
// message store so QQ group history persists across restarts (matches the
// Discord / Feishu / ZaloPersonal pattern). The gateway hands the returned
// closure to the InstanceLoader.
func FactoryWithPendingStore(pendingStore store.PendingMessageStore) channels.ChannelFactory {
	return func(name string, creds json.RawMessage, cfg json.RawMessage,
		msgBus *bus.MessageBus, pairingSvc store.PairingStore) (channels.Channel, error) {
		return buildChannel(name, creds, cfg, msgBus, pairingSvc, pendingStore)
	}
}

// buildChannel is the shared construction path. It decodes credentials +
// config, applies defaults, validates the half-config that would otherwise
// fail at runtime, and wires the channel. Network/protocol connection is
// deferred to Start() so a misconfigured row never crashes boot.
func buildChannel(name string, creds json.RawMessage, cfg json.RawMessage,
	msgBus *bus.MessageBus, pairingSvc store.PairingStore,
	pendingStore store.PendingMessageStore) (channels.Channel, error) {

	var c qqCreds
	if len(creds) > 0 {
		if err := json.Unmarshal(creds, &c); err != nil {
			return nil, fmt.Errorf("qq: decode credentials: %w", err)
		}
	}

	var ic config.QQConfig
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &ic); err != nil {
			return nil, fmt.Errorf("qq: decode config: %w", err)
		}
	}
	ic = applyDefaults(ic)

	// Forward mode requires an endpoint to dial; catch the half-config here so
	// the operator sees an actionable error instead of an infinite reconnect
	// loop with no explanation.
	if ic.Direction == "forward" && ic.Endpoint == "" {
		return nil, errors.New("qq: direction=forward requires endpoint (ws://host:port)")
	}

	ch, err := New(ic, c, msgBus, pairingSvc, pendingStore)
	if err != nil {
		return nil, err
	}
	ch.SetType(channels.TypeQQ)
	ch.SetName(name)
	return ch, nil
}
