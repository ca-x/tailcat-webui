package tailnet

import (
	"context"
	"net"
	"net/netip"

	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

type RuntimeFactory interface {
	NewServer(context.Context, ServerSpec) (ServerRuntime, error)
	NewClient(context.Context, ClientSpec) (ClientRuntime, error)
}

type ServerRuntime interface {
	Start() error
	Close() error
	DrainTCP(context.Context) error
	ConnectionToken() string
	PublicKey() string
	AddAllowedClient(key.NodePublic)
}

type ClientRuntime interface {
	Close() error
	PublicKey() string
	DiscoPing(context.Context) (PingResult, error)
	DialTCPPort(context.Context, uint16) (net.Conn, error)
	Dial(context.Context, string, string) (net.Conn, error)
}

type TCPHandler func(context.Context, net.Conn)

// ReservedTCPHandlerFactory binds one pre-registered reserved service to the
// immutable ID of each server runtime built by Manager.
type ReservedTCPHandlerFactory func(serverID string) TCPHandler

type ServerSpec struct {
	Key                 key.NodePrivate
	Region              *tailcfg.DERPRegion
	RegionID            tailcfg.DERPRegionID
	DERPMapURL          string
	AllowedClients      []key.NodePublic
	TCPHandlers         map[uint16]TCPHandler
	ReservedTCPHandlers map[uint16]TCPHandler
	NoAuthSSHPorts      map[uint16]struct{}
	AllowProxy          func(netip.AddrPort) bool
	ForwardTCPHandler   func(netip.AddrPort) TCPHandler
	Logf                func(string, ...any)
}

type ClientSpec struct {
	ConnectionToken string
	Key             key.NodePrivate
	DERPMapURL      string
	Logf            func(string, ...any)
}

type PingResult struct {
	Endpoint       string
	PeerRelay      string
	LatencySeconds float64
}
