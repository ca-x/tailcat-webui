package tailnet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/ca-x/tailcat-webui/ent"
	"github.com/ca-x/tailcat-webui/ent/allowedclient"
	"github.com/ca-x/tailcat-webui/ent/exitrule"
	"github.com/ca-x/tailcat-webui/ent/portmapping"
	"github.com/ca-x/tailcat-webui/ent/tailclient"
	"github.com/ca-x/tailcat-webui/ent/tailserver"
	"github.com/ca-x/tailcat-webui/internal/events"
	"github.com/ca-x/tailcat-webui/internal/secrets"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

var (
	ErrNotFound           = errors.New("tailnet resource not found")
	ErrAlreadyRunning     = errors.New("Tailcat server is already running")
	ErrNotRunning         = errors.New("Tailcat server is not running")
	ErrConflict           = errors.New("tailnet resource conflicts with an existing resource")
	ErrInvalid            = errors.New("invalid tailnet input")
	ErrCapacity           = errors.New("tailnet capacity reached")
	ErrRestartRequired    = errors.New("Tailcat server must be stopped before changing this resource")
	ErrRegistrationClosed = errors.New("Tailcat reserved handler registration is closed")
)

type RuntimePhase = events.RuntimePhase

const (
	RuntimePhaseIdle        = events.RuntimePhaseIdle
	RuntimePhaseStarting    = events.RuntimePhaseStarting
	RuntimePhaseConnecting  = events.RuntimePhaseConnecting
	RuntimePhaseReady       = events.RuntimePhaseReady
	RuntimePhaseRunning     = events.RuntimePhaseRunning
	RuntimePhaseStopping    = events.RuntimePhaseStopping
	RuntimePhaseStopped     = events.RuntimePhaseStopped
	RuntimePhaseError       = events.RuntimePhaseError
	RuntimePhaseInterrupted = events.RuntimePhaseInterrupted
)

type Event struct {
	UserID       string       `json:"user_id"`
	ResourceKind string       `json:"resource_kind"`
	ResourceID   string       `json:"resource_id"`
	State        RuntimePhase `json:"state"`
	Message      string       `json:"message,omitempty"`
	At           time.Time    `json:"at"`
}

type EventRecorder func(context.Context, Event) error

type ServerView struct {
	ID               string       `json:"id"`
	Name             string       `json:"name"`
	KeyMode          string       `json:"key_mode"`
	Region           string       `json:"region"`
	DERPMapURL       string       `json:"derp_map_url,omitempty"`
	ExitNodeEnabled  bool         `json:"exit_node_enabled"`
	DesiredRunning   bool         `json:"desired_running"`
	RuntimeState     RuntimePhase `json:"runtime_state"`
	ConnectionToken  string       `json:"connection_token,omitempty"`
	PublicKey        string       `json:"public_key,omitempty"`
	StartedAt        time.Time    `json:"started_at,omitzero"`
	MappingCount     int          `json:"mapping_count"`
	AllowedKeyCount  int          `json:"allowed_key_count"`
	AllowlistEnabled bool         `json:"allowlist_enabled"`
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
}

type ClientView struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	DERPMapURL   string       `json:"derp_map_url,omitempty"`
	SavedKey     bool         `json:"saved_key"`
	TokenHint    string       `json:"token_hint"`
	RuntimeState RuntimePhase `json:"runtime_state"`
	PublicKey    string       `json:"public_key,omitempty"`
	LastPingMS   *int64       `json:"last_ping_ms,omitempty"`
	LastPath     string       `json:"last_path,omitempty"`
	LastPingAt   *time.Time   `json:"last_ping_at,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

type PortMappingView struct {
	ID         string    `json:"id"`
	ServerID   string    `json:"server_id"`
	Name       string    `json:"name"`
	Kind       string    `json:"kind"`
	ListenPort uint16    `json:"listen_port"`
	TargetHost string    `json:"target_host"`
	TargetPort uint16    `json:"target_port"`
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type AllowedClientView struct {
	ID        string    `json:"id"`
	ServerID  string    `json:"server_id"`
	Name      string    `json:"name"`
	PublicKey string    `json:"public_key"`
	CreatedAt time.Time `json:"created_at"`
}

type ExitRuleView struct {
	ID        string    `json:"id"`
	ServerID  string    `json:"server_id"`
	Prefix    string    `json:"prefix"`
	StartPort uint16    `json:"start_port"`
	EndPort   uint16    `json:"end_port"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CreateServerInput struct {
	Name            string
	KeyMode         string
	Region          string
	DERPMapURL      string
	ExitNodeEnabled bool
	Start           bool
}

type CreateClientInput struct {
	Name         string
	Server       string
	DERPMapURL   string
	SaveIdentity bool
}

type CreateMappingInput struct {
	Name       string
	Kind       string
	ListenPort uint16
	TargetHost string
	TargetPort uint16
}

type CreateExitRuleInput struct {
	Prefix    string
	StartPort uint16
	EndPort   uint16
	Enabled   bool
}

type runningServer struct {
	server    ServerRuntime
	userID    string
	startedAt time.Time
	token     string
	publicKey string
}

type runningClient struct {
	client ClientRuntime
	userID string
	state  RuntimePhase
}

type operationLock struct {
	mu   sync.Mutex
	refs int
}

func (r *runningServer) shutdown(ctx context.Context) error {
	return errors.Join(r.server.DrainTCP(ctx), r.server.Close())
}

type Manager struct {
	db               *ent.Client
	box              *secrets.Box
	mappingPolicy    *TargetPolicy
	exitPolicy       *TargetPolicy
	allowedDERPHosts map[string]struct{}
	unsafeSSH        bool
	runtimeFactory   RuntimeFactory
	recordEvent      EventRecorder
	logger           *slog.Logger
	eventsMu         sync.Mutex
	userEvents       map[string]*events.Broker[events.Envelope]
	eventSequences   map[string]uint64
	reservedMu       sync.Mutex
	reservedSealed   bool
	reservedHandlers map[uint16]ReservedTCPHandlerFactory
	mu               sync.RWMutex
	quotaMu          sync.Mutex
	opMu             sync.Mutex
	serverOps        map[string]*operationLock
	clientOps        map[string]*operationLock
	starting         map[string]string
	servers          map[string]*runningServer
	clients          map[string]*runningClient
}

func NewManager(db *ent.Client, box *secrets.Box, mappingPolicy, exitPolicy *TargetPolicy, allowedDERPHosts []string, unsafeSSH bool, recorder EventRecorder, logger *slog.Logger, factories ...RuntimeFactory) (*Manager, error) {
	if db == nil || box == nil || mappingPolicy == nil || exitPolicy == nil {
		return nil, errors.New("tailnet manager: nil dependency")
	}
	if len(factories) > 1 || len(factories) == 1 && factories[0] == nil {
		return nil, errors.New("tailnet manager: invalid runtime factory")
	}
	runtimeFactory := RuntimeFactory(tailcatRuntimeFactory{})
	if len(factories) == 1 {
		runtimeFactory = factories[0]
	}
	allowedHosts := map[string]struct{}{"tailcat.dev": {}}
	for _, host := range allowedDERPHosts {
		if host = normalizeDERPHost(host); host != "" {
			allowedHosts[host] = struct{}{}
		}
	}
	manager := &Manager{
		db: db, box: box, mappingPolicy: mappingPolicy, exitPolicy: exitPolicy, logger: logger,
		allowedDERPHosts: allowedHosts,
		unsafeSSH:        unsafeSSH,
		runtimeFactory:   runtimeFactory,
		recordEvent:      recorder,
		userEvents:       make(map[string]*events.Broker[events.Envelope]),
		eventSequences:   make(map[string]uint64),
		reservedHandlers: make(map[uint16]ReservedTCPHandlerFactory),
		servers:          make(map[string]*runningServer), clients: make(map[string]*runningClient), serverOps: make(map[string]*operationLock), clientOps: make(map[string]*operationLock), starting: make(map[string]string),
	}
	return manager, nil
}

// RegisterReservedTCPHandler installs a per-server reserved service factory.
// Registration is thread-safe and must finish before Restore or any server
// start seals the runtime configuration.
func (m *Manager) RegisterReservedTCPHandler(port uint16, factory ReservedTCPHandlerFactory) error {
	if m == nil || port == 0 || factory == nil {
		return ErrInvalid
	}
	m.reservedMu.Lock()
	defer m.reservedMu.Unlock()
	if m.reservedSealed {
		return ErrRegistrationClosed
	}
	if m.reservedHandlers[port] != nil {
		return ErrConflict
	}
	m.reservedHandlers[port] = factory
	return nil
}

func (m *Manager) sealReservedHandlers() {
	m.reservedMu.Lock()
	m.reservedSealed = true
	m.reservedMu.Unlock()
}

func (m *Manager) reservedHandlersForServer(serverID string) (map[uint16]TCPHandler, error) {
	m.reservedMu.Lock()
	m.reservedSealed = true
	factories := make(map[uint16]ReservedTCPHandlerFactory, len(m.reservedHandlers))
	for port, factory := range m.reservedHandlers {
		factories[port] = factory
	}
	m.reservedMu.Unlock()

	handlers := make(map[uint16]TCPHandler, len(factories))
	for port, factory := range factories {
		handler := factory(serverID)
		if handler == nil {
			return nil, fmt.Errorf("%w: nil handler for reserved Tailcat TCP port %d", ErrInvalid, port)
		}
		handlers[port] = handler
	}
	return handlers, nil
}

func (m *Manager) Events(userID string) *events.Broker[events.Envelope] {
	m.eventsMu.Lock()
	defer m.eventsMu.Unlock()
	return m.eventsForUserLocked(userID)
}

func (m *Manager) eventsForUserLocked(userID string) *events.Broker[events.Envelope] {
	broker := m.userEvents[userID]
	if broker == nil {
		broker = events.NewBroker[events.Envelope]()
		m.userEvents[userID] = broker
	}
	return broker
}

func (m *Manager) ReleaseEvents(userID string) {
	m.eventsMu.Lock()
	defer m.eventsMu.Unlock()
	delete(m.userEvents, userID)
}

func (m *Manager) Restore(ctx context.Context) error {
	m.sealReservedHandlers()
	rows, err := m.db.TailServer.Query().Where(tailserver.DesiredRunning(true)).All(ctx)
	if err != nil {
		return fmt.Errorf("list desired Tailcat servers: %w", err)
	}
	var errs []error
	var errsMu sync.Mutex
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for _, row := range rows {
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				m.publish(row.UserID, "server", row.ID, RuntimePhaseError, "restore deadline exceeded")
				errsMu.Lock()
				errs = append(errs, fmt.Errorf("restore server %s: %w", row.ID, ctx.Err()))
				errsMu.Unlock()
				return
			}
			instanceCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			if _, err := m.StartServer(instanceCtx, row.UserID, row.ID); err != nil {
				m.publish(row.UserID, "server", row.ID, RuntimePhaseError, "restore failed")
				errsMu.Lock()
				errs = append(errs, fmt.Errorf("restore server %s: %w", row.ID, err))
				errsMu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (m *Manager) Close() error {
	m.sealReservedHandlers()
	m.mu.Lock()
	servers := m.servers
	clients := m.clients
	m.servers = make(map[string]*runningServer)
	m.clients = make(map[string]*runningClient)
	m.mu.Unlock()
	var wg sync.WaitGroup
	errCh := make(chan error, len(servers)+len(clients))
	for _, runtime := range servers {
		wg.Go(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			errCh <- runtime.shutdown(shutdownCtx)
		})
	}
	for _, runtime := range clients {
		wg.Go(func() { errCh <- runtime.client.Close() })
	}
	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) ListServers(ctx context.Context, userID string) ([]ServerView, error) {
	rows, err := m.db.TailServer.Query().Where(tailserver.UserIDEQ(userID)).WithMappings(func(q *ent.PortMappingQuery) { q.Where(portmapping.UserIDEQ(userID)) }).WithAllowedClients(func(q *ent.AllowedClientQuery) { q.Where(allowedclient.UserIDEQ(userID)) }).Order(ent.Desc(tailserver.FieldCreatedAt)).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list Tailcat servers: %w", err)
	}
	views := make([]ServerView, 0, len(rows))
	for _, row := range rows {
		view := serverView(row)
		view.MappingCount = len(row.Edges.Mappings)
		view.AllowedKeyCount = len(row.Edges.AllowedClients)
		m.mu.RLock()
		runtime := m.servers[row.ID]
		m.mu.RUnlock()
		if runtime != nil {
			view.RuntimeState = RuntimePhaseRunning
			view.ConnectionToken = runtime.token
			view.PublicKey = runtime.publicKey
			view.StartedAt = runtime.startedAt
		}
		views = append(views, view)
	}
	return views, nil
}

func (m *Manager) CreateServer(ctx context.Context, userID string, in CreateServerInput) (ServerView, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.KeyMode = strings.TrimSpace(in.KeyMode)
	if in.Name == "" || len(in.Name) > 80 || len(in.Region) > 255 || len(in.DERPMapURL) > 2048 || in.ExitNodeEnabled || (in.KeyMode != "ephemeral" && in.KeyMode != "saved") {
		return ServerView{}, ErrInvalid
	}
	if err := m.validateDERPConfig(in.Region, in.DERPMapURL); err != nil {
		return ServerView{}, err
	}
	id := uuid.NewV7().String()
	create := m.db.TailServer.Create().SetID(id).SetUserID(userID).SetName(in.Name).SetKeyMode(tailserver.KeyMode(in.KeyMode)).SetRegion(normalizeRegion(in.Region)).SetDerpMapURL(strings.TrimSpace(in.DERPMapURL)).SetDesiredRunning(in.Start)
	if in.KeyMode == "saved" {
		private := key.NewNode()
		text, err := private.MarshalText()
		if err != nil {
			return ServerView{}, fmt.Errorf("marshal Tailcat server key: %w", err)
		}
		ciphertext, err := m.box.Seal(text, secretAD(userID, id))
		if err != nil {
			return ServerView{}, err
		}
		create.SetKeyCipher(ciphertext)
	}
	m.quotaMu.Lock()
	count, countErr := m.db.TailServer.Query().Where(tailserver.UserIDEQ(userID)).Count(ctx)
	if countErr != nil {
		m.quotaMu.Unlock()
		return ServerView{}, fmt.Errorf("count Tailcat servers: %w", countErr)
	}
	if count >= 32 {
		m.quotaMu.Unlock()
		return ServerView{}, ErrCapacity
	}
	row, err := create.Save(ctx)
	m.quotaMu.Unlock()
	if err != nil {
		if ent.IsConstraintError(err) {
			return ServerView{}, ErrConflict
		}
		return ServerView{}, fmt.Errorf("create Tailcat server: %w", err)
	}
	if in.Start {
		view, err := m.StartServer(ctx, userID, row.ID)
		if err != nil {
			deleteErr := m.db.TailServer.DeleteOneID(row.ID).Exec(context.WithoutCancel(ctx))
			return ServerView{}, errors.Join(err, deleteErr)
		}
		return view, nil
	}
	return serverView(row), nil
}

func (m *Manager) StartServer(ctx context.Context, userID, id string) (ServerView, error) {
	m.sealReservedHandlers()
	unlock := m.lockServerOperation(id)
	defer unlock()
	m.mu.Lock()
	if m.servers[id] != nil || m.starting[id] != "" {
		m.mu.Unlock()
		return ServerView{}, ErrAlreadyRunning
	}
	runningForUser := 0
	for _, runtime := range m.servers {
		if runtime.userID == userID {
			runningForUser++
		}
	}
	for _, ownerID := range m.starting {
		if ownerID == userID {
			runningForUser++
		}
	}
	if runningForUser >= 8 || len(m.servers)+len(m.starting) >= 64 {
		m.mu.Unlock()
		return ServerView{}, ErrCapacity
	}
	m.starting[id] = userID
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.starting, id)
		m.mu.Unlock()
	}()
	row, err := m.db.TailServer.Query().Where(tailserver.IDEQ(id), tailserver.UserIDEQ(userID)).WithMappings(func(q *ent.PortMappingQuery) { q.Where(portmapping.Enabled(true), portmapping.UserIDEQ(userID)) }).WithAllowedClients(func(q *ent.AllowedClientQuery) { q.Where(allowedclient.UserIDEQ(userID)) }).WithExitRules(func(q *ent.ExitRuleQuery) { q.Where(exitrule.Enabled(true), exitrule.UserIDEQ(userID)) }).Only(ctx)
	if ent.IsNotFound(err) {
		return ServerView{}, ErrNotFound
	}
	if err != nil {
		return ServerView{}, fmt.Errorf("load Tailcat server: %w", err)
	}
	serverRuntime, err := m.buildServer(ctx, row)
	if err != nil {
		return ServerView{}, err
	}
	if err := serverRuntime.Start(); err != nil {
		_ = serverRuntime.Close()
		return ServerView{}, fmt.Errorf("start Tailcat server: %w", err)
	}
	runtime := &runningServer{
		server:    serverRuntime,
		startedAt: time.Now(),
		token:     serverRuntime.ConnectionToken(),
		publicKey: serverRuntime.PublicKey(),
	}
	if err := row.Update().SetDesiredRunning(true).Exec(ctx); err != nil {
		_ = serverRuntime.Close()
		return ServerView{}, fmt.Errorf("persist desired server state: %w", err)
	}
	runtime.userID = userID
	m.mu.Lock()
	m.servers[id] = runtime
	m.mu.Unlock()
	m.publish(userID, "server", id, RuntimePhaseRunning, "")
	view := serverView(row)
	view.DesiredRunning = true
	view.RuntimeState = RuntimePhaseRunning
	view.ConnectionToken = runtime.token
	view.PublicKey = runtime.publicKey
	view.StartedAt = runtime.startedAt
	view.MappingCount = len(row.Edges.Mappings)
	view.AllowedKeyCount = len(row.Edges.AllowedClients)
	return view, nil
}

func (m *Manager) StopServer(ctx context.Context, userID, id string) error {
	unlock := m.lockServerOperation(id)
	defer unlock()
	return m.stopServerLocked(ctx, userID, id)
}

func (m *Manager) stopServerLocked(ctx context.Context, userID, id string) error {
	row, err := m.db.TailServer.Query().Where(tailserver.IDEQ(id), tailserver.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load Tailcat server: %w", err)
	}
	m.mu.RLock()
	runtime := m.servers[id]
	m.mu.RUnlock()
	if runtime == nil {
		return row.Update().SetDesiredRunning(false).Exec(ctx)
	}
	if err := row.Update().SetDesiredRunning(false).Exec(ctx); err != nil {
		return fmt.Errorf("persist stopped server state: %w", err)
	}
	m.mu.Lock()
	delete(m.servers, id)
	m.mu.Unlock()
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := runtime.shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("stop Tailcat server: %w", err)
	}
	m.publish(userID, "server", id, RuntimePhaseStopped, "")
	return nil
}

func (m *Manager) DeleteServer(ctx context.Context, userID, id string) error {
	unlock := m.lockServerOperation(id)
	defer unlock()
	if m.isServerRunning(id) {
		if err := m.stopServerLocked(ctx, userID, id); err != nil {
			return err
		}
	}
	count, err := m.db.TailServer.Delete().Where(tailserver.IDEQ(id), tailserver.UserIDEQ(userID)).Exec(ctx)
	if err != nil {
		return fmt.Errorf("delete Tailcat server: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	m.publish(userID, "server", id, RuntimePhaseStopped, "")
	return nil
}

func (m *Manager) SetExitNodeEnabled(ctx context.Context, userID, id string, enabled bool) (ServerView, error) {
	unlock := m.lockServerOperation(id)
	defer unlock()
	row, err := m.db.TailServer.Query().Where(tailserver.IDEQ(id), tailserver.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return ServerView{}, ErrNotFound
	}
	if err != nil {
		return ServerView{}, fmt.Errorf("load Tailcat server: %w", err)
	}
	if enabled {
		hasRule, err := m.db.ExitRule.Query().Where(exitrule.ServerIDEQ(id), exitrule.UserIDEQ(userID), exitrule.Enabled(true)).Exist(ctx)
		if err != nil {
			return ServerView{}, fmt.Errorf("validate enabled exit rules: %w", err)
		}
		if !hasRule {
			return ServerView{}, ErrInvalid
		}
	}
	if row.ExitNodeEnabled == enabled {
		view := serverView(row)
		m.mu.RLock()
		runtime := m.servers[id]
		m.mu.RUnlock()
		if runtime != nil {
			view.RuntimeState = RuntimePhaseRunning
			view.ConnectionToken = runtime.token
			view.PublicKey = runtime.publicKey
			view.StartedAt = runtime.startedAt
		}
		return view, nil
	}
	if m.isServerRunning(id) {
		if err := m.stopServerLocked(ctx, userID, id); err != nil {
			return ServerView{}, err
		}
	}
	row, err = row.Update().SetExitNodeEnabled(enabled).Save(ctx)
	if err != nil {
		return ServerView{}, fmt.Errorf("persist exit-node state: %w", err)
	}
	return serverView(row), nil
}

func (m *Manager) lockServerOperation(id string) func() {
	return m.lockOperation(m.serverOps, id)
}

func (m *Manager) lockClientOperation(id string) func() {
	return m.lockOperation(m.clientOps, id)
}

func (m *Manager) lockOperation(locks map[string]*operationLock, id string) func() {
	m.opMu.Lock()
	lock := locks[id]
	if lock == nil {
		lock = new(operationLock)
		locks[id] = lock
	}
	lock.refs++
	m.opMu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.opMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(locks, id)
		}
		m.opMu.Unlock()
	}
}

func (m *Manager) ListMappings(ctx context.Context, userID, serverID string) ([]PortMappingView, error) {
	rows, err := m.db.PortMapping.Query().Where(portmapping.UserIDEQ(userID), portmapping.ServerIDEQ(serverID)).Order(ent.Asc(portmapping.FieldListenPort)).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list port mappings: %w", err)
	}
	views := make([]PortMappingView, 0, len(rows))
	for _, row := range rows {
		views = append(views, mappingView(row))
	}
	return views, nil
}

func (m *Manager) CreateMapping(ctx context.Context, userID, serverID string, in CreateMappingInput) (PortMappingView, error) {
	unlock := m.lockServerOperation(serverID)
	defer unlock()
	serverRow, err := m.db.TailServer.Query().Where(tailserver.IDEQ(serverID), tailserver.UserIDEQ(userID)).WithAllowedClients().Only(ctx)
	if ent.IsNotFound(err) {
		return PortMappingView{}, ErrNotFound
	} else if err != nil {
		return PortMappingView{}, err
	}
	if m.isServerRunning(serverID) {
		return PortMappingView{}, ErrRestartRequired
	}
	if in.ListenPort == 0 || m.isReservedTailcatPort(in.ListenPort) || len(strings.TrimSpace(in.Name)) == 0 || len(in.Name) > 80 || len(in.TargetHost) > 253 || (in.Kind != "tcp" && in.Kind != "no_auth_ssh") || (in.Kind == "no_auth_ssh" && !m.unsafeSSH) {
		return PortMappingView{}, ErrInvalid
	}
	if in.Kind == "no_auth_ssh" && (!serverRow.AllowlistEnabled || len(serverRow.Edges.AllowedClients) == 0) {
		return PortMappingView{}, ErrInvalid
	}
	if in.Kind == "tcp" && (strings.TrimSpace(in.TargetHost) == "" || in.TargetPort == 0) {
		return PortMappingView{}, ErrInvalid
	}
	if in.Kind == "tcp" {
		checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if _, err := m.mappingPolicy.Resolve(checkCtx, in.TargetHost, in.TargetPort); err != nil {
			return PortMappingView{}, err
		}
	}
	m.quotaMu.Lock()
	count, countErr := m.db.PortMapping.Query().Where(portmapping.ServerIDEQ(serverID)).Count(ctx)
	if countErr != nil {
		m.quotaMu.Unlock()
		return PortMappingView{}, fmt.Errorf("count port mappings: %w", countErr)
	}
	if count >= 64 {
		m.quotaMu.Unlock()
		return PortMappingView{}, ErrCapacity
	}
	row, err := m.db.PortMapping.Create().SetUserID(userID).SetServerID(serverID).SetName(strings.TrimSpace(in.Name)).SetKind(portmapping.Kind(in.Kind)).SetListenPort(in.ListenPort).SetTargetHost(strings.TrimSpace(in.TargetHost)).SetTargetPort(in.TargetPort).Save(ctx)
	m.quotaMu.Unlock()
	if err != nil {
		if ent.IsConstraintError(err) {
			return PortMappingView{}, ErrConflict
		}
		return PortMappingView{}, fmt.Errorf("create port mapping: %w", err)
	}
	return mappingView(row), nil
}

func (m *Manager) DeleteMapping(ctx context.Context, userID, id string) error {
	row, err := m.db.PortMapping.Query().Where(portmapping.IDEQ(id), portmapping.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load port mapping: %w", err)
	}
	unlock := m.lockServerOperation(row.ServerID)
	defer unlock()
	if m.isServerRunning(row.ServerID) {
		if err := m.stopServerLocked(ctx, userID, row.ServerID); err != nil {
			return err
		}
	}
	count, err := m.db.PortMapping.Delete().Where(portmapping.IDEQ(id), portmapping.UserIDEQ(userID)).Exec(ctx)
	if err != nil {
		return fmt.Errorf("delete port mapping: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (m *Manager) ListExitRules(ctx context.Context, userID, serverID string) ([]ExitRuleView, error) {
	if _, err := m.db.TailServer.Query().Where(tailserver.IDEQ(serverID), tailserver.UserIDEQ(userID)).Only(ctx); ent.IsNotFound(err) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("load Tailcat server: %w", err)
	}
	rows, err := m.db.ExitRule.Query().Where(exitrule.UserIDEQ(userID), exitrule.ServerIDEQ(serverID)).Order(ent.Asc(exitrule.FieldCreatedAt)).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list exit rules: %w", err)
	}
	views := make([]ExitRuleView, 0, len(rows))
	for _, row := range rows {
		views = append(views, exitRuleView(row))
	}
	return views, nil
}

func (m *Manager) CreateExitRule(ctx context.Context, userID, serverID string, in CreateExitRuleInput) (ExitRuleView, error) {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(in.Prefix))
	if err != nil || prefix.Addr().Is4In6() || in.StartPort == 0 || in.EndPort < in.StartPort {
		return ExitRuleView{}, ErrInvalid
	}
	prefix = prefix.Masked()
	unlock := m.lockServerOperation(serverID)
	defer unlock()
	if _, err := m.db.TailServer.Query().Where(tailserver.IDEQ(serverID), tailserver.UserIDEQ(userID)).Only(ctx); ent.IsNotFound(err) {
		return ExitRuleView{}, ErrNotFound
	} else if err != nil {
		return ExitRuleView{}, fmt.Errorf("load Tailcat server: %w", err)
	}
	count, err := m.db.ExitRule.Query().Where(exitrule.ServerIDEQ(serverID)).Count(ctx)
	if err != nil {
		return ExitRuleView{}, fmt.Errorf("count exit rules: %w", err)
	}
	if count >= 128 {
		return ExitRuleView{}, ErrCapacity
	}
	duplicate, err := m.db.ExitRule.Query().Where(exitrule.ServerIDEQ(serverID), exitrule.PrefixEQ(prefix.String()), exitrule.StartPortEQ(in.StartPort), exitrule.EndPortEQ(in.EndPort)).Exist(ctx)
	if err != nil {
		return ExitRuleView{}, fmt.Errorf("validate exit rule: %w", err)
	}
	if duplicate {
		return ExitRuleView{}, ErrConflict
	}
	if m.isServerRunning(serverID) {
		if err := m.stopServerLocked(ctx, userID, serverID); err != nil {
			return ExitRuleView{}, err
		}
	}
	row, err := m.db.ExitRule.Create().SetUserID(userID).SetServerID(serverID).SetPrefix(prefix.String()).SetStartPort(in.StartPort).SetEndPort(in.EndPort).SetEnabled(in.Enabled).Save(ctx)
	if err != nil {
		if ent.IsConstraintError(err) {
			return ExitRuleView{}, ErrConflict
		}
		return ExitRuleView{}, fmt.Errorf("create exit rule: %w", err)
	}
	return exitRuleView(row), nil
}

func (m *Manager) DeleteExitRule(ctx context.Context, userID, id string) error {
	row, err := m.db.ExitRule.Query().Where(exitrule.IDEQ(id), exitrule.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load exit rule: %w", err)
	}
	unlock := m.lockServerOperation(row.ServerID)
	defer unlock()
	row, err = m.db.ExitRule.Query().Where(exitrule.IDEQ(id), exitrule.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("reload exit rule: %w", err)
	}
	if m.isServerRunning(row.ServerID) {
		if err := m.stopServerLocked(ctx, userID, row.ServerID); err != nil {
			return err
		}
	}
	tx, err := m.db.Tx(ctx)
	if err != nil {
		return fmt.Errorf("begin exit rule deletion: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	count, err := tx.ExitRule.Delete().Where(exitrule.IDEQ(id), exitrule.UserIDEQ(userID)).Exec(ctx)
	if err != nil {
		return fmt.Errorf("delete exit rule: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	if row.Enabled {
		enabled, err := tx.ExitRule.Query().Where(exitrule.ServerIDEQ(row.ServerID), exitrule.UserIDEQ(userID), exitrule.Enabled(true)).Exist(ctx)
		if err != nil {
			return fmt.Errorf("count remaining enabled exit rules: %w", err)
		}
		if !enabled {
			if err := tx.TailServer.UpdateOneID(row.ServerID).Where(tailserver.UserIDEQ(userID)).SetExitNodeEnabled(false).Exec(ctx); err != nil {
				return fmt.Errorf("disable exit node after final rule deletion: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit exit rule deletion: %w", err)
	}
	committed = true
	return nil
}

func (m *Manager) ListAllowedClients(ctx context.Context, userID, serverID string) ([]AllowedClientView, error) {
	if _, err := m.db.TailServer.Query().Where(tailserver.IDEQ(serverID), tailserver.UserIDEQ(userID)).Only(ctx); ent.IsNotFound(err) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	rows, err := m.db.AllowedClient.Query().Where(allowedclient.UserIDEQ(userID), allowedclient.ServerIDEQ(serverID)).Order(ent.Asc(allowedclient.FieldCreatedAt)).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list allowed Tailcat clients: %w", err)
	}
	views := make([]AllowedClientView, 0, len(rows))
	for _, row := range rows {
		views = append(views, AllowedClientView{ID: row.ID, ServerID: row.ServerID, Name: row.Name, PublicKey: row.PublicKey, CreatedAt: row.CreatedAt})
	}
	return views, nil
}

func (m *Manager) CreateAllowedClient(ctx context.Context, userID, serverID, name, rawPublicKey string) (AllowedClientView, error) {
	name = strings.TrimSpace(name)
	rawPublicKey = strings.TrimSpace(rawPublicKey)
	var public key.NodePublic
	if name == "" || len(name) > 80 || len(rawPublicKey) > 128 || public.UnmarshalText([]byte(rawPublicKey)) != nil || public.IsZero() {
		return AllowedClientView{}, ErrInvalid
	}
	unlock := m.lockServerOperation(serverID)
	defer unlock()
	serverRow, err := m.db.TailServer.Query().Where(tailserver.IDEQ(serverID), tailserver.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return AllowedClientView{}, ErrNotFound
	} else if err != nil {
		return AllowedClientView{}, err
	}
	count, countErr := m.db.AllowedClient.Query().Where(allowedclient.ServerIDEQ(serverID)).Count(ctx)
	if countErr != nil {
		return AllowedClientView{}, fmt.Errorf("count allowed clients: %w", countErr)
	}
	if count >= 128 {
		return AllowedClientView{}, ErrCapacity
	}
	if !serverRow.AllowlistEnabled && m.isServerRunning(serverID) {
		if err := m.stopServerLocked(ctx, userID, serverID); err != nil {
			return AllowedClientView{}, err
		}
	}
	m.quotaMu.Lock()
	tx, err := m.db.Tx(ctx)
	if err != nil {
		m.quotaMu.Unlock()
		return AllowedClientView{}, fmt.Errorf("begin allowlist transaction: %w", err)
	}
	row, err := tx.AllowedClient.Create().SetUserID(userID).SetServerID(serverID).SetName(name).SetPublicKey(public.String()).Save(ctx)
	if err != nil {
		_ = tx.Rollback()
		m.quotaMu.Unlock()
		if ent.IsConstraintError(err) {
			return AllowedClientView{}, ErrConflict
		}
		return AllowedClientView{}, fmt.Errorf("create allowed Tailcat client: %w", err)
	}
	if err := tx.TailServer.UpdateOneID(serverID).SetAllowlistEnabled(true).Exec(ctx); err != nil {
		_ = tx.Rollback()
		m.quotaMu.Unlock()
		return AllowedClientView{}, fmt.Errorf("enable Tailcat client allowlist: %w", err)
	}
	if err := tx.Commit(); err != nil {
		m.quotaMu.Unlock()
		return AllowedClientView{}, fmt.Errorf("commit allowlist transaction: %w", err)
	}
	m.quotaMu.Unlock()
	m.mu.RLock()
	runtime := m.servers[serverID]
	m.mu.RUnlock()
	if runtime != nil {
		runtime.server.AddAllowedClient(public)
	}
	return AllowedClientView{ID: row.ID, ServerID: row.ServerID, Name: row.Name, PublicKey: row.PublicKey, CreatedAt: row.CreatedAt}, nil
}

func (m *Manager) DeleteAllowedClient(ctx context.Context, userID, id string) error {
	row, err := m.db.AllowedClient.Query().Where(allowedclient.IDEQ(id), allowedclient.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load allowed Tailcat client: %w", err)
	}
	unlock := m.lockServerOperation(row.ServerID)
	defer unlock()
	if m.isServerRunning(row.ServerID) {
		if err := m.stopServerLocked(ctx, userID, row.ServerID); err != nil {
			return err
		}
	}
	count, err := m.db.AllowedClient.Delete().Where(allowedclient.IDEQ(id), allowedclient.UserIDEQ(userID)).Exec(ctx)
	if err != nil {
		return fmt.Errorf("delete allowed Tailcat client: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (m *Manager) isServerRunning(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.servers[id] != nil || m.starting[id] != ""
}

func (m *Manager) ListClients(ctx context.Context, userID string) ([]ClientView, error) {
	rows, err := m.db.TailClient.Query().Where(tailclient.UserIDEQ(userID)).Order(ent.Desc(tailclient.FieldCreatedAt)).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list Tailcat clients: %w", err)
	}
	views := make([]ClientView, 0, len(rows))
	for _, row := range rows {
		view := clientView(row)
		m.mu.RLock()
		runtime := m.clients[row.ID]
		m.mu.RUnlock()
		if runtime != nil {
			view.RuntimeState = runtime.state
			view.PublicKey = runtime.client.PublicKey()
		}
		views = append(views, view)
	}
	return views, nil
}

func (m *Manager) CreateClient(ctx context.Context, userID string, in CreateClientInput) (ClientView, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" || len(in.Name) > 80 || len(in.Server) > 16<<10 || len(in.DERPMapURL) > 2048 {
		return ClientView{}, ErrInvalid
	}
	if err := m.validateDERPConfig("", in.DERPMapURL); err != nil {
		return ClientView{}, err
	}
	token, err := resolveToken(ctx, strings.TrimSpace(in.Server))
	if err != nil {
		return ClientView{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := m.validateTokenDERP(token); err != nil {
		if errors.Is(err, ErrTargetDenied) {
			return ClientView{}, err
		}
		return ClientView{}, fmt.Errorf("%w: invalid Tailcat connection token", ErrInvalid)
	}
	id := uuid.NewV7().String()
	tokenCipher, err := m.box.Seal([]byte(token), secretAD(userID, id)+"/token")
	if err != nil {
		return ClientView{}, err
	}
	create := m.db.TailClient.Create().SetID(id).SetUserID(userID).SetName(in.Name).SetServerTokenCipher(tokenCipher).SetTokenHint(tokenHint(string(token))).SetDerpMapURL(strings.TrimSpace(in.DERPMapURL))
	if in.SaveIdentity {
		private := key.NewNode()
		text, err := private.MarshalText()
		if err != nil {
			return ClientView{}, fmt.Errorf("marshal Tailcat client key: %w", err)
		}
		ciphertext, err := m.box.Seal(text, secretAD(userID, id))
		if err != nil {
			return ClientView{}, err
		}
		create.SetKeyCipher(ciphertext)
	}
	m.quotaMu.Lock()
	count, countErr := m.db.TailClient.Query().Where(tailclient.UserIDEQ(userID)).Count(ctx)
	if countErr != nil {
		m.quotaMu.Unlock()
		return ClientView{}, fmt.Errorf("count Tailcat clients: %w", countErr)
	}
	if count >= 128 {
		m.quotaMu.Unlock()
		return ClientView{}, ErrCapacity
	}
	row, err := create.Save(ctx)
	m.quotaMu.Unlock()
	if err != nil {
		if ent.IsConstraintError(err) {
			return ClientView{}, ErrConflict
		}
		return ClientView{}, fmt.Errorf("create Tailcat client: %w", err)
	}
	return clientView(row), nil
}

func (m *Manager) DeleteClient(ctx context.Context, userID, id string) error {
	unlock := m.lockClientOperation(id)
	defer unlock()
	if _, err := m.db.TailClient.Query().Where(tailclient.IDEQ(id), tailclient.UserIDEQ(userID)).Only(ctx); ent.IsNotFound(err) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	m.mu.Lock()
	runtime := m.clients[id]
	delete(m.clients, id)
	m.mu.Unlock()
	if runtime != nil {
		_ = runtime.client.Close()
	}
	if err := m.db.TailClient.DeleteOneID(id).Exec(ctx); err != nil {
		return fmt.Errorf("delete Tailcat client: %w", err)
	}
	m.publish(userID, "client", id, RuntimePhaseStopped, "")
	return nil
}

func (m *Manager) PingClient(ctx context.Context, userID, id string) (ClientView, error) {
	client, row, err := m.client(ctx, userID, id)
	if err != nil {
		return ClientView{}, err
	}
	result, err := client.DiscoPing(ctx)
	if err != nil {
		m.setClientState(id, RuntimePhaseError)
		m.publish(userID, "client", id, RuntimePhaseError, "ping failed")
		return ClientView{}, fmt.Errorf("ping Tailcat server: %w", err)
	}
	path := pingResultPath(result)
	latencyMS := int64(result.LatencySeconds * 1000)
	now := time.Now()
	row, err = row.Update().SetLastPingMs(latencyMS).SetLastPath(path).SetLastPingAt(now).Save(ctx)
	if err != nil {
		return ClientView{}, fmt.Errorf("store ping result: %w", err)
	}
	view := clientView(row)
	m.setClientState(id, RuntimePhaseReady)
	view.RuntimeState = RuntimePhaseReady
	view.PublicKey = client.PublicKey()
	m.publish(userID, "client", id, RuntimePhaseReady, path)
	return view, nil
}

func (m *Manager) CurrentPath(ctx context.Context, userID, id string) (string, error) {
	client, _, err := m.client(ctx, userID, id)
	if err != nil {
		return "", err
	}
	result, err := client.DiscoPing(ctx)
	if err != nil {
		return "", err
	}
	return pingResultPath(result), nil
}

func pingResultPath(result PingResult) string {
	if result.Endpoint != "" {
		return "direct"
	}
	if result.PeerRelay != "" {
		return "peer-relay"
	}
	return "derp"
}

func (m *Manager) DialPort(ctx context.Context, userID, id string, port uint16) (net.Conn, error) {
	if port == 0 {
		return nil, ErrInvalid
	}
	client, _, err := m.client(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	connection, err := client.DialTCPPort(ctx, port)
	if err != nil {
		m.setClientState(id, RuntimePhaseError)
		return nil, err
	}
	m.setClientState(id, RuntimePhaseReady)
	return connection, nil
}

func (m *Manager) Dial(ctx context.Context, userID, id, address string) (net.Conn, error) {
	if len(address) > 512 {
		return nil, ErrInvalid
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" {
		return nil, ErrInvalid
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return nil, ErrInvalid
	}
	client, _, err := m.client(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	connection, err := client.Dial(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		m.setClientState(id, RuntimePhaseError)
		return nil, err
	}
	m.setClientState(id, RuntimePhaseReady)
	return connection, nil
}

func (m *Manager) ParseToken(raw string) (any, error) {
	return tailcat.ParseConnBlobRaw(tailcat.ConnBlob(strings.TrimSpace(raw)))
}

func (m *Manager) ResolveToken(ctx context.Context, raw string) (string, error) {
	token, err := resolveToken(ctx, strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	resolved, err := token.Resolve(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve Tailcat token: %w", err)
	}
	return string(resolved), nil
}

func (m *Manager) buildServer(ctx context.Context, row *ent.TailServer) (ServerRuntime, error) {
	if err := m.validateDERPConfig(row.Region, row.DerpMapURL); err != nil {
		return nil, err
	}
	spec := ServerSpec{
		DERPMapURL:     row.DerpMapURL,
		TCPHandlers:    make(map[uint16]TCPHandler),
		NoAuthSSHPorts: make(map[uint16]struct{}),
	}
	reservedHandlers, err := m.reservedHandlersForServer(row.ID)
	if err != nil {
		return nil, err
	}
	spec.ReservedTCPHandlers = reservedHandlers
	if row.KeyMode == tailserver.KeyModeSaved {
		plaintext, err := m.box.Open(row.KeyCipher, secretAD(row.UserID, row.ID))
		if err != nil {
			return nil, err
		}
		if err := spec.Key.UnmarshalText(plaintext); err != nil {
			return nil, fmt.Errorf("decode Tailcat server key: %w", err)
		}
	}
	region, regionID, err := m.resolveRegion(ctx, row.Region, row.DerpMapURL)
	if err != nil {
		return nil, err
	}
	if region != nil {
		if err := m.validateDERPRegion(region); err != nil {
			return nil, err
		}
	}
	spec.Region, spec.RegionID = region, regionID
	for _, allowed := range row.Edges.AllowedClients {
		var public key.NodePublic
		if err := public.UnmarshalText([]byte(allowed.PublicKey)); err != nil {
			return nil, fmt.Errorf("decode allowed client %s: %w", allowed.ID, err)
		}
		spec.AllowedClients = append(spec.AllowedClients, public)
	}
	if row.AllowlistEnabled && len(spec.AllowedClients) == 0 {
		// Tailcat treats an empty slice as allow-all. A persisted enabled
		// allowlist with no entries must instead fail closed.
		spec.AllowedClients = append(spec.AllowedClients, key.NewNode().Public())
	}
	for _, mapping := range row.Edges.Mappings {
		if m.isReservedTailcatPort(mapping.ListenPort) {
			return nil, fmt.Errorf("%w: Tailcat TCP port %d is reserved", ErrInvalid, mapping.ListenPort)
		}
		if mapping.Kind == portmapping.KindNoAuthSSH && (!m.unsafeSSH || !row.AllowlistEnabled || len(row.Edges.AllowedClients) == 0) {
			return nil, errors.New("auth-free SSH is disabled outside loopback demo mode")
		}
		if mapping.Kind == portmapping.KindNoAuthSSH {
			spec.NoAuthSSHPorts[mapping.ListenPort] = struct{}{}
			continue
		}
		spec.TCPHandlers[mapping.ListenPort] = func(runtimeCtx context.Context, inbound net.Conn) {
			dialCtx, cancel := context.WithTimeout(runtimeCtx, 10*time.Second)
			defer cancel()
			target, err := m.mappingPolicy.Resolve(dialCtx, mapping.TargetHost, mapping.TargetPort)
			if err != nil {
				m.logger.Warn("Tailcat target denied", "server_id", row.ID, "mapping_id", mapping.ID)
				_ = inbound.Close()
				return
			}
			outbound, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", target)
			if err != nil {
				m.logger.Warn("Tailcat target dial failed", "server_id", row.ID, "mapping_id", mapping.ID, "error", err)
				_ = inbound.Close()
				return
			}
			proxyConnectionsContext(runtimeCtx, inbound, outbound)
		}
	}
	if row.ExitNodeEnabled {
		allowTarget := func(target netip.AddrPort) bool {
			return m.exitPolicy.AllowAddrPort(target) && exitRulesAllow(row.Edges.ExitRules, target)
		}
		spec.AllowProxy = allowTarget
		spec.ForwardTCPHandler = func(target netip.AddrPort) TCPHandler {
			if !allowTarget(target) {
				return nil
			}
			return func(runtimeCtx context.Context, tracked net.Conn) {
				dialCtx, cancel := context.WithTimeout(runtimeCtx, 10*time.Second)
				defer cancel()
				outbound, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", target.String())
				if err != nil {
					_ = tracked.Close()
					return
				}
				proxyConnectionsContext(runtimeCtx, tracked, outbound)
			}
		}
	}
	spec.Logf = func(format string, args ...any) {
		m.logger.Debug("Tailcat runtime", "server_id", row.ID, "message", fmt.Sprintf(format, args...))
	}
	runtime, err := m.runtimeFactory.NewServer(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("create Tailcat server runtime: %w", err)
	}
	return runtime, nil
}

func exitRulesAllow(rules []*ent.ExitRule, target netip.AddrPort) bool {
	for _, rule := range rules {
		prefix, err := netip.ParsePrefix(rule.Prefix)
		if err == nil && rule.Enabled && prefix.Contains(target.Addr().Unmap()) && rule.StartPort <= target.Port() && target.Port() <= rule.EndPort {
			return true
		}
	}
	return false
}

func (m *Manager) client(ctx context.Context, userID, id string) (ClientRuntime, *ent.TailClient, error) {
	unlock := m.lockClientOperation(id)
	defer unlock()
	row, err := m.db.TailClient.Query().Where(tailclient.IDEQ(id), tailclient.UserIDEQ(userID)).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("load Tailcat client: %w", err)
	}
	if err := m.validateDERPConfig("", row.DerpMapURL); err != nil {
		return nil, nil, err
	}
	m.mu.RLock()
	cached := m.clients[id]
	m.mu.RUnlock()
	if cached != nil {
		return cached.client, row, nil
	}
	serverToken, err := m.box.Open(row.ServerTokenCipher, secretAD(userID, id)+"/token")
	if err != nil {
		return nil, nil, err
	}
	if err := m.validateTokenDERP(tailcat.ConnBlob(serverToken)); err != nil {
		return nil, nil, err
	}
	spec := ClientSpec{ConnectionToken: string(serverToken), DERPMapURL: row.DerpMapURL}
	if len(row.KeyCipher) > 0 {
		plaintext, err := m.box.Open(row.KeyCipher, secretAD(userID, id))
		if err != nil {
			return nil, nil, err
		}
		if err := spec.Key.UnmarshalText(plaintext); err != nil {
			return nil, nil, fmt.Errorf("decode Tailcat client key: %w", err)
		}
	}
	spec.Logf = func(format string, args ...any) {
		m.logger.Debug("Tailcat runtime", "client_id", id, "message", fmt.Sprintf(format, args...))
	}
	client, err := m.runtimeFactory.NewClient(ctx, spec)
	if err != nil {
		return nil, nil, fmt.Errorf("create Tailcat client runtime: %w", err)
	}
	m.mu.Lock()
	if existing := m.clients[id]; existing != nil {
		m.mu.Unlock()
		_ = client.Close()
		return existing.client, row, nil
	}
	activeForUser := 0
	for _, runtime := range m.clients {
		if runtime.userID == userID {
			activeForUser++
		}
	}
	if activeForUser >= 32 || len(m.clients) >= 256 {
		m.mu.Unlock()
		_ = client.Close()
		return nil, nil, ErrCapacity
	}
	m.clients[id] = &runningClient{client: client, userID: userID, state: "idle"}
	m.mu.Unlock()
	return client, row, nil
}

func (m *Manager) resolveRegion(ctx context.Context, spec, derpMapURL string) (*tailcfg.DERPRegion, tailcfg.DERPRegionID, error) {
	spec = normalizeRegion(spec)
	if strings.Contains(spec, ".") || strings.Contains(spec, ",") {
		region := &tailcfg.DERPRegion{RegionID: 900, RegionCode: "custom", RegionName: "Custom DERP"}
		for i, host := range strings.Split(spec, ",") {
			host = strings.TrimSpace(host)
			if host != "" {
				region.Nodes = append(region.Nodes, &tailcfg.DERPNode{Name: fmt.Sprintf("custom-%d", i+1), RegionID: 900, HostName: host})
			}
		}
		if len(region.Nodes) == 0 {
			return nil, 0, errors.New("custom DERP region has no hosts")
		}
		return region, 0, nil
	}
	opts := []any{}
	if derpMapURL != "" {
		opts = append(opts, tailcat.DERPMapURL(derpMapURL))
	}
	opts = append(opts, tailcat.ExpandForServer)
	dm, err := tailcat.FetchDERPMap(ctx, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch DERP map: %w", err)
	}
	for _, region := range dm.Regions {
		if err := m.validateDERPRegion(region); err != nil {
			return nil, 0, fmt.Errorf("validate DERP map region %d: %w", region.RegionID, err)
		}
	}
	if spec == "auto" {
		regionID, err := tailcat.PickBestRegion(ctx, dm)
		if err != nil {
			return nil, 0, fmt.Errorf("pick DERP region: %w", err)
		}
		region := dm.Regions[regionID]
		if region == nil {
			return nil, 0, errors.New("no usable DERP region")
		}
		return region, 0, nil
	}
	if regionID, err := tailcfg.ParseDERPRegionID(spec); err == nil {
		region := dm.Regions[regionID]
		if region == nil {
			return nil, 0, fmt.Errorf("unknown DERP region %d", regionID)
		}
		return region, 0, nil
	}
	for _, region := range dm.Regions {
		if strings.EqualFold(region.RegionCode, spec) || strings.EqualFold(region.RegionName, spec) {
			return region, 0, nil
		}
	}
	return nil, 0, fmt.Errorf("unknown DERP region %q", spec)
}

func resolveToken(ctx context.Context, raw string) (tailcat.ConnBlob, error) {
	if strings.HasPrefix(raw, "tc") {
		return tailcat.ConnBlob(raw), nil
	}
	if raw == "" {
		return "", errors.New("Tailcat token or DNS name is required")
	}
	records, err := net.DefaultResolver.LookupTXT(ctx, raw)
	if err != nil {
		return "", fmt.Errorf("resolve Tailcat DNS name: %w", err)
	}
	for _, record := range records {
		if value, ok := strings.CutPrefix(strings.TrimSpace(record), "tailcat="); ok && strings.HasPrefix(value, "tc") {
			return tailcat.ConnBlob(value), nil
		}
	}
	return "", errors.New("DNS name has no tailcat= TXT record")
}

func normalizeRegion(region string) string {
	region = strings.TrimSpace(region)
	if region == "" {
		return "auto"
	}
	return region
}

func (m *Manager) validateDERPConfig(region, mapURL string) error {
	if raw := strings.TrimSpace(mapURL); raw != "" {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || (parsed.Port() != "" && parsed.Port() != "443") || parsed.Path != "/derpmap.json" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return ErrInvalid
		}
		if !m.isAllowedDERPHost(parsed.Hostname()) {
			return ErrTargetDenied
		}
	}
	region = normalizeRegion(region)
	if region == "auto" {
		return nil
	}
	if _, err := strconv.Atoi(region); err == nil {
		return nil
	}
	if strings.Contains(region, ".") || strings.Contains(region, ",") {
		for host := range strings.SplitSeq(region, ",") {
			if !m.isAllowedDERPHost(host) {
				return ErrTargetDenied
			}
		}
	}
	return nil
}

func normalizeDERPHost(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
}

func (m *Manager) validateTokenDERP(token tailcat.ConnBlob) error {
	info, err := tailcat.ParseConnBlob(token)
	if err != nil {
		return err
	}
	for _, region := range info.Region {
		if err := m.validateDERPRegion(region); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) validateDERPRegion(region *tailcfg.DERPRegion) error {
	for _, node := range region.Nodes {
		if node == nil || !m.isAllowedDERPHost(node.HostName) || node.InsecureForTests || (node.DERPPort != 0 && node.DERPPort != 443) || (node.STUNPort != -1 && node.STUNPort != 0 && node.STUNPort != 3478) {
			return ErrTargetDenied
		}
		if node.CertName != "" && !strings.EqualFold(node.CertName, node.HostName) {
			return ErrTargetDenied
		}
		for _, raw := range []string{node.IPv4, node.IPv6} {
			if raw == "" || raw == "none" {
				continue
			}
			addr, err := netip.ParseAddr(raw)
			addr = addr.Unmap()
			if err != nil || !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
				return ErrTargetDenied
			}
		}
	}
	return nil
}

func (m *Manager) isAllowedDERPHost(host string) bool {
	host = normalizeDERPHost(host)
	if strings.HasSuffix(host, ".ipn.dev") {
		return true
	}
	_, ok := m.allowedDERPHosts[host]
	return ok
}

func secretAD(userID, resourceID string) string { return userID + "/" + resourceID }

func tokenHint(raw string) string {
	if len(raw) <= 18 {
		return raw
	}
	return raw[:10] + "…" + raw[len(raw)-6:]
}

func serverView(row *ent.TailServer) ServerView {
	return ServerView{ID: row.ID, Name: row.Name, KeyMode: string(row.KeyMode), Region: row.Region, DERPMapURL: row.DerpMapURL, ExitNodeEnabled: row.ExitNodeEnabled, AllowlistEnabled: row.AllowlistEnabled, DesiredRunning: row.DesiredRunning, RuntimeState: RuntimePhaseStopped, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func clientView(row *ent.TailClient) ClientView {
	return ClientView{ID: row.ID, Name: row.Name, DERPMapURL: row.DerpMapURL, SavedKey: len(row.KeyCipher) > 0, TokenHint: row.TokenHint, RuntimeState: RuntimePhaseIdle, LastPingMS: row.LastPingMs, LastPath: row.LastPath, LastPingAt: row.LastPingAt, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func mappingView(row *ent.PortMapping) PortMappingView {
	return PortMappingView{ID: row.ID, ServerID: row.ServerID, Name: row.Name, Kind: string(row.Kind), ListenPort: row.ListenPort, TargetHost: row.TargetHost, TargetPort: row.TargetPort, Enabled: row.Enabled, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func (m *Manager) isReservedTailcatPort(port uint16) bool {
	m.reservedMu.Lock()
	defer m.reservedMu.Unlock()
	return m.reservedHandlers[port] != nil
}

func exitRuleView(row *ent.ExitRule) ExitRuleView {
	return ExitRuleView{ID: row.ID, ServerID: row.ServerID, Prefix: row.Prefix, StartPort: row.StartPort, EndPort: row.EndPort, Enabled: row.Enabled, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func (m *Manager) publish(userID, kind, id string, phase RuntimePhase, message string) {
	at := time.Now()
	m.publishEnvelope(userID, events.Envelope{Version: 1, Type: "runtime", ResourceKind: kind, ResourceID: id, Phase: phase, At: at})
	event := Event{UserID: userID, ResourceKind: kind, ResourceID: id, State: phase, Message: message, At: at}
	if m.recordEvent != nil {
		auditCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := m.recordEvent(auditCtx, event); err != nil {
			m.logger.ErrorContext(auditCtx, "Write runtime audit event failed", "error", err)
		}
	}
}

// PublishEvent publishes a typed owner-scoped operation through the same
// non-blocking broker and monotonic per-owner sequence as runtime events.
func (m *Manager) PublishEvent(userID string, event events.Envelope) {
	if event.Version == 0 {
		event.Version = 1
	}
	m.publishEnvelope(userID, event)
}

func (m *Manager) publishEnvelope(userID string, event events.Envelope) {
	if event.At.IsZero() {
		event.At = time.Now()
	}
	m.eventsMu.Lock()
	broker := m.eventsForUserLocked(userID)
	m.eventSequences[userID]++
	event.Sequence = m.eventSequences[userID]
	broker.Publish(event)
	m.eventsMu.Unlock()
}

func (m *Manager) setClientState(id string, state RuntimePhase) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime := m.clients[id]; runtime != nil {
		runtime.state = state
	}
}
