package transfer

import (
	"bytes"
	"context"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ca-x/tailcat-webui/ent"
	"github.com/ca-x/tailcat-webui/ent/auditevent"
	"github.com/ca-x/tailcat-webui/ent/enttest"
	"github.com/ca-x/tailcat-webui/ent/tailclient"
	"github.com/ca-x/tailcat-webui/ent/tailserver"
	"github.com/ca-x/tailcat-webui/ent/transferitem"
	"github.com/ca-x/tailcat-webui/ent/transferjob"
	"github.com/ca-x/tailcat-webui/ent/transfershare"
	"github.com/ca-x/tailcat-webui/internal/audit"
	"github.com/ca-x/tailcat-webui/internal/events"
	"github.com/ca-x/tailcat-webui/internal/secrets"
	"github.com/zeebo/blake3"

	_ "github.com/lib-x/entsqlite"
)

type transferDialerFunc func(context.Context, string, string, uint16) (net.Conn, error)

func (function transferDialerFunc) DialPort(ctx context.Context, ownerID, clientID string, port uint16) (net.Conn, error) {
	return function(ctx, ownerID, clientID, port)
}

type transferAuditFunc func(context.Context, audit.Entry) error

func (function transferAuditFunc) Record(ctx context.Context, entry audit.Entry) error {
	return function(ctx, entry)
}

func (function transferAuditFunc) RecordWithClient(ctx context.Context, _ *ent.Client, entry audit.Entry) error {
	return function(ctx, entry)
}

type selectiveFailAudit struct {
	delegate     *audit.Service
	failure      error
	resourceKind string
}

func (recorder *selectiveFailAudit) Record(ctx context.Context, entry audit.Entry) error {
	return recorder.delegate.Record(ctx, entry)
}

func (recorder *selectiveFailAudit) RecordWithClient(ctx context.Context, client *ent.Client, entry audit.Entry) error {
	if entry.Action == "transfer.limit" && entry.ResourceKind == recorder.resourceKind {
		return recorder.failure
	}
	return recorder.delegate.RecordWithClient(ctx, client, entry)
}

type transferPublisherFunc func(string, events.Envelope)

func (function transferPublisherFunc) PublishEvent(ownerID string, event events.Envelope) {
	function(ownerID, event)
}

func TestServiceRejectsNilDependencies(t *testing.T) {
	db, storage, box, _, _, _ := newTransferServiceData(t)
	dialer := transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") })
	auditor := transferAuditFunc(func(context.Context, audit.Entry) error { return nil })
	publisher := transferPublisherFunc(func(string, events.Envelope) {})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, test := range []struct {
		name      string
		db        *ent.Client
		storage   *Storage
		box       *secrets.Box
		dialer    ClientDialer
		auditor   AuditRecorder
		publisher EventPublisher
		logger    *slog.Logger
	}{
		{name: "database", storage: storage, box: box, dialer: dialer, auditor: auditor, publisher: publisher, logger: logger},
		{name: "storage", db: db, box: box, dialer: dialer, auditor: auditor, publisher: publisher, logger: logger},
		{name: "box", db: db, storage: storage, dialer: dialer, auditor: auditor, publisher: publisher, logger: logger},
		{name: "dialer", db: db, storage: storage, box: box, auditor: auditor, publisher: publisher, logger: logger},
		{name: "auditor", db: db, storage: storage, box: box, dialer: dialer, publisher: publisher, logger: logger},
		{name: "publisher", db: db, storage: storage, box: box, dialer: dialer, auditor: auditor, logger: logger},
		{name: "logger", db: db, storage: storage, box: box, dialer: dialer, auditor: auditor, publisher: publisher},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewService(t.Context(), test.db, test.storage, test.box, test.dialer, test.auditor, test.publisher, test.logger); err == nil {
				t.Fatal("nil dependency was accepted")
			}
		})
	}
	unavailable, err := secrets.NewBox(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(t.Context(), db, storage, unavailable, dialer, auditor, publisher, logger); !errors.Is(err, secrets.ErrUnavailable) {
		t.Fatalf("unavailable secret box error = %v, want ErrUnavailable", err)
	}
}

func TestReservedHandlerHasIndependentBoundedAdmission(t *testing.T) {
	db, storage, box, _, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	if cap(service.handlerSlots) != 16 {
		t.Fatalf("transfer handler admission = %d, want 16", cap(service.handlerSlots))
	}
	for range cap(service.handlerSlots) {
		service.handlerSlots <- struct{}{}
	}
	client, peer := net.Pipe()
	done := make(chan struct{})
	go func() {
		service.ReservedHandler(server.ID)(t.Context(), peer)
		close(done)
	}()
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("over-capacity transfer handler left its connection open")
	}
	<-done
	for range cap(service.handlerSlots) {
		<-service.handlerSlots
	}
	_ = client.Close()
}

func TestServiceRequiresExactlyFourWorkers(t *testing.T) {
	db, storage, box, _, _, _ := newTransferServiceData(t)
	dialer := transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") })
	auditor := transferAuditFunc(func(context.Context, audit.Entry) error { return nil })
	publisher := transferPublisherFunc(func(string, events.Envelope) {})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, workers := range []int{1, 2, 3, 5} {
		limits := DefaultServiceLimits()
		limits.Workers = workers
		if service, err := NewServiceWithLimits(t.Context(), db, storage, box, dialer, auditor, publisher, logger, limits); err == nil {
			_ = service.Close()
			t.Errorf("NewServiceWithLimits accepted %d workers", workers)
		}
	}
}

func TestOwnerWideRetainedShareAndJobCapsAreAtomic(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("object-cap-client").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	limits := DefaultServiceLimits()
	limits.MaxSharesPerOwner = 1
	limits.MaxRetainedJobsPerOwner = 1
	var service *Service
	dialer := transferDialerFunc(func(ctx context.Context, gotOwnerID, gotClientID string, port uint16) (net.Conn, error) {
		if gotOwnerID != owner.ID || gotClientID != client.ID || port != ReservedPort {
			return nil, errors.New("unexpected transfer dial target")
		}
		return handlerDial(t, service.ReservedHandler(server.ID))(ctx)
	})
	var err error
	service, err = NewServiceWithLimits(t.Context(), db, storage, box, dialer,
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }),
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })

	const attempts = 8
	start := make(chan struct{})
	errorsCh := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Go(func() {
			<-start
			_, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
			errorsCh <- err
		})
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	created, rejected := 0, 0
	for err := range errorsCh {
		switch {
		case err == nil:
			created++
		case errors.Is(err, ErrOwnerCapacity):
			rejected++
		default:
			t.Fatalf("CreateShare error = %v", err)
		}
	}
	if created != 1 || rejected != attempts-1 {
		t.Fatalf("concurrent shares created=%d rejected=%d", created, rejected)
	}
	share := db.TransferShare.Query().Where(transfershare.UserIDEQ(owner.ID)).OnlyX(t.Context())
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "cap.txt", Size: 1, Body: io.NopCloser(strings.NewReader("x"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	capability, err := service.RotateShare(t.Context(), owner.ID, share.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: capability}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: capability}); !errors.Is(err, ErrOwnerCapacity) {
		t.Fatalf("second retained job error = %v, want owner capacity", err)
	}
}

func TestStageFileClosesSourceWhenServiceIsAlreadyClosed(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, "", server.ID)
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	source := &recordingReadCloser{reader: strings.NewReader("x")}
	if _, err := service.StageFile(t.Context(), owner.ID, newEntityID(), StageFileInput{VirtualPath: "closed.txt", Size: 1, Body: source}); !errors.Is(err, ErrServiceClosed) {
		t.Fatalf("StageFile error = %v, want ErrServiceClosed", err)
	}
	if !source.closed.Load() {
		t.Fatal("StageFile did not close source after closed-service rejection")
	}
}

func TestConcurrentStageFileUsesAtomicConfiguredShareLimitBeforeSecondBodyRead(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	limits := DefaultServiceLimits()
	limits.MaxFileBytes = 4
	limits.MaxShareBytes = 4
	limits.MaxJobBytes = 6
	service, err := NewServiceWithLimits(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }), transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	first := newBlockingReadCloser()
	firstDone := make(chan error, 1)
	go func() {
		_, err := service.StageFile(ctx, owner.ID, share.ID, StageFileInput{VirtualPath: "first.bin", Size: 4, Body: first})
		firstDone <- err
	}()
	<-first.started
	second := &recordingReadCloser{reader: strings.NewReader("x")}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "second.bin", Size: 1, Body: second}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("second concurrent StageFile error = %v, want ErrQuotaExceeded", err)
	}
	if len(second.readSizes) != 0 || !second.closed.Load() {
		t.Fatalf("rejected StageFile source reads=%v closed=%t", second.readSizes, second.closed.Load())
	}
	cancel()
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first StageFile did not stop")
	}
}

func TestStageFileClassifiesProbeByteAgainstConfiguredFileLimit(t *testing.T) {
	db, _, box, owner, server, _ := newTransferServiceData(t)
	storage, err := NewStorageWithLimits(filepath.Join(t.TempDir(), "lower-file"), StorageLimits{MaxFileBytes: 4, MaxScopeBytes: 8, MaxOwnerBytes: 16, MaxFilesPerScope: MaxFilesPerShare})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	limits := DefaultServiceLimits()
	limits.MaxFileBytes = 4
	limits.MaxShareBytes = 8
	limits.MaxJobBytes = 8
	service, err := NewServiceWithLimits(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }), transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "max.bin", Size: 4, Body: io.NopCloser(strings.NewReader("12345"))}); !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("StageFile probe at max error = %v, want ErrFileTooLarge", err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "mismatch.bin", Size: 3, Body: io.NopCloser(strings.NewReader("1234"))}); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("StageFile probe below max error = %v, want ErrSizeMismatch", err)
	}
}

func TestCreateShareCommitFailureLeavesMetadataAndAuditAbsent(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		auditor,
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	var generated []byte
	service.secretHooks.capture = func(kind string, secret []byte) {
		if kind == "generated.capability" {
			generated = secret
		}
	}
	commitFailure := errors.New("injected share-create commit failure")
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "share.create" {
			return commitFailure
		}
		return nil
	}
	if _, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID}); !errors.Is(err, commitFailure) {
		t.Fatalf("CreateShare error = %v, want commit failure", err)
	}
	if got := db.TransferShare.Query().CountX(t.Context()); got != 0 {
		t.Fatalf("transfer shares after failed commit = %d, want 0", got)
	}
	if got := db.AuditEvent.Query().CountX(t.Context()); got != 0 {
		t.Fatalf("audit rows after failed commit = %d, want 0", got)
	}
	if len(generated) == 0 || !allZeroBytes(generated) {
		t.Fatal("failed share create retained mutable capability plaintext")
	}
}

func TestOutgoingShareManifestRangeServerBindingRotationAndExpiry(t *testing.T) {
	db, storage, box, owner, server, otherServer := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	created, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID, ExpiresAt: time.Now().Add(500 * time.Millisecond)})
	if err != nil {
		t.Fatalf("CreateShare: %v", err)
	}
	row := db.TransferShare.GetX(t.Context(), created.ID)
	if bytes.Contains(row.CapabilityHash, []byte(created.Capability)) || len(row.CapabilityHash) != 32 {
		t.Fatalf("stored capability material = %x", row.CapabilityHash)
	}
	file, err := service.StageFile(t.Context(), owner.ID, created.ID, StageFileInput{
		VirtualPath: "folder/hello.txt",
		Size:        3,
		Body:        io.NopCloser(bytes.NewBufferString("abc")),
	})
	if err != nil {
		t.Fatalf("StageFile: %v", err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, created.ID); err != nil {
		t.Fatalf("FinalizeShare: %v", err)
	}

	dial := handlerDial(t, service.ReservedHandler(server.ID))
	manifest, err := fetchManifest(t.Context(), dial, created.ID, created.Capability)
	if err != nil {
		t.Fatalf("fetchManifest: %v", err)
	}
	files := manifest.Files()
	if len(files) != 1 || files[0].FileID() != file.ID || files[0].VirtualPath() != "folder/hello.txt" || files[0].Size() != 3 {
		t.Fatalf("manifest files = %+v", files)
	}
	data, err := fetchRange(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), created.ID, created.Capability, file.ID, 0, 3)
	if err != nil || string(data) != "abc" {
		t.Fatalf("fetchRange data=%q error=%v", data, err)
	}
	if _, err := fetchRange(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), created.ID, created.Capability, file.ID, 1, 2); protocolCode(err) != CodeProtocolInvalid {
		t.Fatalf("non-block range error = %v, want %s", err, CodeProtocolInvalid)
	}
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(otherServer.ID)), created.ID, created.Capability); protocolCode(err) != CodeInvalidCapability {
		t.Fatalf("wrong-server error = %v, want %s", err, CodeInvalidCapability)
	}
	rotated, err := service.RotateShare(t.Context(), owner.ID, created.ID)
	if err != nil {
		t.Fatalf("RotateShare: %v", err)
	}
	if rotated == created.Capability {
		t.Fatal("rotation reused the prior capability")
	}
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), created.ID, created.Capability); protocolCode(err) != CodeInvalidCapability {
		t.Fatalf("old capability error = %v, want %s", err, CodeInvalidCapability)
	}
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), created.ID, rotated); err != nil {
		t.Fatalf("rotated capability: %v", err)
	}

	time.Sleep(time.Until(created.ExpiresAt) + 10*time.Millisecond)
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), created.ID, rotated); protocolCode(err) != CodeInvalidCapability {
		t.Fatalf("expired capability error = %v, want %s", err, CodeInvalidCapability)
	}
}

func TestJobCapabilityAssociatedDataBindsOwnerAndJob(t *testing.T) {
	_, _, box, owner, _, _ := newTransferServiceData(t)
	jobID := newEntityID()
	ciphertext, err := box.Seal([]byte("tcs1.secret"), jobCapabilityAAD(owner.ID, jobID))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := box.Open(ciphertext, jobCapabilityAAD(newEntityID(), jobID)); err == nil {
		t.Fatal("cipher opened with wrong owner associated data")
	}
	if _, err := box.Open(ciphertext, jobCapabilityAAD(owner.ID, newEntityID())); err == nil {
		t.Fatal("cipher opened with wrong job associated data")
	}
	plaintext, err := box.Open(ciphertext, jobCapabilityAAD(owner.ID, jobID))
	defer clearSecret(plaintext)
	if err != nil || string(plaintext) != "tcs1.secret" {
		t.Fatalf("Open associated-data-bound capability error=%v", err)
	}
}

func TestCapabilityAuthorizationAlwaysUsesConstantTimeDummyComparison(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	created, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	comparisons := make([][2]int, 0, 3)
	service.compareCapability = func(left, right []byte) int {
		mu.Lock()
		comparisons = append(comparisons, [2]int{len(left), len(right)})
		mu.Unlock()
		return 0
	}
	for _, request := range []wireRequest{
		{Version: protocolVersion, ShareID: created.ID, Capability: capabilityText(created.Capability), Operation: operationManifest},
		{Version: protocolVersion, ShareID: newEntityID(), Capability: capabilityText(created.Capability), Operation: operationManifest},
		{Version: protocolVersion, ShareID: created.ID, Capability: capabilityText("not-a-capability"), Operation: operationManifest},
	} {
		if _, err := service.authorizeRequest(t.Context(), server.ID, request); protocolCode(err) != CodeInvalidCapability {
			t.Fatalf("authorizeRequest error = %v, want invalid capability", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(comparisons) != 3 {
		t.Fatalf("constant-time comparisons = %d, want 3", len(comparisons))
	}
	for _, lengths := range comparisons {
		if lengths != [2]int{32, 32} {
			t.Fatalf("comparison lengths = %v, want [32 32]", lengths)
		}
	}
}

func TestServiceRejectsCrossOwnerShareServerFileClientAndJobIDs(t *testing.T) {
	db, storage, box, ownerA, serverA, _ := newTransferServiceData(t)
	ownerB := db.User.Create().SetIssuer("test").SetSubject(t.Name() + "-other").SaveX(t.Context())
	serverB := db.TailServer.Create().SetUserID(ownerB.ID).SetName("server-b").SetRegion("tailcat.dev").SaveX(t.Context())
	clientA := db.TailClient.Create().SetUserID(ownerA.ID).SetName("client-a").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, ownerA.ID, clientA.ID, serverA.ID)
	if _, err := service.CreateShare(t.Context(), ownerB.ID, CreateShareInput{ServerID: serverA.ID}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner CreateShare error = %v, want not found", err)
	}
	shareA, err := service.CreateShare(t.Context(), ownerA.ID, CreateShareInput{ServerID: serverA.ID})
	if err != nil {
		t.Fatal(err)
	}
	fileA, err := service.StageFile(t.Context(), ownerA.ID, shareA.ID, StageFileInput{VirtualPath: "a.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("aaa"))})
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"stage": func() error {
			_, err := service.StageFile(t.Context(), ownerB.ID, shareA.ID, StageFileInput{VirtualPath: "b.txt", Size: 1, Body: io.NopCloser(bytes.NewBufferString("b"))})
			return err
		},
		"finalize":   func() error { _, err := service.FinalizeShare(t.Context(), ownerB.ID, shareA.ID); return err },
		"rotate":     func() error { _, err := service.RotateShare(t.Context(), ownerB.ID, shareA.ID); return err },
		"delete":     func() error { return service.DeleteShare(t.Context(), ownerB.ID, shareA.ID) },
		"list_files": func() error { _, err := service.ListShareFiles(t.Context(), ownerB.ID, shareA.ID); return err },
	} {
		if err := call(); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-owner %s error = %v, want not found", name, err)
		}
	}
	if _, err := service.FinalizeShare(t.Context(), ownerA.ID, shareA.ID); err != nil {
		t.Fatal(err)
	}
	shareB, err := service.CreateShare(t.Context(), ownerB.ID, CreateShareInput{ServerID: serverB.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), ownerB.ID, shareB.ID, StageFileInput{VirtualPath: "b.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("bbb"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), ownerB.ID, shareB.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := fetchRange(t.Context(), handlerDial(t, service.ReservedHandler(serverB.ID)), shareB.ID, shareB.Capability, fileA.ID, 0, 3); protocolCode(err) != CodeShareNotFound {
		t.Fatalf("cross-owner file range error = %v, want share not found", err)
	}
	if _, err := service.CreateIncomingJob(t.Context(), ownerB.ID, CreateIncomingJobInput{ClientID: clientA.ID, Capability: shareA.Capability}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner client error = %v, want not found", err)
	}
	job, err := service.CreateIncomingJob(t.Context(), ownerA.ID, CreateIncomingJobInput{ClientID: clientA.ID, Capability: shareA.Capability})
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"get":        func() error { _, err := service.Job(t.Context(), ownerB.ID, job.ID); return err },
		"list_items": func() error { _, err := service.ListJobItems(t.Context(), ownerB.ID, job.ID); return err },
		"start":      func() error { _, err := service.StartJob(t.Context(), ownerB.ID, job.ID); return err },
		"cancel":     func() error { return service.CancelJob(t.Context(), ownerB.ID, job.ID) },
		"delete":     func() error { return service.DeleteJob(t.Context(), ownerB.ID, job.ID) },
		"retry":      func() error { _, err := service.RetryJob(t.Context(), ownerB.ID, job.ID); return err },
	} {
		if err := call(); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-owner job %s error = %v, want not found", name, err)
		}
	}
}

func TestParentScopedCleanupHidesOwnersAndRemovesMetadataBytesAndQuota(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("cleanup-client").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	other := db.User.Create().SetIssuer("test").SetSubject(t.Name() + "-other-parent").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "parent.txt", Size: 3, Body: io.NopCloser(strings.NewReader("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteServerResources(t.Context(), other.ID, server.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner server cleanup error = %v, want not found", err)
	}
	if err := service.DeleteClientResources(t.Context(), other.ID, client.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner client cleanup error = %v, want not found", err)
	}
	if err := service.DeleteServerResources(t.Context(), owner.ID, server.ID); err != nil {
		t.Fatalf("DeleteServerResources: %v", err)
	}
	if db.TransferShare.Query().Where(transfershare.IDEQ(share.ID)).ExistX(t.Context()) {
		t.Fatal("server cleanup retained dependent share metadata")
	}
	if !db.TailServer.Query().Where(tailserver.IDEQ(server.ID)).ExistX(t.Context()) {
		t.Fatal("server cleanup deleted the parent row")
	}
	if err := service.DeleteClientResources(t.Context(), owner.ID, client.ID); err != nil {
		t.Fatalf("DeleteClientResources: %v", err)
	}
	if db.TransferJob.Query().Where(transferjob.IDEQ(job.ID)).ExistX(t.Context()) {
		t.Fatal("client cleanup retained dependent job metadata")
	}
	if !db.TailClient.Query().Where(tailclient.IDEQ(client.ID)).ExistX(t.Context()) {
		t.Fatal("client cleanup deleted the parent row")
	}
	usage, err := storage.Usage(t.Context(), owner.ID, share.ID)
	if err != nil {
		t.Fatal(err)
	}
	if usage.OwnerBytes != 0 || usage.ShareBytes != 0 {
		t.Fatalf("usage after parent cleanup = %+v, want zero", usage)
	}
}

func TestExpirySchedulerDeletesIdleShareAndCompletedJobWithoutAccessOrRestart(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("expiry-client").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	expiresAt := time.Now().Add(5 * time.Second)

	idleShare, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID, ExpiresAt: expiresAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, idleShare.ID, StageFileInput{VirtualPath: "idle.txt", Size: 3, Body: io.NopCloser(strings.NewReader("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, idleShare.ID); err != nil {
		t.Fatal(err)
	}

	jobShare, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID, ExpiresAt: expiresAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, jobShare.ID, StageFileInput{VirtualPath: "job.txt", Size: 3, Body: io.NopCloser(strings.NewReader("xyz"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, jobShare.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: jobShare.Capability, ExpiresAt: expiresAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StartJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobStatus(t, db, job.ID, transferjob.StatusCompleted)

	deadline := expiresAt.Add(3 * time.Second)
	for time.Now().Before(deadline) {
		shareExists := db.TransferShare.Query().Where(transfershare.IDEQ(idleShare.ID)).ExistX(t.Context())
		jobExists := db.TransferJob.Query().Where(transferjob.IDEQ(job.ID)).ExistX(t.Context())
		if !shareExists && !jobExists {
			usage, err := storage.Usage(t.Context(), owner.ID, idleShare.ID)
			if err != nil {
				t.Fatal(err)
			}
			if usage.OwnerBytes != 0 {
				t.Fatalf("owner usage after expiry = %+v, want zero", usage)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expiry scheduler retained idle share=%v completed job=%v", db.TransferShare.Query().Where(transfershare.IDEQ(idleShare.ID)).ExistX(t.Context()), db.TransferJob.Query().Where(transferjob.IDEQ(job.ID)).ExistX(t.Context()))
}

func TestExpirySchedulerUsesLowerConfiguredLifetimeWithoutRecoveryCall(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("lower-expiry-client").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	now := time.Now()
	oldCreatedAt := now.Add(-time.Hour)
	share := db.TransferShare.Create().
		SetID(newEntityID()).
		SetUserID(owner.ID).
		SetServerID(server.ID).
		SetCapabilityHash(bytes.Repeat([]byte{1}, 32)).
		SetExpiresAt(now.Add(23 * time.Hour).UTC()).
		SetCreatedAt(oldCreatedAt).
		SaveX(t.Context())
	job := db.TransferJob.Create().
		SetID(newEntityID()).
		SetUserID(owner.ID).
		SetClientID(client.ID).
		SetRemoteShareID(share.ID).
		SetRemoteCapabilityCipher([]byte("cipher")).
		SetExpiresAt(now.Add(23 * time.Hour).UTC()).
		SetCreatedAt(oldCreatedAt).
		SaveX(t.Context())
	decoy := db.TransferShare.Create().
		SetID(newEntityID()).
		SetUserID(owner.ID).
		SetServerID(server.ID).
		SetCapabilityHash(bytes.Repeat([]byte{2}, 32)).
		SetExpiresAt(now.Add(time.Hour).UTC()).
		SetCreatedAt(now).
		SaveX(t.Context())
	decoyJob := db.TransferJob.Create().
		SetID(newEntityID()).
		SetUserID(owner.ID).
		SetClientID(client.ID).
		SetRemoteShareID(decoy.ID).
		SetRemoteCapabilityCipher([]byte("cipher")).
		SetExpiresAt(now.Add(time.Hour).UTC()).
		SetCreatedAt(now).
		SaveX(t.Context())

	limits := DefaultServiceLimits()
	limits.Expiry = 30 * time.Minute
	restarted, err := NewServiceWithLimits(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }),
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		shareExists := db.TransferShare.Query().Where(transfershare.IDEQ(share.ID)).ExistX(t.Context())
		jobExists := db.TransferJob.Query().Where(transferjob.IDEQ(job.ID)).ExistX(t.Context())
		if !shareExists && !jobExists {
			if !db.TransferShare.Query().Where(transfershare.IDEQ(decoy.ID)).ExistX(t.Context()) {
				t.Fatal("expiry scheduler removed later decoy share")
			}
			if !db.TransferJob.Query().Where(transferjob.IDEQ(decoyJob.ID)).ExistX(t.Context()) {
				t.Fatal("expiry scheduler removed later decoy job")
			}
			usage, err := storage.Usage(t.Context(), owner.ID, share.ID)
			if err != nil {
				t.Fatal(err)
			}
			if usage != (QuotaUsage{}) {
				t.Fatalf("owner usage after lower effective expiry = %+v, want zero", usage)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lower effective expiry retained share=%v job=%v", db.TransferShare.Query().Where(transfershare.IDEQ(share.ID)).ExistX(t.Context()), db.TransferJob.Query().Where(transferjob.IDEQ(job.ID)).ExistX(t.Context()))
}

func TestDeleteJobClosesAndJoinsCompletedItemReadLease(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("lease-client").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "lease.txt", Size: 3, Body: io.NopCloser(strings.NewReader("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StartJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobStatus(t, db, job.ID, transferjob.StatusCompleted)
	item := db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID)).OnlyX(t.Context())
	opened, err := service.OpenCompletedItem(t.Context(), owner.ID, job.ID, item.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.DeleteJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := opened.Handle.ReadAt(make([]byte, 1), 0); err == nil {
		t.Fatal("completed read handle remained readable after successful deletion")
	}
	if err := opened.Handle.Close(); err != nil {
		t.Fatalf("idempotent lease close: %v", err)
	}
}

func TestCreateIncomingJobEncryptsCapabilityAndCreatesRootedPartialsTransactionally(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("receiver").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatalf("CreateShare: %v", err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "hello.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatalf("StageFile: %v", err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatalf("FinalizeShare: %v", err)
	}

	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatalf("CreateIncomingJob: %v", err)
	}
	row := db.TransferJob.GetX(t.Context(), job.ID)
	if row.Status != transferjob.StatusReady || row.RemoteShareID != share.ID || row.TotalBytes != 3 || row.ReceivedBytes != 0 {
		t.Fatalf("incoming job row = %+v", row)
	}
	if bytes.Contains(row.RemoteCapabilityCipher, []byte(share.Capability)) {
		t.Fatal("incoming capability was stored in plaintext")
	}
	plaintext, err := box.Open(row.RemoteCapabilityCipher, jobCapabilityAAD(owner.ID, row.ID))
	defer clearSecret(plaintext)
	if err != nil || string(plaintext) != share.Capability {
		t.Fatalf("decrypt capability error=%v", err)
	}
	items := db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID), transferitem.UserIDEQ(owner.ID)).AllX(t.Context())
	if len(items) != 1 || items[0].Status != transferitem.StatusReady || items[0].SizeBytes != 3 || items[0].VirtualPath != "hello.txt" {
		t.Fatalf("incoming items = %+v", items)
	}
	partial, err := storage.OpenPartial(t.Context(), owner.ID, job.ID, items[0].StorageName, items[0].SizeBytes)
	if err != nil {
		t.Fatalf("OpenPartial: %v", err)
	}
	if err := partial.Close(); err != nil {
		t.Fatalf("Close partial: %v", err)
	}
}

func TestCapabilityBuffersAreClearedAfterAuthorizationIncomingAndRunner(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("zeroize").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	var capturesMu sync.Mutex
	captures := make(map[string][][]byte)
	service.secretHooks.capture = func(kind string, secret []byte) {
		capturesMu.Lock()
		captures[kind] = append(captures[kind], secret)
		capturesMu.Unlock()
	}
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "secret.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StartJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobStatus(t, db, job.ID, transferjob.StatusCompleted)
	deadline := time.Now().Add(2 * time.Second)
	for {
		capturesMu.Lock()
		allZero := len(captures["incoming.capability"]) > 0 && len(captures["incoming.secret"]) > 0 &&
			len(captures["runner.capability"]) > 0 && len(captures["runner.secret"]) > 0 &&
			len(captures["handler.request"]) > 0 && len(captures["authorization.secret"]) > 0
		for _, buffers := range captures {
			for _, buffer := range buffers {
				allZero = allZero && allZeroBytes(buffer)
			}
		}
		kinds := slices.Sorted(maps.Keys(captures))
		capturesMu.Unlock()
		if allZero {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capability buffers were not all cleared; kinds=%v", kinds)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestJobRunnerUsesExactlyFourWorkersAndCompletesOnlyAfterWholeHash(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("runner").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	payload := bytes.Repeat([]byte("x"), int(BlockSize)+3)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "large.bin", Size: int64(len(payload)), Body: io.NopCloser(bytes.NewReader(payload))}); err != nil {
		t.Fatalf("StageFile: %v", err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatalf("CreateIncomingJob: %v", err)
	}
	var started atomic.Int64
	var stopped atomic.Int64
	var active atomic.Int64
	var maximum atomic.Int64
	service.runnerHooks.workerStarted = func() {
		started.Add(1)
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
	}
	service.runnerHooks.workerStopped = func() {
		active.Add(-1)
		stopped.Add(1)
	}
	if _, err := service.StartJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		row := db.TransferJob.GetX(t.Context(), job.ID)
		if row.Status == transferjob.StatusCompleted {
			if row.ReceivedBytes != int64(len(payload)) || row.FinishedAt == nil {
				t.Fatalf("completed job = %+v", row)
			}
			break
		}
		if row.Status == transferjob.StatusFailed || row.Status == transferjob.StatusCanceled || row.Status == transferjob.StatusInterrupted {
			t.Fatalf("job ended as %s with %s", row.Status, row.ErrorCode)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete; status=%s received=%d", row.Status, row.ReceivedBytes)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if started.Load() != 4 || stopped.Load() != 4 || active.Load() != 0 || maximum.Load() > 4 {
		t.Fatalf("worker lifecycle started=%d stopped=%d active=%d max=%d", started.Load(), stopped.Load(), active.Load(), maximum.Load())
	}
	item := db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID)).OnlyX(t.Context())
	if item.Status != transferitem.StatusCompleted || item.ReceivedBytes != int64(len(payload)) || len(item.CompletedBlocks) != 2 {
		t.Fatalf("completed item = %+v", item)
	}
	manifest, err := storage.BuildFileManifest(t.Context(), owner.ID, job.ID, item.StorageName, item.ID, item.VirtualPath)
	if err != nil || manifest.BLAKE3() != item.Blake3 {
		t.Fatalf("final manifest hash=%q want=%q error=%v", manifest.BLAKE3(), item.Blake3, err)
	}
}

func TestTerminalPersistenceRetriesAndCASWinnerAuditsAndPublishesOnce(t *testing.T) {
	service, db, owner, client, box, terminalEvents := newTerminalTestService(t)
	job := createTerminalTestJob(t, db, box, owner.ID, client.ID, transferjob.StatusRunning)
	registerTerminalTestActive(service, owner.ID, job.ID)
	commitFailure := errors.New("transient terminal commit failure")
	attempts := 0
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "job.terminal" {
			attempts++
			if attempts < lifecyclePersistAttempts {
				return commitFailure
			}
		}
		return nil
	}
	service.finishJob(job.ID, nil)
	if attempts != lifecyclePersistAttempts {
		t.Fatalf("terminal attempts = %d, want %d", attempts, lifecyclePersistAttempts)
	}
	if status := db.TransferJob.GetX(t.Context(), job.ID).Status; status != transferjob.StatusCompleted {
		t.Fatalf("terminal status = %s, want completed", status)
	}
	service.finishJob(job.ID, nil)
	auditID := "job:" + job.ID + ":transfer.complete"
	if count := db.AuditEvent.Query().Where(auditevent.IDEQ(auditID)).CountX(t.Context()); count != 1 {
		t.Fatalf("terminal audit count = %d, want 1", count)
	}
	if terminalEvents.Load() != 1 {
		t.Fatalf("terminal event count = %d, want 1", terminalEvents.Load())
	}
	service.mu.Lock()
	active, ownerActive := len(service.activeJobs), service.ownerJobs[owner.ID]
	service.mu.Unlock()
	if active != 0 || ownerActive != 0 {
		t.Fatalf("terminal admission active=%d owner=%d", active, ownerActive)
	}
	if err := service.Close(); err != nil {
		t.Fatalf("Service.Close after transient retry: %v", err)
	}
}

func TestPermanentTerminalFailureReleasesSlotSurfacesCloseAndRestartRepairs(t *testing.T) {
	service, db, owner, client, box, _ := newTerminalTestService(t)
	zombie := createTerminalTestJob(t, db, box, owner.ID, client.ID, transferjob.StatusRunning)
	registerTerminalTestActive(service, owner.ID, zombie.ID)
	commitFailure := errors.New("permanent terminal commit failure")
	attempts := 0
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "job.terminal" {
			attempts++
			return commitFailure
		}
		return nil
	}
	service.finishJob(zombie.ID, nil)
	if attempts != lifecyclePersistAttempts {
		t.Fatalf("terminal attempts = %d, want %d", attempts, lifecyclePersistAttempts)
	}
	if status := db.TransferJob.GetX(t.Context(), zombie.ID).Status; status != transferjob.StatusRunning {
		t.Fatalf("unresolved terminal status = %s, want running marker", status)
	}
	service.mu.Lock()
	active, ownerActive := len(service.activeJobs), service.ownerJobs[owner.ID]
	service.mu.Unlock()
	if active != 0 || ownerActive != 0 {
		t.Fatalf("unresolved terminal admission active=%d owner=%d", active, ownerActive)
	}

	service.lifecycleHooks.beforeCommit = nil
	reuse := createTerminalTestJob(t, db, box, owner.ID, client.ID, transferjob.StatusReady)
	if _, err := service.StartJob(t.Context(), owner.ID, reuse.ID); err != nil {
		t.Fatalf("slot reuse StartJob: %v", err)
	}
	waitForTransferJobStatus(t, db, reuse.ID, transferjob.StatusCompleted)
	if err := service.Close(); !errors.Is(err, commitFailure) {
		t.Fatalf("Service.Close error = %v, want terminal failure", err)
	}

	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewService(t.Context(), db, service.storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		auditor,
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("restart Service: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if status := db.TransferJob.GetX(t.Context(), zombie.ID).Status; status != transferjob.StatusInterrupted {
		t.Fatalf("recovered zombie status = %s, want interrupted", status)
	}
	interruptAuditID := "job:" + zombie.ID + ":transfer.interrupt"
	if count := db.AuditEvent.Query().Where(auditevent.IDEQ(interruptAuditID)).CountX(t.Context()); count != 1 {
		t.Fatalf("interrupt audit count = %d, want 1", count)
	}
}

func TestShareRotationCancelsActiveHandlerAndDeleteRetriesStorageFailure(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	file, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	client, serverConn := net.Pipe()
	handlerDone := make(chan struct{})
	go func() {
		service.ReservedHandler(server.ID)(t.Context(), serverConn)
		close(handlerDone)
	}()
	if err := writeRequest(t.Context(), client, wireRequest{Version: protocolVersion, ShareID: share.ID, Capability: capabilityText(share.Capability), Operation: operationRange, FileID: file.ID, Offset: 0, Length: 3}); err != nil {
		t.Fatalf("writeRequest: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		service.mu.Lock()
		active := service.activeShareStreamsLocked(share.ID)
		service.mu.Unlock()
		if active == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("handler stream was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := service.RotateShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatalf("RotateShare: %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("rotation did not close active handler stream")
	}
	_ = client.Close()

	removeFailure := errors.New("remove staged file failed")
	storage.hooks.remove = func(*os.Root, string) error { return removeFailure }
	if err := service.DeleteShare(t.Context(), owner.ID, share.ID); !errors.Is(err, removeFailure) {
		t.Fatalf("DeleteShare error = %v, want removal failure", err)
	}
	if status := db.TransferShare.GetX(t.Context(), share.ID).Status; status != transfershare.StatusDeleting {
		t.Fatalf("share status after failed delete = %s, want deleting", status)
	}
	storage.hooks.remove = (*os.Root).Remove
	if err := service.DeleteShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatalf("retry DeleteShare: %v", err)
	}
	if _, err := db.TransferShare.Get(t.Context(), share.ID); !ent.IsNotFound(err) {
		t.Fatalf("share metadata remains after retry: %v", err)
	}
}

func TestRotationRevokesRequestAuthorizedBeforeStreamRegistration(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	file, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}

	authorized := make(chan struct{})
	releaseAuthorization := make(chan struct{})
	revocationClosed := make(chan struct{})
	var authorizeOnce sync.Once
	service.handlerHooks.afterAuthorized = func() {
		authorizeOnce.Do(func() {
			close(authorized)
			<-releaseAuthorization
		})
	}
	service.handlerHooks.afterRevocationClosed = func(gotShareID string) {
		if gotShareID == share.ID {
			select {
			case <-revocationClosed:
			default:
				close(revocationClosed)
			}
		}
	}

	type rangeResult struct {
		data []byte
		err  error
	}
	rangeDone := make(chan rangeResult, 1)
	go func() {
		data, err := fetchRange(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), share.ID, share.Capability, file.ID, 0, 3)
		rangeDone <- rangeResult{data: data, err: err}
	}()
	<-authorized
	rotateDone := make(chan struct {
		capability string
		err        error
	}, 1)
	go func() {
		capability, err := service.RotateShare(t.Context(), owner.ID, share.ID)
		rotateDone <- struct {
			capability string
			err        error
		}{capability: capability, err: err}
	}()
	<-revocationClosed
	select {
	case result := <-rotateDone:
		t.Fatalf("rotation returned before provisional request joined: %v", result.err)
	default:
	}
	close(releaseAuthorization)
	result := <-rangeDone
	if result.err == nil || len(result.data) != 0 {
		t.Fatalf("revoked request data=%q error=%v", result.data, result.err)
	}
	rotation := <-rotateDone
	if rotation.err != nil || rotation.capability == "" {
		t.Fatalf("RotateShare capability-present=%v error=%v", rotation.capability != "", rotation.err)
	}
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), share.ID, rotation.capability); err != nil {
		t.Fatalf("new capability after revocation: %v", err)
	}
}

func TestRotationReopensCommittedGenerationAfterCallerCancellation(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	service.lifecycleHooks.afterCommit = func(operation string) {
		if operation == "share.rotate" {
			cancel()
		}
	}
	capability, err := service.RotateShare(ctx, owner.ID, share.ID)
	if err != nil || capability == "" {
		t.Fatalf("RotateShare capability-present=%v error=%v", capability != "", err)
	}
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), share.ID, capability); err != nil {
		t.Fatalf("committed capability after caller cancellation: %v", err)
	}
}

func TestFinalizeAndEachRotationHaveDistinctAtomicAuditOccurrences(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		auditor, transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "audit.txt", Size: 1, Body: io.NopCloser(strings.NewReader("x"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := service.RotateShare(t.Context(), owner.ID, share.ID); err != nil {
			t.Fatal(err)
		}
	}
	if count := db.AuditEvent.Query().Where(auditevent.UserIDEQ(owner.ID), auditevent.ActionEQ("transfer.finalize")).CountX(t.Context()); count != 1 {
		t.Fatalf("finalize audits = %d, want 1", count)
	}
	if count := db.AuditEvent.Query().Where(auditevent.UserIDEQ(owner.ID), auditevent.ActionEQ("transfer.rotate")).CountX(t.Context()); count != 2 {
		t.Fatalf("rotation audits = %d, want 2", count)
	}
}

func TestInitialRetryAndTerminalAuditsUseDurableAttemptOccurrences(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("attempt-client").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	var failDial atomic.Bool
	var service *Service
	dialer := transferDialerFunc(func(ctx context.Context, _, _ string, port uint16) (net.Conn, error) {
		if port != ReservedPort {
			return nil, errors.New("wrong port")
		}
		if failDial.Load() {
			return nil, errors.New("offline")
		}
		return handlerDial(t, service.ReservedHandler(server.ID))(ctx)
	})
	service, err = NewService(t.Context(), db, storage, box, dialer, auditor, transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "attempt.txt", Size: 1, Body: io.NopCloser(strings.NewReader("x"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	failDial.Store(true)
	if _, err := service.StartJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatal(err)
	}
	waitForTransferJobStatus(t, db, job.ID, transferjob.StatusFailed)
	failDial.Store(false)
	var retryErr error
	for range 100 {
		if _, retryErr = service.RetryJob(t.Context(), owner.ID, job.ID); retryErr == nil {
			break
		}
		if !errors.Is(retryErr, ErrAlreadyActive) {
			t.Fatal(retryErr)
		}
		time.Sleep(time.Millisecond)
	}
	if retryErr != nil {
		t.Fatalf("RetryJob did not become admissible: %v", retryErr)
	}
	waitForTransferJobStatus(t, db, job.ID, transferjob.StatusCompleted)
	for action, want := range map[string]int{
		"transfer.start":    1,
		"transfer.retry":    1,
		"transfer.fail":     1,
		"transfer.complete": 1,
	} {
		if count := db.AuditEvent.Query().Where(auditevent.UserIDEQ(owner.ID), auditevent.ResourceIDEQ(job.ID), auditevent.ActionEQ(action)).CountX(t.Context()); count != want {
			t.Fatalf("%s audits = %d, want %d", action, count, want)
		}
	}
	row := db.TransferJob.GetX(t.Context(), job.ID)
	if row.Attempt != 2 || row.AttemptKind != transferjob.AttemptKindRetry {
		t.Fatalf("durable attempt = %d/%s, want 2/retry", row.Attempt, row.AttemptKind)
	}
}

func TestRotationDoesNotReopenOrReturnSecretAfterLaterGenerationWins(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	service.lifecycleHooks.afterCommit = func(operation string) {
		if operation == "share.rotate" {
			_, _ = service.closeShareAdmission(context.Background(), share.ID, ErrInvalidCapability)
		}
	}
	capability, err := service.RotateShare(t.Context(), owner.ID, share.ID)
	if capability != "" || !errors.Is(err, ErrInvalidState) {
		t.Fatalf("RotateShare capability-present=%v error=%v, want closed-generation error", capability != "", err)
	}
	if _, err := service.beginShareAdmission(t.Context(), share.ID); protocolCode(err) != CodeInvalidCapability {
		t.Fatalf("later-generation admission error = %v, want invalid capability", err)
	}
	service.lifecycleHooks.afterCommit = nil
	capability, err = service.RotateShare(t.Context(), owner.ID, share.ID)
	if err != nil || capability == "" {
		t.Fatalf("repair rotation capability-present=%v error=%v", capability != "", err)
	}
}

func TestDeleteRevokesRequestAuthorizedBeforeStreamRegistration(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	file, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}

	authorized := make(chan struct{})
	releaseAuthorization := make(chan struct{})
	revocationClosed := make(chan struct{})
	service.handlerHooks.afterAuthorized = func() {
		close(authorized)
		<-releaseAuthorization
	}
	service.handlerHooks.afterRevocationClosed = func(gotShareID string) {
		if gotShareID == share.ID {
			close(revocationClosed)
		}
	}

	rangeDone := make(chan error, 1)
	go func() {
		_, err := fetchRange(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), share.ID, share.Capability, file.ID, 0, 3)
		rangeDone <- err
	}()
	<-authorized
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- service.DeleteShare(t.Context(), owner.ID, share.ID) }()
	<-revocationClosed
	select {
	case err := <-deleteDone:
		t.Fatalf("delete returned before provisional request joined: %v", err)
	default:
	}
	close(releaseAuthorization)
	if err := <-rangeDone; err == nil {
		t.Fatal("request authorized before delete served bytes")
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteShare: %v", err)
	}
	if _, err := db.TransferShare.Get(t.Context(), share.ID); !ent.IsNotFound(err) {
		t.Fatalf("deleted share lookup error = %v, want not found", err)
	}
}

func TestDeleteShareCommitFailureLeavesDeletingRowAndRetriesAuditAtomically(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		auditor,
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	commitFailure := errors.New("injected delete commit failure")
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "share.delete" {
			return commitFailure
		}
		return nil
	}
	if err := service.DeleteShare(t.Context(), owner.ID, share.ID); !errors.Is(err, commitFailure) {
		t.Fatalf("DeleteShare error = %v, want commit failure", err)
	}
	if status := db.TransferShare.GetX(t.Context(), share.ID).Status; status != transfershare.StatusDeleting {
		t.Fatalf("share status = %s, want deleting", status)
	}
	deleteAuditID := "share:" + share.ID + ":transfer.delete"
	if exists := db.AuditEvent.Query().Where(auditevent.IDEQ(deleteAuditID)).ExistX(t.Context()); exists {
		t.Fatal("delete audit committed without metadata deletion")
	}
	service.lifecycleHooks.beforeCommit = nil
	if err := service.DeleteShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatalf("retry DeleteShare: %v", err)
	}
	if _, err := db.TransferShare.Get(t.Context(), share.ID); !ent.IsNotFound(err) {
		t.Fatalf("share lookup after retry = %v, want not found", err)
	}
	if count := db.AuditEvent.Query().Where(auditevent.IDEQ(deleteAuditID)).CountX(t.Context()); count != 1 {
		t.Fatalf("delete audit count = %d, want 1", count)
	}
}

func TestRotationIncomingCreateAndJobStartCommitFailuresRollbackStateAndAudit(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("atomic").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	var service *Service
	dialer := transferDialerFunc(func(ctx context.Context, _, _ string, _ uint16) (net.Conn, error) {
		return handlerDial(t, service.ReservedHandler(server.ID))(ctx)
	})
	service, err = NewService(t.Context(), db, storage, box, dialer, auditor,
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "atomic.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	var failedRotationSecret []byte
	service.secretHooks.capture = func(kind string, secret []byte) {
		if kind == "generated.capability" {
			failedRotationSecret = secret
		}
	}
	commitFailure := errors.New("injected lifecycle commit failure")
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "share.rotate" {
			return commitFailure
		}
		return nil
	}
	if capability, err := service.RotateShare(t.Context(), owner.ID, share.ID); capability != "" || !errors.Is(err, commitFailure) {
		t.Fatalf("RotateShare error = %v, want commit failure", err)
	}
	if len(failedRotationSecret) == 0 || !allZeroBytes(failedRotationSecret) {
		t.Fatal("failed rotation retained mutable capability plaintext")
	}
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), share.ID, share.Capability); err != nil {
		t.Fatalf("old capability after failed rotation: %v", err)
	}
	rotateAuditID := "share:" + share.ID + ":transfer.rotate"
	if exists := db.AuditEvent.Query().Where(auditevent.IDEQ(rotateAuditID)).ExistX(t.Context()); exists {
		t.Fatal("rotation audit committed without hash rotation")
	}

	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "job.create" {
			return commitFailure
		}
		return nil
	}
	if _, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability}); !errors.Is(err, commitFailure) {
		t.Fatalf("CreateIncomingJob error = %v, want commit failure", err)
	}
	if count := db.TransferJob.Query().CountX(t.Context()); count != 0 {
		t.Fatalf("jobs after failed create = %d, want 0", count)
	}
	if count := db.TransferItem.Query().CountX(t.Context()); count != 0 {
		t.Fatalf("items after failed create = %d, want 0", count)
	}

	service.lifecycleHooks.beforeCommit = nil
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "job.start" {
			return commitFailure
		}
		return nil
	}
	if _, err := service.StartJob(t.Context(), owner.ID, job.ID); !errors.Is(err, commitFailure) {
		t.Fatalf("StartJob error = %v, want commit failure", err)
	}
	if status := db.TransferJob.GetX(t.Context(), job.ID).Status; status != transferjob.StatusReady {
		t.Fatalf("job status after failed start = %s, want ready", status)
	}
	startAuditID := "job:" + job.ID + ":transfer.start"
	if exists := db.AuditEvent.Query().Where(auditevent.IDEQ(startAuditID)).ExistX(t.Context()); exists {
		t.Fatal("start audit committed without running transition")
	}
	service.mu.Lock()
	active := service.ownerJobs[owner.ID]
	service.mu.Unlock()
	if active != 0 {
		t.Fatalf("owner admission after failed start = %d, want 0", active)
	}
}

func TestRecoveryExpiredShareAndJobAuditCommitFailuresRemainRetryable(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("expiry").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	var service *Service
	dialer := transferDialerFunc(func(ctx context.Context, _, _ string, port uint16) (net.Conn, error) {
		if port != ReservedPort {
			return nil, errors.New("unexpected port")
		}
		return handlerDial(t, service.ReservedHandler(server.ID))(ctx)
	})
	service, err = NewService(t.Context(), db, storage, box, dialer, auditor,
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	expiresAt := time.Now().Add(500 * time.Millisecond)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID, ExpiresAt: expiresAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "expire.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability, ExpiresAt: expiresAt})
	if err != nil {
		t.Fatal(err)
	}
	commitFailure := errors.New("injected expiry audit commit failure")
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "share.expire" || operation == "job.expire" {
			return commitFailure
		}
		return nil
	}
	time.Sleep(time.Until(expiresAt) + 10*time.Millisecond)
	if err := service.RecoverAfterRestore(t.Context()); !errors.Is(err, commitFailure) {
		t.Fatalf("RecoverAfterRestore error = %v, want commit failure", err)
	}
	if status := db.TransferShare.GetX(t.Context(), share.ID).Status; status != transfershare.StatusDeleting {
		t.Fatalf("expired share status = %s, want deleting", status)
	}
	if status := db.TransferJob.GetX(t.Context(), job.ID).Status; status != transferjob.StatusDeleting {
		t.Fatalf("expired job status = %s, want deleting", status)
	}
	service.cancelExpiry(errServiceClosed)
	service.wg.Wait()
	service.lifecycleHooks.beforeCommit = nil
	if err := service.DeleteShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatalf("public retry DeleteShare: %v", err)
	}
	if err := service.DeleteJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatalf("public retry DeleteJob: %v", err)
	}
	if _, err := db.TransferShare.Get(t.Context(), share.ID); !ent.IsNotFound(err) {
		t.Fatalf("expired share lookup = %v, want not found", err)
	}
	if _, err := db.TransferJob.Get(t.Context(), job.ID); !ent.IsNotFound(err) {
		t.Fatalf("expired job lookup = %v, want not found", err)
	}
	for _, auditID := range []string{"share:" + share.ID + ":transfer.expire", "job:" + job.ID + ":transfer.expire"} {
		if count := db.AuditEvent.Query().Where(auditevent.IDEQ(auditID)).CountX(t.Context()); count != 1 {
			t.Fatalf("expiry audit %q count = %d, want 1", auditID, count)
		}
	}
	for _, auditID := range []string{"share:" + share.ID + ":transfer.delete", "job:" + job.ID + ":transfer.delete"} {
		if exists := db.AuditEvent.Query().Where(auditevent.IDEQ(auditID)).ExistX(t.Context()); exists {
			t.Fatalf("public expiry retry wrote delete audit %q", auditID)
		}
	}
}

func TestRecoveryLimitShareAndJobCommitFailuresRetryWithOnlyLimitAudit(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("limit-retry").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	first := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := first.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "limit.txt", Size: 3, Body: io.NopCloser(strings.NewReader("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := first.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultServiceLimits()
	limits.MaxFileBytes = 2
	limits.MaxShareBytes = 2
	limits.MaxJobBytes = 2
	second, err := NewServiceWithLimits(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		auditor, transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	auditFailure := errors.New("injected limit audit failure")
	second.auditor = &selectiveFailAudit{delegate: auditor, failure: auditFailure, resourceKind: "share"}
	commitFailure := errors.New("injected limit commit failure")
	second.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "job.limit" {
			return commitFailure
		}
		return nil
	}
	if err := second.RecoverAfterRestore(t.Context()); !errors.Is(err, commitFailure) || !errors.Is(err, auditFailure) {
		t.Fatalf("RecoverAfterRestore error = %v, want audit and commit failures", err)
	}
	shareRow := db.TransferShare.GetX(t.Context(), share.ID)
	if shareRow.Status != transfershare.StatusDeleting || shareRow.ErrorCode != transfershare.ErrorCodeTransferLimitExceeded {
		t.Fatalf("limit share state = %s/%s", shareRow.Status, shareRow.ErrorCode)
	}
	jobRow := db.TransferJob.GetX(t.Context(), job.ID)
	if jobRow.Status != transferjob.StatusDeleting || jobRow.ErrorCode != transferjob.ErrorCodeTransferLimitExceeded {
		t.Fatalf("limit job state = %s/%s", jobRow.Status, jobRow.ErrorCode)
	}
	for _, auditID := range []string{"share:" + share.ID + ":transfer.limit", "job:" + job.ID + ":transfer.limit"} {
		if db.AuditEvent.Query().Where(auditevent.IDEQ(auditID)).ExistX(t.Context()) {
			t.Fatalf("limit audit committed before metadata deletion: %s", auditID)
		}
	}
	second.auditor = auditor
	second.lifecycleHooks.beforeCommit = nil
	if err := second.DeleteShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatalf("public retry DeleteShare: %v", err)
	}
	if err := second.DeleteJob(t.Context(), owner.ID, job.ID); err != nil {
		t.Fatalf("public retry DeleteJob: %v", err)
	}
	for _, auditID := range []string{"share:" + share.ID + ":transfer.limit", "job:" + job.ID + ":transfer.limit"} {
		if count := db.AuditEvent.Query().Where(auditevent.IDEQ(auditID)).CountX(t.Context()); count != 1 {
			t.Fatalf("limit audit %q count = %d, want 1", auditID, count)
		}
	}
	for _, action := range []string{"transfer.expire", "transfer.delete"} {
		if count := db.AuditEvent.Query().Where(auditevent.UserIDEQ(owner.ID), auditevent.ActionEQ(action)).CountX(t.Context()); count != 0 {
			t.Fatalf("limit retry wrote %d %s audits", count, action)
		}
	}
}

func TestExpiryRevokesRequestAuthorizedBeforeStreamRegistration(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	file, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}

	authorized := make(chan struct{})
	releaseAuthorization := make(chan struct{})
	revocationClosed := make(chan struct{})
	expiryTrigger := make(chan func(), 1)
	service.handlerHooks.afterExpiryArmed = func(gotShareID string, trigger func()) {
		if gotShareID == share.ID {
			expiryTrigger <- trigger
		}
	}
	service.handlerHooks.afterAuthorized = func() {
		close(authorized)
		<-releaseAuthorization
	}
	service.handlerHooks.afterRevocationClosed = func(gotShareID string) {
		if gotShareID == share.ID {
			close(revocationClosed)
		}
	}
	rangeDone := make(chan error, 1)
	go func() {
		_, err := fetchRange(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), share.ID, share.Capability, file.ID, 0, 3)
		rangeDone <- err
	}()
	<-authorized
	go (<-expiryTrigger)()
	select {
	case <-revocationClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("share expiry did not close provisional admission")
	}
	close(releaseAuthorization)
	if err := <-rangeDone; err == nil {
		t.Fatal("request authorized before expiry served bytes")
	}
	if _, err := fetchManifest(t.Context(), handlerDial(t, service.ReservedHandler(server.ID)), share.ID, share.Capability); protocolCode(err) != CodeInvalidCapability {
		t.Fatalf("post-expiry request error = %v, want invalid capability", err)
	}
}

func TestExpiryCallbackCannotRecreateGateWhileDeleteRemovesIt(t *testing.T) {
	db, storage, box, _, _, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	shareID := newEntityID()
	expiryTrigger := make(chan func(), 1)
	service.handlerHooks.afterExpiryArmed = func(_ string, trigger func()) { expiryTrigger <- trigger }
	admission, err := service.beginShareAdmission(t.Context(), shareID)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.armShareExpiry(admission, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.commitShareAdmission(admission, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	service.finishShareAdmission(admission)
	beforeCancel := make(chan struct{})
	releaseCancel := make(chan struct{})
	service.handlerHooks.beforeGateExpiryCancel = func(string) {
		close(beforeCancel)
		<-releaseCancel
	}
	removeDone := make(chan struct{})
	go func() {
		service.removeShareGate(shareID)
		close(removeDone)
	}()
	<-beforeCancel
	triggerDone := make(chan struct{})
	go func() {
		(<-expiryTrigger)()
		close(triggerDone)
	}()
	<-triggerDone
	close(releaseCancel)
	<-removeDone
	service.mu.Lock()
	_, ghost := service.shareGates[shareID]
	service.mu.Unlock()
	if ghost {
		t.Fatal("expiry callback recreated a ghost gate during deletion")
	}
}

func TestTwoActiveJobsPerOwnerAreReservedBeforeWorkersAndCancelJoins(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("capacity").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "file.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	jobs := make([]JobView, 3)
	for index := range jobs {
		jobs[index], err = service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
		if err != nil {
			t.Fatalf("CreateIncomingJob %d: %v", index, err)
		}
	}
	service.dialer = transferDialerFunc(func(ctx context.Context, _, _ string, port uint16) (net.Conn, error) {
		if port != ReservedPort {
			return nil, errors.New("wrong port")
		}
		clientConn, peer := net.Pipe()
		go func() {
			<-ctx.Done()
			_ = peer.Close()
		}()
		return clientConn, nil
	})
	for index := range 2 {
		if _, err := service.StartJob(t.Context(), owner.ID, jobs[index].ID); err != nil {
			t.Fatalf("StartJob %d: %v", index, err)
		}
	}
	if _, err := service.StartJob(t.Context(), owner.ID, jobs[2].ID); !errors.Is(err, ErrOwnerCapacity) {
		t.Fatalf("third StartJob error = %v, want ErrOwnerCapacity", err)
	}
	for index := range 2 {
		if err := service.CancelJob(t.Context(), owner.ID, jobs[index].ID); err != nil {
			t.Fatalf("CancelJob %d: %v", index, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for index := range 2 {
		for {
			row := db.TransferJob.GetX(t.Context(), jobs[index].ID)
			if row.Status == transferjob.StatusCanceled {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %d did not cancel; status=%s", index, row.Status)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for {
		service.mu.Lock()
		activeJobs := len(service.activeJobs)
		ownerJobs := service.ownerJobs[owner.ID]
		service.mu.Unlock()
		if activeJobs == 0 && ownerJobs == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission after cancel active=%d owner=%d", activeJobs, ownerJobs)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLegalTransferTransitionsRejectImpossibleEnumEdges(t *testing.T) {
	for _, test := range []struct {
		from string
		to   string
		want bool
	}{
		{from: "staging", to: "ready", want: true},
		{from: "ready", to: "running", want: true},
		{from: "running", to: "completed", want: true},
		{from: "interrupted", to: "running", want: true},
		{from: "completed", to: "running", want: false},
		{from: "failed", to: "completed", want: false},
		{from: "deleting", to: "ready", want: false},
		{from: "unknown", to: "running", want: false},
	} {
		if got := legalTransferTransition(test.from, test.to); got != test.want {
			t.Fatalf("legalTransferTransition(%q, %q) = %v, want %v", test.from, test.to, got, test.want)
		}
	}
}

func TestProgressEventsNeverExceedOneHertzAndTerminalIsDistinct(t *testing.T) {
	var published atomic.Int64
	now := time.Unix(100, 0)
	service := &Service{
		publisher:         transferPublisherFunc(func(string, events.Envelope) { published.Add(1) }),
		progressNow:       func() time.Time { return now },
		progressPublished: make(map[string]time.Time),
	}
	payload := TransferEventPayload{JobID: "job", Status: string(transferjob.StatusRunning), ReceivedBytes: 1, TotalBytes: 2}
	service.publishJobProgress("owner", "job", payload)
	now = now.Add(999 * time.Millisecond)
	service.publishJobProgress("owner", "job", payload)
	if published.Load() != 1 {
		t.Fatalf("progress events before one second = %d, want 1", published.Load())
	}
	now = now.Add(time.Millisecond)
	service.publishJobProgress("owner", "job", payload)
	if published.Load() != 2 {
		t.Fatalf("progress events at one second = %d, want 2", published.Load())
	}
	service.publishTransfer("owner", "job", events.RuntimePhaseReady, TransferEventPayload{JobID: "job", Status: string(transferjob.StatusCompleted)})
	if published.Load() != 3 {
		t.Fatalf("terminal event count = %d, want distinct third event", published.Load())
	}
}

func TestRunnerRejectsBadBlockBeforeWriteAndWholeHashBeforeCompletion(t *testing.T) {
	for _, test := range []struct {
		name                  string
		wholeHash             string
		rangeBody             string
		wantReceived          int64
		wantCompleted         int
		wantProgressAfterSync bool
	}{
		{name: "block hash", wholeHash: blake3Hex("abc"), rangeBody: "xyz", wantReceived: 0, wantCompleted: 0},
		{name: "whole hash", wholeHash: blake3Hex("different"), rangeBody: "abc", wantReceived: 3, wantCompleted: 1, wantProgressAfterSync: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, storage, box, owner, _, _ := newTransferServiceData(t)
			client := db.TailClient.Create().SetUserID(owner.ID).SetName("integrity").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
			remoteShareID := newEntityID()
			remoteFileID := newEntityID()
			capability, _, err := newTestCapability(remoteShareID)
			if err != nil {
				t.Fatal(err)
			}
			manifest := manifestWire{
				Version: protocolVersion, ShareID: remoteShareID, BlockSize: BlockSize,
				Files: []manifestFileWire{{
					FileID: remoteFileID, VirtualPath: "file.bin", Size: 3,
					MTime: time.Unix(1, 0).UTC().Format(time.RFC3339Nano), BLAKE3: test.wholeHash,
					BlockSize: BlockSize, BlockHashes: []string{blake3Hex("abc")},
				}},
			}
			dialer := transferDialerFunc(func(ctx context.Context, ownerID, clientID string, port uint16) (net.Conn, error) {
				if ownerID != owner.ID || clientID != client.ID || port != ReservedPort {
					return nil, errors.New("unexpected integrity-test dial")
				}
				peer, server := net.Pipe()
				go func() {
					defer server.Close()
					request, err := readRequest(ctx, server)
					if err != nil {
						return
					}
					switch request.Operation {
					case operationManifest:
						payload, _ := json.Marshal(&manifest)
						_ = writeSuccessResponse(ctx, server, payload, MaxManifestResponseBytes)
					case operationRange:
						_ = writeSuccessResponse(ctx, server, []byte(test.rangeBody), MaxRangeResponseBytes)
					}
				}()
				return peer, nil
			})
			service, err := NewService(t.Context(), db, storage, box, dialer,
				transferAuditFunc(func(context.Context, audit.Entry) error { return nil }),
				transferPublisherFunc(func(string, events.Envelope) {}),
				slog.New(slog.NewTextHandler(io.Discard, nil)),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = service.Close() })
			var synced atomic.Bool
			var progressAfterSync atomic.Bool
			service.runnerHooks.afterBlockSync = func(string, int) { synced.Store(true) }
			service.runnerHooks.beforeProgressSave = func(string, int) { progressAfterSync.Store(synced.Load()) }
			job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: capability})
			if err != nil {
				t.Fatalf("CreateIncomingJob: %v", err)
			}
			if _, err := service.StartJob(t.Context(), owner.ID, job.ID); err != nil {
				t.Fatalf("StartJob: %v", err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				row := db.TransferJob.GetX(t.Context(), job.ID)
				if row.Status == transferjob.StatusFailed {
					if row.ErrorCode != transferjob.ErrorCodeTransferIntegrityMismatch {
						t.Fatalf("job error code = %s", row.ErrorCode)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("job did not fail; status=%s", row.Status)
				}
				time.Sleep(10 * time.Millisecond)
			}
			item := db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID)).OnlyX(t.Context())
			if item.ReceivedBytes != test.wantReceived || len(item.CompletedBlocks) != test.wantCompleted || item.Status != transferitem.StatusFailed {
				t.Fatalf("failed item = %+v", item)
			}
			if progressAfterSync.Load() != test.wantProgressAfterSync {
				t.Fatalf("progress-after-sync = %v, want %v", progressAfterSync.Load(), test.wantProgressAfterSync)
			}
			if test.wantReceived == 0 {
				handle, err := storage.Open(t.Context(), owner.ID, job.ID, item.StorageName)
				if err != nil {
					t.Fatal(err)
				}
				data := make([]byte, 3)
				_, readErr := handle.ReadAt(data, 0)
				_ = handle.Close()
				if readErr != nil || !bytes.Equal(data, []byte{0, 0, 0}) {
					t.Fatalf("bad block reached partial data=%v error=%v", data, readErr)
				}
			}
		})
	}
}

func TestStartupInterruptsAbandonedJobThenRecoveryResumesIt(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("restart").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	first := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := first.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "resume.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := first.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	db.TransferJob.UpdateOneID(job.ID).SetStatus(transferjob.StatusRunning).SetStartedAt(now).ExecX(t.Context())
	db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID)).OnlyX(t.Context()).Update().SetStatus(transferitem.StatusRunning).SetStartedAt(now).ExecX(t.Context())

	var second *Service
	dialer := transferDialerFunc(func(ctx context.Context, gotOwnerID, gotClientID string, port uint16) (net.Conn, error) {
		if gotOwnerID != owner.ID || gotClientID != client.ID || port != ReservedPort {
			return nil, errors.New("unexpected restart dial")
		}
		return handlerDial(t, second.ReservedHandler(server.ID))(ctx)
	})
	second, err = NewService(t.Context(), db, storage, box, dialer,
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }),
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewService restart: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if status := db.TransferJob.GetX(t.Context(), job.ID).Status; status != transferjob.StatusInterrupted {
		t.Fatalf("startup job status = %s, want interrupted", status)
	}
	if status := db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID)).OnlyX(t.Context()).Status; status != transferitem.StatusInterrupted {
		t.Fatalf("startup item status = %s, want interrupted", status)
	}
	if err := second.RecoverAfterRestore(t.Context()); err != nil {
		t.Fatalf("RecoverAfterRestore: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		row := db.TransferJob.GetX(t.Context(), job.ID)
		if row.Status == transferjob.StatusCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("resumed job did not complete; status=%s", row.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLowerLimitsAfterRestartDenyHandlerAndStartThenRecoveryCleansUsage(t *testing.T) {
	db, highStorage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("lower-restart").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	first := newLoopbackTransferService(t, db, highStorage, box, owner.ID, client.ID, server.ID)
	type pair struct {
		share ShareView
		job   JobView
	}
	pairs := make([]pair, 3)
	for index := range pairs {
		share, err := first.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := first.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: fmt.Sprintf("large-%d.bin", index), Size: 6, Body: io.NopCloser(strings.NewReader("123456"))}); err != nil {
			t.Fatal(err)
		}
		if _, err := first.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
			t.Fatal(err)
		}
		job, err := first.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
		if err != nil {
			t.Fatal(err)
		}
		pairs[index] = pair{share: share, job: job}
	}
	rootPath := highStorage.root.Name()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	db.TransferJob.UpdateOneID(pairs[2].job.ID).SetStatus(transferjob.StatusRunning).SetStartedAt(now).ExecX(t.Context())
	db.TransferItem.Query().Where(transferitem.JobIDEQ(pairs[2].job.ID)).OnlyX(t.Context()).Update().SetStatus(transferitem.StatusRunning).SetStartedAt(now).ExecX(t.Context())
	if err := highStorage.Close(); err != nil {
		t.Fatal(err)
	}
	lowStorage, err := NewStorageWithLimits(rootPath, StorageLimits{MaxFileBytes: 4, MaxScopeBytes: 5, MaxOwnerBytes: 64, MaxFilesPerScope: MaxFilesPerShare})
	if err != nil {
		t.Fatalf("reopen lower-limit storage: %v", err)
	}
	t.Cleanup(func() { _ = lowStorage.Close() })
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultServiceLimits()
	limits.MaxFileBytes = 4
	limits.MaxShareBytes = 4
	limits.MaxJobBytes = 5
	second, err := NewServiceWithLimits(t.Context(), db, lowStorage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) {
			return nil, errors.New("unexpected dial")
		}),
		auditor, transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatalf("NewServiceWithLimits lower restart: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if _, err := fetchManifest(t.Context(), handlerDial(t, second.ReservedHandler(server.ID)), pairs[0].share.ID, pairs[0].share.Capability); protocolCode(err) != CodeLimitExceeded {
		t.Fatalf("lower-limit handler error = %v, want %s", err, CodeLimitExceeded)
	}
	if _, err := second.StartJob(t.Context(), owner.ID, pairs[0].job.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("lower-limit StartJob error = %v, want ErrInvalidState", err)
	}
	if _, err := second.RetryJob(t.Context(), owner.ID, pairs[1].job.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("lower-limit RetryJob error = %v, want ErrInvalidState", err)
	}
	if err := second.RecoverAfterRestore(t.Context()); err != nil {
		t.Fatalf("RecoverAfterRestore lower limits: %v", err)
	}
	if got := db.TransferShare.Query().CountX(t.Context()); got != 0 {
		t.Fatalf("shares after lower-limit reconciliation = %d", got)
	}
	if got := db.TransferJob.Query().CountX(t.Context()); got != 0 {
		t.Fatalf("jobs after lower-limit reconciliation = %d", got)
	}
	usage, err := lowStorage.Usage(t.Context(), owner.ID, pairs[0].share.ID)
	if err != nil || usage != (QuotaUsage{}) {
		t.Fatalf("usage after lower-limit reconciliation = %+v, %v", usage, err)
	}
	if got := db.AuditEvent.Query().Where(auditevent.UserIDEQ(owner.ID), auditevent.ActionEQ("transfer.limit")).CountX(t.Context()); got != 6 {
		t.Fatalf("lower-limit audits = %d, want 6", got)
	}
	for _, action := range []string{"transfer.expire", "transfer.delete"} {
		if got := db.AuditEvent.Query().Where(auditevent.UserIDEQ(owner.ID), auditevent.ActionEQ(action)).CountX(t.Context()); got != 0 {
			t.Fatalf("lower-limit reconciliation wrote %d %s audits", got, action)
		}
	}
}

func TestConfiguredEffectiveExpiryOverridesLongerPersistedExpiryOnRecovery(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "retention.txt", Size: 3, Body: io.NopCloser(strings.NewReader("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	limits := DefaultServiceLimits()
	limits.Expiry = time.Millisecond
	restarted, err := NewServiceWithLimits(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }), transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.RecoverAfterRestore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if db.TransferShare.Query().CountX(t.Context()) != 0 {
		t.Fatal("effective configured expiry did not remove longer-lived persisted share")
	}
}

func TestRecoveryReconcilesExistingOwnerUsageAfterLowering(t *testing.T) {
	db, highStorage, box, owner, server, _ := newTransferServiceData(t)
	first := newTransferServiceForTest(t, db, highStorage, box)
	for index := range 3 {
		share, err := first.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := first.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: fmt.Sprintf("owner-%d.bin", index), Size: 4, Body: io.NopCloser(strings.NewReader("1234"))}); err != nil {
			t.Fatal(err)
		}
		if _, err := first.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
			t.Fatal(err)
		}
	}
	rootPath := highStorage.root.Name()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := highStorage.Close(); err != nil {
		t.Fatal(err)
	}
	lowStorage, err := NewStorageWithLimits(rootPath, StorageLimits{MaxFileBytes: 4, MaxScopeBytes: 4, MaxOwnerBytes: 8, MaxFilesPerScope: MaxFilesPerShare})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lowStorage.Close() })
	limits := DefaultServiceLimits()
	limits.MaxFileBytes = 4
	limits.MaxShareBytes = 4
	limits.MaxJobBytes = 4
	second, err := NewServiceWithLimits(t.Context(), db, lowStorage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }), transferPublisherFunc(func(string, events.Envelope) {}), slog.New(slog.NewTextHandler(io.Discard, nil)), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if err := second.RecoverAfterRestore(t.Context()); err != nil {
		t.Fatal(err)
	}
	remaining := db.TransferShare.Query().AllX(t.Context())
	if len(remaining) != 2 {
		t.Fatalf("remaining shares = %d, want 2", len(remaining))
	}
	usage, err := lowStorage.Usage(t.Context(), owner.ID, remaining[0].ID)
	if err != nil || usage.OwnerBytes != 8 {
		t.Fatalf("reconciled owner usage = %+v, %v", usage, err)
	}
}

func TestRecoveryQueuesThirdJobDeduplicatesAndStartsAfterRelease(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("queue").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "queue.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	jobs := make([]JobView, 3)
	for index := range jobs {
		jobs[index], err = service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
		if err != nil {
			t.Fatal(err)
		}
		db.TransferJob.UpdateOneID(jobs[index].ID).SetStatus(transferjob.StatusInterrupted).ExecX(t.Context())
		db.TransferItem.Query().Where(transferitem.JobIDEQ(jobs[index].ID)).OnlyX(t.Context()).Update().SetStatus(transferitem.StatusInterrupted).ExecX(t.Context())
	}
	releaseRanges := make(chan struct{})
	service.handlerHooks.afterAuthorized = func() {
		<-releaseRanges
	}
	if err := service.RecoverAfterRestore(t.Context()); err != nil {
		t.Fatalf("RecoverAfterRestore: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		service.mu.Lock()
		active := service.ownerJobs[owner.ID]
		queued := len(service.resumeQueue[owner.ID])
		service.mu.Unlock()
		if active == 2 && queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery admission active=%d queued=%d, want 2/1", active, queued)
		}
		time.Sleep(time.Millisecond)
	}
	if err := service.RecoverAfterRestore(t.Context()); err != nil {
		t.Fatalf("second RecoverAfterRestore: %v", err)
	}
	service.mu.Lock()
	queued := len(service.resumeQueue[owner.ID])
	service.mu.Unlock()
	if queued != 1 {
		t.Fatalf("deduplicated queue length = %d, want 1", queued)
	}
	close(releaseRanges)
	for _, job := range jobs {
		waitForTransferJobStatus(t, db, job.ID, transferjob.StatusCompleted)
	}
}

func TestRecoveryCapacityReleaseBeforeSchedulerClearKeepsWakeup(t *testing.T) {
	queueCtx, cancelQueue := context.WithCancelCause(context.Background())
	defer cancelQueue(nil)
	service := &Service{
		ownerJobs: make(map[string]int), resumeQueue: make(map[string][]*queuedResume),
		resumeScheduling: make(map[string]bool), queueCtx: queueCtx,
	}
	const ownerID = "owner"
	service.ownerJobs[ownerID] = maxActiveJobsPerOwner
	service.resumeQueue[ownerID] = []*queuedResume{{jobID: "queued"}}
	service.resumeScheduling[ownerID] = true
	beforeClear := make(chan struct{})
	releaseClear := make(chan struct{})
	service.runnerHooks.beforeResumeCapacityClear = func() {
		close(beforeClear)
		<-releaseClear
	}
	continueResult := make(chan bool, 1)
	go func() { continueResult <- service.finishResumeSchedulerForCapacity(ownerID) }()
	<-beforeClear
	service.mu.Lock()
	service.ownerJobs[ownerID]--
	service.scheduleQueuedResumesLocked(ownerID)
	service.mu.Unlock()
	close(releaseClear)
	if shouldContinue := <-continueResult; !shouldContinue {
		t.Fatal("scheduler cleared after capacity was released")
	}
	service.mu.Lock()
	scheduling := service.resumeScheduling[ownerID]
	queued := len(service.resumeQueue[ownerID])
	service.mu.Unlock()
	if !scheduling || queued != 1 {
		t.Fatalf("scheduler state scheduling=%v queued=%d, want true/1", scheduling, queued)
	}
}

func TestCloseCancelsAndJoinsBackoffResumeScheduler(t *testing.T) {
	db, storage, box, _, _, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	const ownerID = "queued-owner"
	service.mu.Lock()
	service.resumeQueue[ownerID] = []*queuedResume{{jobID: "queued-job", nextAttempt: time.Now().Add(time.Hour)}}
	service.scheduleQueuedResumesLocked(ownerID)
	service.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- service.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Service.Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Service.Close did not cancel resume backoff")
	}
	service.mu.Lock()
	scheduling := service.resumeScheduling[ownerID]
	service.mu.Unlock()
	if scheduling {
		t.Fatal("resume scheduler remained registered after Close")
	}
}

func TestNonretryableQueuedStartClearsProgressTimestamp(t *testing.T) {
	db, storage, box, _, _, _ := newTransferServiceData(t)
	service := newTransferServiceForTest(t, db, storage, box)
	ownerID := newEntityID()
	jobID := newEntityID()
	service.mu.Lock()
	service.progressPublished[jobID] = time.Now()
	service.resumeQueue[ownerID] = []*queuedResume{{jobID: jobID}}
	service.scheduleQueuedResumesLocked(ownerID)
	service.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for {
		service.mu.Lock()
		_, queued := service.resumeQueue[ownerID]
		_, scheduled := service.resumeScheduling[ownerID]
		_, retained := service.progressPublished[jobID]
		service.mu.Unlock()
		if !queued && !scheduled {
			if retained {
				t.Fatal("nonretryable queued start retained a progress timestamp")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("nonretryable queued start did not leave the resume queue")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRecoveryTransientStartFailureStaysQueuedAndRetriesAutomatically(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("queue-retry").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "retry.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	db.TransferJob.UpdateOneID(job.ID).SetStatus(transferjob.StatusInterrupted).ExecX(t.Context())
	db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID)).OnlyX(t.Context()).Update().SetStatus(transferitem.StatusInterrupted).ExecX(t.Context())
	startFailure := errors.New("transient recovery start failure")
	var attempts atomic.Int64
	service.lifecycleHooks.beforeCommit = func(operation string) error {
		if operation == "job.start" && attempts.Add(1) == 1 {
			return startFailure
		}
		return nil
	}
	if err := service.RecoverAfterRestore(t.Context()); !errors.Is(err, startFailure) {
		t.Fatalf("RecoverAfterRestore error = %v, want transient start failure", err)
	}
	waitForTransferJobStatus(t, db, job.ID, transferjob.StatusCompleted)
	if attempts.Load() != 2 {
		t.Fatalf("start attempts = %d, want 2", attempts.Load())
	}
	service.mu.Lock()
	queued := len(service.resumeQueue[owner.ID])
	service.mu.Unlock()
	if queued != 0 {
		t.Fatalf("queue length after retry = %d, want 0", queued)
	}
}

func TestRecoveryManagedDialFailureRequeuesAndCompletesWithoutManualRetry(t *testing.T) {
	db, storage, box, owner, server, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("dial-retry").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	service := newLoopbackTransferService(t, db, storage, box, owner.ID, client.ID, server.ID)
	share, err := service.CreateShare(t.Context(), owner.ID, CreateShareInput{ServerID: server.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StageFile(t.Context(), owner.ID, share.ID, StageFileInput{VirtualPath: "dial.txt", Size: 3, Body: io.NopCloser(bytes.NewBufferString("abc"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalizeShare(t.Context(), owner.ID, share.ID); err != nil {
		t.Fatal(err)
	}
	job, err := service.CreateIncomingJob(t.Context(), owner.ID, CreateIncomingJobInput{ClientID: client.ID, Capability: share.Capability})
	if err != nil {
		t.Fatal(err)
	}
	db.TransferJob.UpdateOneID(job.ID).SetStatus(transferjob.StatusInterrupted).ExecX(t.Context())
	db.TransferItem.Query().Where(transferitem.JobIDEQ(job.ID)).OnlyX(t.Context()).Update().SetStatus(transferitem.StatusInterrupted).ExecX(t.Context())
	originalDialer := service.dialer
	dialFailure := errors.New("transient range dial failure")
	var attempts atomic.Int64
	var runningEvents atomic.Int64
	service.progressNow = func() time.Time { return time.Unix(500, 0) }
	service.publisher = transferPublisherFunc(func(_ string, event events.Envelope) {
		payload, ok := event.Payload.(TransferEventPayload)
		if ok && payload.Status == string(transferjob.StatusRunning) {
			runningEvents.Add(1)
		}
	})
	service.dialer = transferDialerFunc(func(ctx context.Context, ownerID, clientID string, port uint16) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			return nil, dialFailure
		}
		return originalDialer.DialPort(ctx, ownerID, clientID, port)
	})
	if err := service.RecoverAfterRestore(t.Context()); err != nil {
		t.Fatalf("RecoverAfterRestore: %v", err)
	}
	waitForTransferJobStatus(t, db, job.ID, transferjob.StatusCompleted)
	if attempts.Load() < 2 {
		t.Fatalf("dial attempts = %d, want automatic retry", attempts.Load())
	}
	if runningEvents.Load() != 1 {
		t.Fatalf("managed-retry running/progress events = %d, want 1 within one second", runningEvents.Load())
	}
}

func TestManagedResumeBackoffGrowsAcrossRunCyclesAndResetsOnProgress(t *testing.T) {
	now := time.Unix(1000, 0)
	service := &Service{
		closed:      true,
		resumeQueue: make(map[string][]*queuedResume), resumeScheduling: make(map[string]bool),
		resumeFailures: make(map[string]int), ownerJobs: make(map[string]int),
		resumeNow: func() time.Time { return now }, resumeJitter: func(delay time.Duration) time.Duration { return delay },
		queueWake: make(chan struct{}, 1),
	}
	const ownerID = "owner"
	const jobID = "job"
	service.requeueManagedResume(ownerID, jobID)
	first := service.resumeQueue[ownerID][0]
	if first.failures != 1 || first.nextAttempt.Sub(now) != time.Second {
		t.Fatalf("first backoff = failures %d delay %s", first.failures, first.nextAttempt.Sub(now))
	}
	service.mu.Lock()
	removeQueuedResumeLocked(service, ownerID, first)
	service.mu.Unlock()
	service.requeueManagedResume(ownerID, jobID)
	second := service.resumeQueue[ownerID][0]
	if second.failures != 2 || second.nextAttempt.Sub(now) != 2*time.Second {
		t.Fatalf("second backoff = failures %d delay %s", second.failures, second.nextAttempt.Sub(now))
	}
	service.resetManagedResumeFailures(jobID)
	service.mu.Lock()
	removeQueuedResumeLocked(service, ownerID, second)
	service.mu.Unlock()
	service.requeueManagedResume(ownerID, jobID)
	reset := service.resumeQueue[ownerID][0]
	if reset.failures != 1 || reset.nextAttempt.Sub(now) != time.Second {
		t.Fatalf("reset backoff = failures %d delay %s", reset.failures, reset.nextAttempt.Sub(now))
	}
}

func newTerminalTestService(t *testing.T) (*Service, *ent.Client, *ent.User, *ent.TailClient, *secrets.Box, *atomic.Int64) {
	t.Helper()
	db, storage, box, owner, _, _ := newTransferServiceData(t)
	client := db.TailClient.Create().SetUserID(owner.ID).SetName("terminal").SetServerTokenCipher([]byte("cipher")).SetTokenHint("hint").SaveX(t.Context())
	auditor, err := audit.NewService(db)
	if err != nil {
		t.Fatal(err)
	}
	terminalEvents := new(atomic.Int64)
	service, err := NewService(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		auditor,
		transferPublisherFunc(func(_ string, event events.Envelope) {
			payload, ok := event.Payload.(TransferEventPayload)
			if ok && payload.Status == string(transferjob.StatusCompleted) {
				terminalEvents.Add(1)
			}
		}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return service, db, owner, client, box, terminalEvents
}

func createTerminalTestJob(t *testing.T, db *ent.Client, box *secrets.Box, ownerID, clientID string, status transferjob.Status) *ent.TransferJob {
	t.Helper()
	jobID := newEntityID()
	remoteShareID := newEntityID()
	capability, _, err := newTestCapability(remoteShareID)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := box.Seal([]byte(capability), jobCapabilityAAD(ownerID, jobID))
	if err != nil {
		t.Fatal(err)
	}
	create := db.TransferJob.Create().
		SetID(jobID).
		SetUserID(ownerID).
		SetClientID(clientID).
		SetRemoteShareID(remoteShareID).
		SetRemoteCapabilityCipher(ciphertext).
		SetStatus(status).
		SetTotalBytes(0).
		SetReceivedBytes(0).
		SetExpiresAt(time.Now().UTC().Add(time.Hour))
	if status == transferjob.StatusRunning {
		create.SetStartedAt(time.Now().UTC())
	}
	return create.SaveX(t.Context())
}

func registerTerminalTestActive(service *Service, ownerID, jobID string) {
	ctx, cancel := context.WithCancelCause(context.Background())
	service.mu.Lock()
	service.activeJobs[jobID] = &activeJob{
		ownerID: ownerID, ctx: ctx, cancel: cancel, stopExpiry: func() {}, done: make(chan struct{}),
	}
	service.ownerJobs[ownerID]++
	service.mu.Unlock()
}

func waitForTransferJobStatus(t *testing.T, db *ent.Client, jobID string, want transferjob.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		row, err := db.TransferJob.Get(t.Context(), jobID)
		if ent.IsNotFound(err) {
			t.Fatalf("job %s was deleted before reaching status %s", jobID, want)
		}
		if err != nil {
			t.Fatalf("load job %s while waiting for status %s: %v", jobID, want, err)
		}
		status := row.Status
		if status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job status = %s, want %s", status, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func blake3Hex(value string) string {
	hash := blake3.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func newTestCapability(shareID string) (string, []byte, error) {
	var secret [capabilitySecretBytes]byte
	for index := range secret {
		secret[index] = byte(index + 1)
	}
	defer clearSecret(secret[:])
	encoded, hash, err := encodeCapabilityBytes(shareID, &secret)
	if err != nil {
		return "", nil, err
	}
	defer encoded.clear()
	return string(encoded), hash, nil
}

func newTransferServiceData(t *testing.T) (*ent.Client, *Storage, *secrets.Box, *ent.User, *ent.TailServer, *ent.TailServer) {
	t.Helper()
	db := enttest.Open(t, "sqlite3", "file:"+t.Name()+"-"+newEntityID()+"?mode=memory&cache=shared&_pragma=foreign_keys(1)")
	owner := db.User.Create().SetIssuer("test").SetSubject(t.Name()).SaveX(t.Context())
	server := db.TailServer.Create().SetUserID(owner.ID).SetName("server").SetRegion("tailcat.dev").SaveX(t.Context())
	otherServer := db.TailServer.Create().SetUserID(owner.ID).SetName("other").SetRegion("tailcat.dev").SaveX(t.Context())
	storage, err := NewStorage(filepath.Join(t.TempDir(), "transfer"))
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	box, err := secrets.NewBox(bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatalf("NewBox: %v", err)
	}
	return db, storage, box, owner, server, otherServer
}

func newTransferServiceForTest(t *testing.T, db *ent.Client, storage *Storage, box *secrets.Box) *Service {
	t.Helper()
	service, err := NewService(t.Context(), db, storage, box,
		transferDialerFunc(func(context.Context, string, string, uint16) (net.Conn, error) { return nil, errors.New("unused") }),
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }),
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Errorf("Service.Close: %v", err)
		}
	})
	return service
}

func newLoopbackTransferService(t *testing.T, db *ent.Client, storage *Storage, box *secrets.Box, ownerID, clientID, serverID string) *Service {
	t.Helper()
	var service *Service
	dialer := transferDialerFunc(func(ctx context.Context, gotOwnerID, gotClientID string, port uint16) (net.Conn, error) {
		if gotOwnerID != ownerID || gotClientID != clientID || port != ReservedPort {
			return nil, errors.New("unexpected transfer dial target")
		}
		return handlerDial(t, service.ReservedHandler(serverID))(ctx)
	})
	var err error
	service, err = NewService(t.Context(), db, storage, box, dialer,
		transferAuditFunc(func(context.Context, audit.Entry) error { return nil }),
		transferPublisherFunc(func(string, events.Envelope) {}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Errorf("Service.Close: %v", err)
		}
	})
	return service
}

func handlerDial(t *testing.T, handler func(context.Context, net.Conn)) protocolDial {
	t.Helper()
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go handler(ctx, server)
		return client, nil
	}
}
