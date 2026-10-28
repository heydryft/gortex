package mcp

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/zzet/gortex/internal/indexer"
)

func newReadRepairWitnessFixture(t *testing.T) (*Server, *mutationCommitRecord, mutationReadRepairWitness, *indexer.PublicationPhaseRecorder) {
	t.Helper()
	s := newMutationServer()
	phases := indexer.NewPublicationPhaseRecorder(4, 1)
	now := time.Now()
	publication := phases.Begin("checkout", "publication", indexer.PublicationSourceMCPEdit, now)
	record := &mutationCommitRecord{
		owner: s, id: "commit", fingerprint: "edit fingerprint", absPath: "/repo/file.go",
		disk: mutationDiskCommitted, graph: mutationGraphPending, graphRecorded: true,
		commitSeq: 1, newSHA: gitBlobSHA([]byte("committed bytes")), startedAt: now, committedAt: now,
		reindexReceipt: "publication", reindexGeneration: 1, checkoutID: "checkout", checkoutIncarnation: "incarnation",
		publication: publication,
	}
	s.mutationCommits.put(record)
	admission := &mutationReceipt{id: record.reindexReceipt, generation: record.reindexGeneration, path: record.absPath}
	s.mutationReceipts.Store(admission.id, admission)
	record.readRepairAdmission = weak.Make(admission)
	witness := mutationReadRepairWitness{
		server: weak.Make(s), record: weak.Make(record), publication: weak.Make(publication),
		id: record.id, fingerprint: record.fingerprint, absPath: record.absPath,
		blobSHA: record.newSHA, receipt: record.reindexReceipt, checkout: record.checkoutID,
		incarnation: record.checkoutIncarnation, commitSeq: record.commitSeq,
		generation: record.reindexGeneration, committedAt: now,
		admission: weak.Make(admission),
	}
	return s, record, witness, phases
}

// Reuse the existing canonical-ticket adapter, but expose its actual enqueue
// boundary so the test can hold Done through reader admission and acceptance.
type readRepairHeldWatcher struct {
	canonicalPublicationWatcher
	enqueued chan struct{}
}

func (w *readRepairHeldWatcher) EnqueueFileMutation(ctx context.Context, path string) (*indexer.MutationTicket, error) {
	ticket, err := w.canonicalPublicationWatcher.EnqueueFileMutation(ctx, path)
	close(w.enqueued)
	return ticket, err
}

func TestReadRepairSeesCommittedAdmissionBeforeACKAndSurvivesSuccessClear(t *testing.T) {
	s := newMutationServer()
	s.mutationReindexWait = 30 * time.Second
	w := &readRepairHeldWatcher{canonicalPublicationWatcher: canonicalPublicationWatcher{hold: true}, enqueued: make(chan struct{})}
	s.watcher = w
	ctx, cancel := context.WithCancel(canonicalTimingContext())
	t.Cleanup(cancel)
	path := filepath.Join(t.TempDir(), "edit.go")
	record := commitTimingFixture(t, s, ctx, path, "package edited\n")
	idx := &indexer.Indexer{}
	outcome := make(chan mutationReindexOutcome, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		outcome <- s.mutationReindexState(ctx, path)
	}()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			select {
			case <-w.enqueued:
				w.results[0] <- indexer.MutationResult{RequestedGeneration: 1, AppliedGeneration: 1, Reindexed: true}
				close(w.results[0])
			case <-time.After(5 * time.Second):
				cancel()
			}
		})
	}
	t.Cleanup(func() {
		release()
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("held mutation waiter was not reaped")
		}
	})
	require.Eventually(t, func() bool { return s.readRepairObserver(ctx, path, idx) != nil }, 5*time.Second, time.Millisecond,
		"a pre-ACK reader must see the real admitted ticket, not post-wait metadata")
	pending := record.snapshot()
	require.Empty(t, pending.ReindexReceipt)
	require.Zero(t, pending.ReindexGeneration, "05ab's required nonzero post-wait generation is absent at this real boundary")
	require.False(t, pending.GraphRecorded)
	select {
	case <-outcome:
		t.Fatal("Done was not held through the reader observation")
	default:
	}
	witness, ok := s.captureReadRepairWitness(path, idx)
	require.True(t, ok)
	witness.bind(mutationReadRepairObservation{generation: 2, windowNS: 10})
	accepted := record.snapshot()
	require.NotNil(t, accepted.readRepair)
	release()
	var result mutationReindexOutcome
	select {
	case result = <-outcome:
	case <-time.After(5 * time.Second):
		t.Fatal("actual ticket completion did not release the original wait")
	}
	require.True(t, result.Reindexed)
	require.False(t, result.Pending)
	require.Empty(t, result.Receipt, "immediate-success public output must still clear its receipt")
	record.recordGraph(result)
	late := record.snapshot()
	require.Empty(t, late.ReindexReceipt)
	require.EqualValues(t, 1, late.ReindexGeneration)
	require.NotNil(t, late.readRepair, "expected initial metadata fill/Receipt clearing must not erase accepted R")
	require.Equal(t, accepted.readRepair, late.readRepair)
	record.recordGraph(mutationReindexOutcome{Receipt: "foreign", Generation: 99, Pending: true})
	require.Nil(t, record.snapshot().readRepair, "actual foreign rebind must still clear R")
	require.EqualValues(t, 2, accepted.readRepair.generation, "prior snapshots remain immutable")
	runtime.KeepAlive(idx)
}

func TestReadRepairBindingRejectsEvictionRebindAndNewerCommit(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Server, *mutationCommitRecord, *indexer.PublicationPhaseRecorder)
	}{
		{"evicted", func(s *Server, _ *mutationCommitRecord, _ *indexer.PublicationPhaseRecorder) {
			s.mutationCommits.mu.Lock()
			s.mutationCommits.evictLocked(time.Now().Add(mutationCommitRetention + time.Second))
			s.mutationCommits.mu.Unlock()
		}},
		{"receipt_rebound", func(_ *Server, r *mutationCommitRecord, _ *indexer.PublicationPhaseRecorder) {
			r.reindexReceipt = "replacement"
		}},
		{"generation_rebound", func(_ *Server, r *mutationCommitRecord, _ *indexer.PublicationPhaseRecorder) { r.reindexGeneration++ }},
		{"incarnation_rebound", func(_ *Server, r *mutationCommitRecord, _ *indexer.PublicationPhaseRecorder) {
			r.checkoutIncarnation = "replacement"
		}},
		{"newer_bytes", func(_ *Server, r *mutationCommitRecord, _ *indexer.PublicationPhaseRecorder) {
			r.newSHA = gitBlobSHA([]byte("newer bytes"))
		}},
		{"newer_commit", func(s *Server, r *mutationCommitRecord, _ *indexer.PublicationPhaseRecorder) {
			s.mutationCommits.put(&mutationCommitRecord{owner: s, id: "newer", absPath: r.absPath, startedAt: time.Now()})
		}},
		{"publication_rebound", func(_ *Server, r *mutationCommitRecord, phases *indexer.PublicationPhaseRecorder) {
			r.publication = phases.Begin("checkout", "replacement", indexer.PublicationSourceMCPEdit, time.Now())
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, record, witness, phases := newReadRepairWitnessFixture(t)
			test.change(s, record, phases)
			witness.bind(mutationReadRepairObservation{generation: 2, windowNS: 10})
			if record.snapshot().readRepair != nil {
				t.Fatal("unowned/rebound mutation gained repair evidence")
			}
			runtime.KeepAlive(s)
			runtime.KeepAlive(phases)
		})
	}
}

func TestReadRepairAcceptedHistoryIsImmutableAfterLaterEdit(t *testing.T) {
	s, record, witness, phases := newReadRepairWitnessFixture(t)
	witness.bind(mutationReadRepairObservation{
		generation: 2, windowNS: 10, causeCount: 1,
		causes: [8]mutationReadRepairCause{{Session: "session", RequestIDKey: "n:7", AdmissionGeneration: 2}},
	})
	early := record.snapshot()
	if early.readRepair == nil || early.readRepair.generation != 2 {
		t.Fatal("accepted actual execution unavailable")
	}
	s.mutationCommits.put(&mutationCommitRecord{owner: s, id: "later", absPath: record.absPath, startedAt: time.Now()})
	witness.bind(mutationReadRepairObservation{generation: 3, windowNS: 20})
	late := record.snapshot()
	if late.readRepair == nil || late.readRepair.generation != 2 || early.readRepair.windowNS != 10 {
		t.Fatal("later event erased or rebound accepted historical evidence")
	}
	late.readRepair.causes[0].Session = "snapshot mutation"
	if record.snapshot().readRepair.causes[0].Session != "session" || early.readRepair.causes[0].Session != "session" {
		t.Fatal("mutable snapshot escaped into retained evidence")
	}
	runtime.KeepAlive(s)
	runtime.KeepAlive(phases)
}

func TestReadRepairUnobservedLedgerDoesNotAllocate(t *testing.T) {
	s := newMutationServer()
	idx := &indexer.Indexer{}
	ctx := t.Context()
	if allocations := testing.AllocsPerRun(100, func() {
		if s.readRepairObserver(ctx, "/repo/file.go", idx) != nil {
			panic("unobserved path acquired a witness")
		}
	}); allocations != 0 {
		t.Fatalf("unobserved allocations: %v", allocations)
	}
}

func TestReadRepairLedgerRetentionDoesNotRestoreCanonicalEvidence(t *testing.T) {
	s, record, witness, phases := newReadRepairWitnessFixture(t)
	witness.bind(mutationReadRepairObservation{generation: 2, windowNS: 10})
	early := record.snapshot()
	for i := range 5 {
		phases.Begin("checkout", "later-"+strconv.Itoa(i), indexer.PublicationSourceMCPEdit, time.Now())
	}
	if _, retained := phases.Lookup("publication"); retained {
		t.Fatal("fixture did not evict the original recorder entry")
	}
	if record.snapshot().readRepair == nil || record.publication.CoalescedPublication() != nil ||
		record.publication.Snapshot().EditTimings().Window.PatchWallMS != nil {
		t.Fatal("recorder eviction changed R retention or R fabricated canonical P/B")
	}
	s.mutationCommits.mu.Lock()
	s.mutationCommits.evictLocked(time.Now().Add(mutationCommitRetention + time.Second))
	s.mutationCommits.mu.Unlock()
	if record.snapshot().readRepair != nil || early.readRepair == nil || early.readRepair.windowNS != 10 {
		t.Fatal("ledger eviction retained live R or mutated an earlier immutable snapshot")
	}
	witness.bind(mutationReadRepairObservation{generation: 3, windowNS: 20})
	if record.snapshot().readRepair != nil {
		t.Fatal("late completion resurrected ledger-evicted repair evidence")
	}
	runtime.KeepAlive(s)
	runtime.KeepAlive(phases)
}
