package indexer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func pointPublicationFixture(t *testing.T) (string, *Indexer, *Watcher) {
	t.Helper()
	root, idx, w := inertTestWatcher(t, "edit.go", "package p\n\nfunc Before() {}\n")
	// Explicit queue admission exercises the real executor without starting an
	// fsnotify loop. Stop must still join every timer/patch and close its handle.
	w.degradedNoFsnotify = true
	t.Cleanup(func() {
		if !w.mutationAdmissionStopped() {
			require.NoError(t, w.Stop())
		}
	})
	path := filepath.Join(root, "edit.go")
	bumpMtime(t, path, "package p\n\nfunc After() {}\n")
	return path, idx, w
}

func newPointPublication(t *testing.T, name string) *PublicationPhaseRecord {
	t.Helper()
	return NewPublicationPhaseRecorder(1, 1).Begin("", name, PublicationSourceMCPEdit, time.Now())
}

func requirePointExecution(t *testing.T, record *PublicationPhaseRecord, want bool) {
	t.Helper()
	offsets := phaseOffsets(record.Snapshot())
	for _, phase := range []PublicationPhase{PublicationCycleStarted, PublicationPatchExecutionStarted, PublicationPatchExecutionFinished} {
		if want {
			require.Contains(t, offsets, phase)
		} else {
			require.NotContains(t, offsets, phase, "rejected/shared attempts must not publish receipt-owned spans")
		}
	}
	if want {
		require.LessOrEqual(t, offsets[PublicationPatchExecutionStarted], offsets[PublicationPatchExecutionFinished])
	}
}

func awaitPublicationSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("point execution did not reach its controlled boundary")
	}
}

func TestCanonicalPublicationObservesRealStandaloneExecution(t *testing.T) {
	path, idx, w := pointPublicationFixture(t)
	record := newPointPublication(t, "standalone")
	ticket, err := w.EnqueueFileMutation(WithPublicationRecord(t.Context(), record), path)
	require.NoError(t, err)
	result := awaitMutationRetryResult(t, ticket)
	require.NoError(t, result.Err)
	require.NotNil(t, nodeByName(t, idx, "edit.go", "After"), "a real parse/apply must have occurred")
	requirePointExecution(t, record, true)
	require.Equal(t, ticket.Generation, record.Snapshot().Ticket)
	require.Zero(t, record.Snapshot().DirtyGenerationID)
}

func TestCanonicalPublicationObservesMultiWatcherExecutor(t *testing.T) {
	mw, mi, root, _ := setupMultiWatcherTest(t)
	for _, w := range mw.watchers {
		w.degradedNoFsnotify = true
		t.Cleanup(func() {
			if !w.mutationAdmissionStopped() {
				require.NoError(t, w.Stop())
			}
		})
	}
	w := mw.watchers["repo-a"]
	require.NotNil(t, w.pointReindexRaw, "production MultiWatcher must install the daemon's MultiIndexer executor")
	path := filepath.Join(root, "main.go")
	bumpMtime(t, path, "package main\n\nfunc ChangedByDaemonPath() {}\n")
	record := newPointPublication(t, "multi")
	ticket, err := w.EnqueueFileMutation(WithPublicationRecord(t.Context(), record), path)
	require.NoError(t, err)
	require.NoError(t, awaitMutationRetryResult(t, ticket).Err)
	require.NotNil(t, nodeByName(t, mi.GetIndexer("repo-a"), "repo-a/main.go", "ChangedByDaemonPath"))
	requirePointExecution(t, record, true)
}

func TestCanonicalPublicationCommitsOnlyGuardedWinningAttempt(t *testing.T) {
	for _, nativeSuccessor := range []bool{false, true} {
		t.Run(fmt.Sprintf("native_successor=%t", nativeSuccessor), func(t *testing.T) {
			path, _, w := pointPublicationFixture(t)
			beforeComplete, release := make(chan struct{}), make(chan struct{})
			var releaseOnce atomic.Bool
			t.Cleanup(func() {
				if releaseOnce.CompareAndSwap(false, true) {
					close(release)
				}
			})
			w.mutationBeforeComplete = func(_ string, generation uint64) {
				if generation == 1 {
					close(beforeComplete)
					<-release
				}
			}
			older := newPointPublication(t, "older")
			first, err := w.EnqueueFileMutation(WithPublicationRecord(t.Context(), older), path)
			require.NoError(t, err)
			awaitPublicationSignal(t, beforeComplete)
			requirePointExecution(t, older, false)
			bumpMtime(t, path, "package p\n\nfunc Newer() {}\n")
			newer := newPointPublication(t, "newer")
			ctx := t.Context()
			if !nativeSuccessor {
				ctx = WithPublicationRecord(ctx, newer)
			}
			second, err := w.EnqueueFileMutation(ctx, path)
			require.NoError(t, err)
			if releaseOnce.CompareAndSwap(false, true) {
				close(release)
			}
			for _, ticket := range []*MutationTicket{first, second} {
				result := awaitMutationRetryResult(t, ticket)
				require.NoError(t, result.Err)
				require.Equal(t, second.Generation, result.AppliedGeneration)
			}
			requirePointExecution(t, older, false)
			requirePointExecution(t, newer, !nativeSuccessor)
			require.Equal(t, first.Generation, older.Snapshot().Ticket)
		})
	}
}

func TestCanonicalPublicationDiscardsRetryAttemptSpans(t *testing.T) {
	path, idx, w := pointPublicationFixture(t)
	failedAt := make(chan time.Time, 1)
	secondEntered, releaseSecond := make(chan struct{}), make(chan struct{})
	var released atomic.Bool
	t.Cleanup(func() {
		if released.CompareAndSwap(false, true) {
			close(releaseSecond)
		}
	})
	var calls atomic.Int32
	w.mutationRetryDelayFn = func(int) time.Duration { return time.Millisecond }
	w.pointReindexRaw = func(file string, _ *PublicationStamps) (*IndexResult, error) {
		if calls.Add(1) == 1 {
			failedAt <- time.Now()
			return nil, fmt.Errorf("busy: %w", context.DeadlineExceeded)
		}
		close(secondEntered)
		<-releaseSecond
		return idx.incrementalPointWatcherPath(idx.rootPath, file)
	}
	origin := time.Now()
	record := NewPublicationPhaseRecorder(1, 1).Begin("", "retry", PublicationSourceMCPEdit, origin)
	ticket, err := w.EnqueueFileMutation(WithPublicationRecord(t.Context(), record), path)
	require.NoError(t, err)
	awaitPublicationSignal(t, secondEntered)
	firstFailed := <-failedAt
	requirePointExecution(t, record, false)
	if released.CompareAndSwap(false, true) {
		close(releaseSecond)
	}
	require.NoError(t, awaitMutationRetryResult(t, ticket).Err)
	requirePointExecution(t, record, true)
	require.EqualValues(t, 2, calls.Load())
	require.GreaterOrEqual(t, phaseOffsets(record.Snapshot())[PublicationPatchExecutionStarted], firstFailed.Sub(origin).Nanoseconds())
}

func TestCanonicalPublicationDiscardsStoppedInFlightAttempt(t *testing.T) {
	path, idx, w := pointPublicationFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var released atomic.Bool
	t.Cleanup(func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	})
	w.pointReindexRaw = func(file string, _ *PublicationStamps) (*IndexResult, error) {
		close(entered)
		<-release
		return idx.incrementalPointWatcherPath(idx.rootPath, file)
	}
	record := newPointPublication(t, "stop")
	ticket, err := w.EnqueueFileMutation(WithPublicationRecord(t.Context(), record), path)
	require.NoError(t, err)
	awaitPublicationSignal(t, entered)
	stopped := make(chan error, 1)
	go func() { stopped <- w.Stop() }()
	require.ErrorIs(t, awaitMutationRetryResult(t, ticket).Err, errWatcherStopped)
	requirePointExecution(t, record, false)
	if released.CompareAndSwap(false, true) {
		close(release)
	}
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not join the released point attempt")
	}
	requirePointExecution(t, record, false)
}

func TestCanonicalPublicationDiscardsFailedExecutionSpans(t *testing.T) {
	path, _, w := pointPublicationFixture(t)
	failure := errors.New("point executor rejected the patch")
	var calls atomic.Int32
	w.pointReindexRaw = func(string, *PublicationStamps) (*IndexResult, error) {
		calls.Add(1)
		return nil, failure
	}
	record := newPointPublication(t, "failed")
	ticket, err := w.EnqueueFileMutation(WithPublicationRecord(t.Context(), record), path)
	require.NoError(t, err)
	require.ErrorIs(t, awaitMutationRetryResult(t, ticket).Err, failure)
	require.EqualValues(t, 1, calls.Load(), "permanent failures must not be retried")
	requirePointExecution(t, record, false)
}

func TestPointPublicationPreservesLaneContextAndNativeAbstention(t *testing.T) {
	w := newMutationRetryTestWatcher(t)
	w.config.DebounceMs = int(time.Hour / time.Millisecond)
	ticket := w.scheduleFileMutation("native.go", ChangeModified)
	require.Nil(t, w.beginPointPublication("native.go", ticket.Generation))
	require.True(t, w.completeMutationWaitersIfCurrent("native.go", ticket.Generation, nil))
	require.NoError(t, awaitMutationRetryResult(t, ticket).Err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	attempt := &pointPublicationAttempt{record: newPointPublication(t, "context"), stamps: &PublicationStamps{}}
	observed := attempt.withContext(ctx)
	wantDeadline, _ := ctx.Deadline()
	gotDeadline, ok := observed.Deadline()
	require.True(t, ok)
	require.Equal(t, wantDeadline, gotDeadline)
	cancel()
	require.ErrorIs(t, observed.Err(), context.Canceled)
	require.Nil(t, phaseRecordFrom(observed), "a collector context must not live-publish into a receipt")
}
