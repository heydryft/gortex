package indexer

import (
	"context"
	"time"
)

// pointPublicationAttempt is local to one runnable point attempt. Failed,
// retried, stopped and superseded attempts never publish this collector into
// a receipt. The guarded existing waiter completion accepts only its exact
// winning record, outside the watcher lock and before Done delivery.
type pointPublicationAttempt struct {
	record *PublicationPhaseRecord
	stamps *PublicationStamps
}

func (w *Watcher) beginPointPublication(path string, generation uint64) *pointPublicationAttempt {
	w.mu.Lock()
	var record *PublicationPhaseRecord
	if !w.stopping && w.pendingGeneration[path] == generation {
		record = w.mutationWaiters[path][generation].publication
	}
	w.mu.Unlock()
	if record == nil {
		return nil // no tracing allocations on native/uninstrumented paths
	}
	stamps := record.newCanonicalPatchStamps()
	stamps.markAt(PublicationCycleStarted, time.Now())
	return &pointPublicationAttempt{record: record, stamps: stamps}
}

func (p *pointPublicationAttempt) withContext(ctx context.Context) context.Context {
	if p == nil {
		return ctx
	}
	// Keep the existing lane cancellation and deadline. Only the attempt's
	// collector travels; attaching a live record would publish rejected spans.
	return context.WithValue(ctx, publicationStampsKey{}, p.stamps)
}

func markPointPublication(ctx context.Context, phase PublicationPhase) time.Time {
	if stamps := PublicationStampsFrom(ctx); stamps != nil {
		at := time.Now()
		stamps.markAt(phase, at)
		return at
	}
	return time.Time{}
}
