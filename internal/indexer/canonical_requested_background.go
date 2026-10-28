package indexer

import (
	"context"
	"math"
	"sync"
	"time"
	"weak"
)

type canonicalSchedulingKey struct{}

// Preserve the outer scheduler's actual entry across MultiWatcher routing.
// Native/unobserved admission performs no clock read or context allocation.
func canonicalSchedulingEntry(ctx context.Context) (context.Context, time.Time) {
	if entered, ok := ctx.Value(canonicalSchedulingKey{}).(time.Time); ok {
		return ctx, entered
	}
	if PublicationRecordFromContext(ctx) == nil {
		return ctx, time.Time{}
	}
	entered := time.Now()
	return context.WithValue(ctx, canonicalSchedulingKey{}, entered), entered
}

// Fixed state for one requested timer successfully canceled before callback,
// followed by exactly one native successor. No event or interval history.
type canonicalBackgroundOwner struct {
	mu        sync.Mutex
	record    weak.Pointer[PublicationPhaseRecord]
	requested uint64
	points    [9]time.Time
	invalid   bool
}

func newCanonicalBackground(record *PublicationPhaseRecord, requested uint64, entered time.Time) *canonicalBackgroundOwner {
	if record == nil || requested == 0 || entered.IsZero() {
		return nil
	}
	out := &canonicalBackgroundOwner{record: weak.Make(record), requested: requested}
	out.points[0] = entered
	return out
}

func (b *canonicalBackgroundOwner) invalidate() { b.mu.Lock(); b.invalid = true; b.mu.Unlock() }
func (b *canonicalBackgroundOwner) armStarted(successor bool, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	index := 1
	if successor {
		index = 5
	}
	if !b.points[index].IsZero() {
		b.invalid = true
	}
	b.points[index] = at
}
func (b *canonicalBackgroundOwner) armFinished(successor bool, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	index := 2
	if successor {
		index = 6
	}
	if !b.points[index].IsZero() {
		b.invalid = true
	}
	b.points[index] = at
}
func (b *canonicalBackgroundOwner) stopped(successor bool, start, end time.Time, stopped, native bool) *canonicalBackgroundOwner {
	b.mu.Lock()
	defer b.mu.Unlock()
	if successor || !stopped || !native || !b.points[3].IsZero() || !b.points[7].IsZero() {
		b.invalid = true
	}
	b.points[3], b.points[4] = start, end
	if b.invalid {
		return nil
	}
	return b
}
func (b *canonicalBackgroundOwner) callback(successor bool, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !successor || !b.points[7].IsZero() {
		b.invalid = true
	}
	b.points[7] = at
}

type canonicalBackgroundEvidence struct {
	origin             time.Time
	points             [9]int64
	requested, applied uint64
	physical           canonicalAttemptWindow
	debounce           canonicalDebounceEvidence
}

func (b *canonicalBackgroundOwner) finish(attempt *pointPublicationAttempt, entered, end time.Time, retry int) {
	if attempt == nil || !attempt.independent || retry != 0 {
		return
	}
	b.mu.Lock()
	if b.invalid || b.points[7] != entered || !b.points[8].IsZero() {
		b.mu.Unlock()
		return
	}
	b.points[8] = end
	evidence := canonicalBackgroundEvidence{origin: b.points[0], requested: b.requested}
	for i, at := range b.points {
		if at.IsZero() || at.Before(b.points[0]) || i > 0 && at.Before(b.points[i-1]) {
			b.mu.Unlock()
			return
		}
		evidence.points[i] = at.Sub(b.points[0]).Nanoseconds()
	}
	b.mu.Unlock()
	record := b.record.Value()
	if record == nil {
		return
	}
	owner := record.owner.Value()
	if owner == nil || attempt.record.owner.Value() != owner {
		return
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	winner := attempt.record
	winner.mu.Lock()
	defer winner.mu.Unlock()
	if owner.byKey[record.key] != record || owner.byKey[winner.key] != winner || record.ticket != b.requested ||
		record.coalescedWinner.Value() != winner || record.background != nil || winner.attemptWindow == nil || winner.debounce == nil || winner.ticket <= b.requested {
		return
	}
	for _, mark := range record.marks {
		if mark.phase == PublicationTicketFailed {
			return
		}
	}
	evidence.applied = winner.ticket
	evidence.physical = *winner.attemptWindow
	evidence.debounce = *winner.debounce
	if !winner.origin.Add(time.Duration(evidence.physical.startNS)).Equal(entered) || !winner.origin.Add(time.Duration(evidence.physical.endNS)).Equal(end) || !evidence.valid() {
		return
	}
	record.background = &evidence // fresh immutable late scalar, no earlier snapshot mutation
}

func (b canonicalBackgroundEvidence) valid() bool {
	if b.origin.IsZero() || b.requested == 0 || b.applied <= b.requested || b.points[0] != 0 {
		return false
	}
	for i, v := range b.points {
		if v < 0 || i > 0 && v < b.points[i-1] {
			return false
		}
	}
	p := b.physical.evaluateAccounting()
	if !p.complete || b.physical.retry != 0 || b.physical.ticket != b.applied || b.physical.endNS-b.physical.startNS != b.points[8]-b.points[7] {
		return false
	}
	if !b.debounce.entered || b.debounce.startedNS < 0 || b.debounce.enteredNS != b.physical.startNS || b.debounce.enteredNS-b.debounce.startedNS != b.points[7]-b.points[5] {
		return false
	}
	sum := int64(0)
	for i := 1; i < 8; i++ {
		delta := b.points[i] - b.points[i-1]
		if sum > math.MaxInt64-delta {
			return false
		}
		sum += delta
	}
	if p.sumNS < 0 || sum > math.MaxInt64-p.sumNS {
		return false
	}
	return sum+p.sumNS == b.points[8]
}

var requestedBackgroundLapNames = [...]string{"scheduling_admission_setup", "initial_timer_arm", "canceled_quiet_wait", "timer_stop", "coalescing_control", "successor_timer_arm", "successor_quiet_wait"}

// RequestedBackgroundTiming is one explicitly measured scheduler-entry through
// serving-return window. It excludes earlier parsing/disk work and may overlap A.
// Only the one-canceled-timer/one-successor topology is supported here.
type RequestedBackgroundTiming struct {
	Scope               string             `json:"scope"`
	Coverage            string             `json:"coverage"`
	SumState            string             `json:"sum_state"`
	OriginWall          string             `json:"origin_wall"`
	RequestedGeneration uint64             `json:"requested_generation"`
	AppliedGeneration   uint64             `json:"applied_generation"`
	WallMS              float64            `json:"background_wall_ms"`
	PhasesMS            map[string]float64 `json:"phases_ms"`
	PartitionComplete   bool               `json:"observed_partition_complete"`
	UnattributedMS      float64            `json:"unattributed_ms"`
}

func (b canonicalBackgroundEvidence) project(physical *EditPatchAccounting) *RequestedBackgroundTiming {
	// This immutable evidence was fully validated at publication. The related
	// snapshot additionally proves it still names this exact physical value.
	if physical == nil || !physical.PartitionComplete {
		return nil
	}
	out := &RequestedBackgroundTiming{Scope: "accepted_scheduling_call_entry_to_serving_attempt_return", Coverage: "one_canceled_timer_one_native_successor", SumState: "partial", OriginWall: b.origin.UTC().Format(time.RFC3339Nano), RequestedGeneration: b.requested, AppliedGeneration: b.applied, WallMS: float64(b.points[8]) / 1e6, PhasesMS: make(map[string]float64, 7+canonicalPatchLapCount), PartitionComplete: true}
	for i, name := range requestedBackgroundLapNames {
		out.PhasesMS[name] = float64(b.points[i+1]-b.points[i]) / 1e6
	}
	for name, value := range physical.PhasesMS {
		if value != nil {
			out.PhasesMS[name] = *value
		}
	}
	return out
}

func (p *PublicationPhaseRecord) RequestedBackground() *RequestedBackgroundTiming {
	if related := p.CoalescedPublication(); related != nil {
		return related.Background
	}
	return nil
}
