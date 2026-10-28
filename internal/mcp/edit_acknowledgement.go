package mcp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// This is a call-scoped private observation, not retained receipt history. In
// particular it must not be stored in the unbounded batchTransactions map.
// Its origin precedes boundHandler, not the dispatcher/transport or the legacy
// publication origin at middleware entry. No graph work is inferred from it.
type editAcknowledgement struct {
	owner   *Server
	id      uint64
	entered time.Time
	mu      sync.Mutex
	actual  time.Time
	reply   time.Time
	outcome string
	laps    editAcknowledgementLaps
}

type editAcknowledgementKey struct{}

var editAcknowledgementSequence atomic.Uint64

// A status call may borrow this pointer long enough to copy a snapshot. It
// never installs one: eviction clears the record's ownership even when other
// pending-source evidence still retains the physical commit record.
func retainedMutationAcknowledgement(record *mutationCommitRecord) *editAcknowledgement {
	if !record.isPhysicalCommit() {
		return nil
	}
	record.mu.RLock()
	defer record.mu.RUnlock()
	return record.acknowledgement
}

// The two walls deliberately overlap: they are diagnostic alternatives, never
// additive laps. A detached handler can finish after the reply wall froze.
// Snapshot values own their scalar pointers; no encoder samples a live clock.
type editAcknowledgementSnapshot struct {
	RPCID                uint64
	Scope                string
	ReplyWallMS          *float64
	ReplyState           string
	ReplyOutcome         string
	ActualWallMS         *float64
	ActualState          string
	ActualFinishedLate   bool
	DispatcherQueueMS    *float64
	DispatcherQueueState string
	DispatcherPreambleMS *float64
	DispatcherState      string
	ParseGate            editAcknowledgementLapSnapshot
	DiskWrite            editAcknowledgementLapSnapshot
	TicketAdmission      editAcknowledgementLapSnapshot
	ReindexWait          editAcknowledgementLapSnapshot
	LapsDisjoint         bool
}

func editAcknowledgementFrom(ctx context.Context, owner *Server) *editAcknowledgement {
	if ctx == nil {
		return nil
	}
	ack, _ := ctx.Value(editAcknowledgementKey{}).(*editAcknowledgement)
	if ack != nil && ack.owner == owner {
		return ack
	}
	return nil
}

// withEditAcknowledgement is called only after the existing measured-source
// classifier succeeded. A same-Server nested raw/facade call borrows the RPC
// identity; another Server cannot complete or freeze that owner's record.
func (s *Server) withEditAcknowledgement(ctx context.Context) (context.Context, *editAcknowledgement, bool) {
	if existing := editAcknowledgementFrom(ctx, s); existing != nil {
		return ctx, existing, false
	}
	ack := &editAcknowledgement{owner: s, id: editAcknowledgementSequence.Add(1), entered: time.Now()}
	return context.WithValue(ctx, editAcknowledgementKey{}, ack), ack, true
}

func (a *editAcknowledgement) finishActual() {
	a.finishActualAt(time.Now())
}

func (a *editAcknowledgement) finishActualAt(at time.Time) {
	a.mu.Lock()
	if a.actual.IsZero() {
		a.actual = at
	}
	a.mu.Unlock()
}

func (a *editAcknowledgement) finishReply(result *mcp.CallToolResult, err error) {
	outcome := "result"
	switch {
	case err != nil:
		outcome = "go_error"
	case result == nil:
		outcome = "no_result"
	case result.IsError:
		outcome = "tool_error"
	}
	a.finishReplyAt(time.Now(), outcome)
}

func (a *editAcknowledgement) finishReplyAt(at time.Time, outcome string) {
	a.mu.Lock()
	if a.reply.IsZero() {
		a.reply, a.outcome = at, outcome
	}
	a.mu.Unlock()
}

func (a *editAcknowledgement) snapshot() editAcknowledgementSnapshot {
	a.mu.Lock()
	actual, reply, outcome, laps := a.actual, a.reply, a.outcome, a.laps
	a.mu.Unlock()
	result := editAcknowledgementSnapshot{
		RPCID: a.id, Scope: "mcp_bound_call", ReplyOutcome: outcome,
		DispatcherQueueState: "unavailable_dispatcher_interface",
		DispatcherState:      "unavailable_dispatcher_interface",
	}
	result.ReplyWallMS, result.ReplyState = editAcknowledgementWall(a.entered, reply)
	result.ActualWallMS, result.ActualState = editAcknowledgementWall(a.entered, actual)
	result.ActualFinishedLate = result.ReplyState == "measured" && result.ActualState == "measured" && actual.After(reply)
	result.ParseGate = laps.snapshot(editAckParseGate, a.entered, reply, actual)
	result.DiskWrite = laps.snapshot(editAckDiskWrite, a.entered, reply, actual)
	result.TicketAdmission = laps.snapshot(editAckTicketAdmission, a.entered, reply, actual)
	result.ReindexWait = laps.snapshot(editAckReindexWait, a.entered, reply, actual)
	result.LapsDisjoint = !laps.overlap
	return result
}

func editAcknowledgementWall(start, end time.Time) (*float64, string) {
	if start.IsZero() {
		return nil, "unavailable_origin"
	}
	if end.IsZero() {
		return nil, "pending"
	}
	if end.Before(start) {
		return nil, "invalid_order"
	}
	ms := float64(end.Sub(start)) / float64(time.Millisecond)
	return &ms, "measured"
}
