package mcp

import (
	"context"
	"crypto/sha256"
	"strconv"
)

// bindBatchAcknowledgement belongs exclusively to runBatchTransaction's first
// execute arm. Duplicate/status/recovery calls must not bind their own clocks.
// No timing pointer or payload is retained by the unbounded transaction map.
func (s *Server) bindBatchAcknowledgement(ctx context.Context, state *batchTransactionState) {
	ack := editAcknowledgementFrom(ctx, s)
	if ack == nil {
		return
	}
	state.mu.Lock()
	if state.acknowledgementReceipt != "" {
		state.mu.Unlock()
		return
	}
	id := "batch-ack-" + strconv.FormatUint(ack.id, 10)
	transactionID := state.receipt.TransactionID
	state.acknowledgementReceipt = id
	state.mu.Unlock()
	// No state lock crosses the existing ledger's eviction or record locks.
	// A racing reader between scalar reservation and this put reports missing
	// evidence, never an invented wall. No work is awakened by this helper.
	s.mutationCommits.put(&mutationCommitRecord{
		kind: batchAcknowledgementCommit, id: id, startedAt: ack.entered,
		acknowledgement: ack, batchOwner: sha256.Sum256([]byte(transactionID)),
	})
}

type batchAcknowledgementSnapshot struct {
	Reference string
	State     string
	Snapshot  *editAcknowledgementSnapshot
}

func (s *Server) batchAcknowledgement(state *batchTransactionState) batchAcknowledgementSnapshot {
	state.mu.RLock()
	id, transactionID := state.acknowledgementReceipt, state.receipt.TransactionID
	state.mu.RUnlock()
	result := batchAcknowledgementSnapshot{Reference: id, State: "unavailable_reference"}
	if id == "" {
		return result // restarted/raw/legacy transactions do not acquire a new origin
	}
	owner := sha256.Sum256([]byte(transactionID))
	ledger := &s.mutationCommits
	ledger.mu.Lock()
	record := ledger.byID[id]
	var ack *editAcknowledgement
	if record != nil && record.kind == batchAcknowledgementCommit && record.batchOwner == owner {
		record.mu.RLock()
		ack = record.acknowledgement
		record.mu.RUnlock()
	}
	ledger.mu.Unlock()
	result.State = "unavailable_ledger_entry"
	if ack == nil {
		return result
	}
	// Borrow only long enough to produce this owned snapshot. No callback,
	// formatter or encoder executes while the ledger/record locks are held.
	snapshot := ack.snapshot()
	result.State, result.Snapshot = "observed", &snapshot
	return result
}
