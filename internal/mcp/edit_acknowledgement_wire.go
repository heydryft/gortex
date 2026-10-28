package mcp

import (
	"strconv"

	"github.com/zzet/gortex/internal/indexer"
)

type acknowledgementLapCount struct {
	Started       uint64 `json:"started"`
	Completed     uint64 `json:"completed"`
	NotApplicable uint64 `json:"not_applicable"`
}

// Acknowledgement and patch walls have different owners and can overlap. The
// shared RPC identity lets a consumer deduplicate several physical file
// receipts; this object never claims its subtotal is a complete critical path.
type acknowledgementPayload struct {
	State                    string                             `json:"state"`
	RPCID                    *string                            `json:"rpc_id"`
	Scope                    string                             `json:"scope"`
	Denominator              string                             `json:"denominator"`
	Clock                    string                             `json:"clock"`
	ReplyWallMS              *float64                           `json:"caller_response_ready_ms"`
	ReplyState               string                             `json:"caller_response_state"`
	ReplyOutcome             string                             `json:"caller_response_outcome,omitempty"`
	ActualWallMS             *float64                           `json:"actual_handler_ms"`
	ActualState              string                             `json:"actual_handler_state"`
	ActualFinishedLate       bool                               `json:"actual_finished_after_reply"`
	PhasesMS                 map[string]*float64                `json:"phases_ms"`
	PhaseStates              map[string]string                  `json:"phase_states"`
	Counts                   map[string]acknowledgementLapCount `json:"counts"`
	ObservedExecutionMS      map[string]*float64                `json:"observed_execution_ms"`
	Disjoint                 bool                               `json:"disjoint_observed_intervals"`
	SubtotalMS               *float64                           `json:"observed_ack_subtotal_ms"`
	UnattributedMS           *float64                           `json:"ack_unattributed_ms"`
	SumState                 string                             `json:"sum_state"`
	CombinedAckPatchSumState string                             `json:"combined_ack_patch_sum_state"`
	ExistingPhaseScope       string                             `json:"existing_ack_phase_scope"`
	PublicationResidualState string                             `json:"publication_residual_state"`
}

func buildAcknowledgementPayload(snapshot *editAcknowledgementSnapshot, state string) acknowledgementPayload {
	out := acknowledgementPayload{
		State: state, Scope: "shared_rpc_mcp_bound_call", Clock: "unavailable",
		Denominator: "bound_tool_entry_to_outer_return_before_transport_serialization",
		ReplyState:  state, ActualState: state, SumState: "unavailable",
		CombinedAckPatchSumState: "not_additive_overlapping_lifetimes",
		ExistingPhaseScope:       "legacy_per_receipt_publication", PublicationResidualState: "legacy_per_receipt_publication",
		PhasesMS: make(map[string]*float64, 9), PhaseStates: make(map[string]string, 9),
		Counts: make(map[string]acknowledgementLapCount, 4), ObservedExecutionMS: make(map[string]*float64, 4),
	}
	for _, name := range []string{"dispatcher_queue", "dispatcher_preamble", "middleware", "parse", "disk_write", "publish", "reindex_wait", "reply_prepare", "transport"} {
		out.PhasesMS[name], out.PhaseStates[name] = nil, state
	}
	if snapshot == nil {
		return out
	}
	out.State = "observed"
	id := "rpc-" + strconv.FormatUint(snapshot.RPCID, 10)
	out.RPCID, out.Clock = &id, "monotonic"
	out.ReplyWallMS, out.ReplyState, out.ReplyOutcome = snapshot.ReplyWallMS, snapshot.ReplyState, snapshot.ReplyOutcome
	out.ActualWallMS, out.ActualState = snapshot.ActualWallMS, snapshot.ActualState
	out.ActualFinishedLate, out.Disjoint = snapshot.ActualFinishedLate, snapshot.LapsDisjoint
	out.ExistingPhaseScope, out.PublicationResidualState = "shared_rpc", "unavailable_shared_rpc_scope"
	for _, name := range []string{"dispatcher_queue", "dispatcher_preamble"} {
		out.PhaseStates[name] = "unavailable_dispatcher_interface"
	}
	for _, name := range []string{"middleware", "reply_prepare", "transport"} {
		out.PhaseStates[name] = "unavailable_boundary"
	}
	var subtotal float64
	observed, invalid := 0, false
	for name, lap := range map[string]editAcknowledgementLapSnapshot{
		"parse": snapshot.ParseGate, "disk_write": snapshot.DiskWrite,
		"publish": snapshot.TicketAdmission, "reindex_wait": snapshot.ReindexWait,
	} {
		out.PhasesMS[name], out.PhaseStates[name] = lap.AckMS, lap.State
		out.Counts[name] = acknowledgementLapCount{lap.Started, lap.Completed, lap.NotApplicable}
		out.ObservedExecutionMS[name] = lap.ObservedMS
		if lap.Completed > lap.Started || lap.NotApplicable > lap.Completed ||
			((lap.AckMS != nil || lap.ObservedMS != nil) && (lap.Started == 0 || lap.Completed != lap.Started)) ||
			(lap.AckMS != nil && (lap.ObservedMS == nil || *lap.AckMS != *lap.ObservedMS)) {
			out.PhasesMS[name], out.ObservedExecutionMS[name] = nil, nil
			out.PhaseStates[name], invalid = "invalid_count", true
			continue
		}
		if lap.State == "invalid_interval" || lap.State == "outside_execution_window" {
			invalid = true
		}
		if lap.AckMS != nil {
			subtotal += *lap.AckMS
			observed++
		}
	}
	switch {
	case !snapshot.LapsDisjoint:
		out.SumState = "invalid_overlap"
	case invalid:
		out.SumState = "invalid_interval"
	case snapshot.ReplyState != "measured" || snapshot.ReplyWallMS == nil:
		out.SumState = "pending_or_unavailable_reply"
	case observed == 0:
		out.SumState = "no_ack_intervals_observed"
	case subtotal < 0 || subtotal > *snapshot.ReplyWallMS:
		out.SumState = "invalid_total"
	default:
		unattributed := *snapshot.ReplyWallMS - subtotal
		out.SubtotalMS, out.UnattributedMS, out.SumState = &subtotal, &unattributed, "partial"
	}
	return out
}

func attachAcknowledgementPayload(payload map[string]any, snapshot *editAcknowledgementSnapshot, state string) {
	ack := buildAcknowledgementPayload(snapshot, state)
	payload["acknowledgement"] = ack
	if snapshot == nil {
		return // preserve independently valid legacy per-file evidence
	}
	phases, _ := payload["phases_ms"].(map[string]*float64)
	states, _ := payload["phase_states"].(map[string]string)
	for _, name := range []string{"dispatcher_queue", "middleware", "parse", "disk_write", "publish", "reindex_wait"} {
		phases[name], states[name] = ack.PhasesMS[name], ack.PhaseStates[name]
	}
	window := payload["phase_timing"].(indexer.EditTimingWindow)
	window.Clock, window.Coverage = "monotonic", "partial"
	window.AcknowledgementWallMS = ack.ReplyWallMS
	// The shared RPC origin and per-file publication origin are different.
	// Keep the original receipt wall/patch evidence, but do not subtract the
	// replacement shared-RPC laps from its per-file publication interval.
	window.PublicationUnattributedWallMS = nil
	payload["phase_timing"] = window
}
