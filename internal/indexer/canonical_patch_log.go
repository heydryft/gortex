package indexer

import "go.uber.org/zap/zapcore"

// canonicalPatchLog is immutable physical-attempt evidence. Logging happens
// before final watcher winner validation, so it must never promise that these
// observations will be attached to a receipt. Encoding only formats the copy.
type canonicalPatchLog struct {
	ticket        uint64
	values        [canonicalPatchLapCount]float64
	states        [canonicalPatchLapCount]string
	counts        [canonicalPatchLapCount]uint64
	laneWaitMS    *float64
	laneWaitState string
}

func (s *PublicationStamps) snapshotPatchLog() (canonicalPatchLog, bool) {
	if s == nil || s.patchLaps == nil {
		return canonicalPatchLog{}, false // no new native-path clocks/allocations/encoder
	}
	s.mu.Lock()
	origin := s.patchLaps.origin
	evidence := s.patchLaps.freeze(origin)
	lane := s.laneWait.freeze(origin)
	ticket := s.patchLaps.ticket
	offsets := make(map[PublicationPhase]int64, len(s.marks)+1)
	if !s.patchLaps.enqueued.IsZero() {
		offsets[PublicationTicketEnqueued] = s.patchLaps.enqueued.Sub(origin).Nanoseconds()
	}
	for _, mark := range s.marks {
		if _, exists := offsets[mark.phase]; !exists {
			offsets[mark.phase] = mark.at.Sub(origin).Nanoseconds()
		}
	}
	s.mu.Unlock()
	projection := EditPhaseTimings{Milliseconds: make(map[string]*float64, canonicalPatchLapCount), States: make(map[string]string, canonicalPatchLapCount)}
	evidence.project(offsets, false, &projection)
	lane.project(offsets, false, &projection)
	out := canonicalPatchLog{ticket: ticket, laneWaitMS: projection.Milliseconds["lane_wait"], laneWaitState: projection.States["lane_wait"]}
	for i, name := range canonicalPatchLapNames {
		out.states[i], out.counts[i] = projection.States[name], evidence.laps[i].done
		if ms := projection.Milliseconds[name]; ms != nil {
			out.values[i] = *ms
		}
	}
	return out, true
}

func (p canonicalPatchLog) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("patch_lap_scope", "physical_attempt_before_receipt_validation")
	enc.AddUint64("attempt_ticket", p.ticket)
	enc.AddString("attempt_lane_wait_state", p.laneWaitState)
	if p.laneWaitMS != nil {
		enc.AddFloat64("attempt_lane_wait_ms", *p.laneWaitMS)
	}
	for i, name := range canonicalPatchLapNames {
		enc.AddString("attempt_"+name+"_state", p.states[i])
		enc.AddUint64("attempt_"+name+"_count", p.counts[i])
		if p.states[i] == "measured" {
			enc.AddFloat64("attempt_"+name+"_ms", p.values[i])
		}
	}
	return nil
}
