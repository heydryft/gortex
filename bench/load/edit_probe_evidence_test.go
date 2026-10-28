package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEditProbeOraclesRejectEchoAndRequireActualSourceAndEdges(t *testing.T) {
	corpus := editProbeCorpus()
	plan, err := makeEditProbePlan(corpus, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	step := plan.Steps[0]
	for oracle, collection := range map[string]string{"summary": "nodes", "editing_context": "defines", "search_visible": "results"} {
		facts := map[string]any{collection: []any{map[string]any{"id": "sample/" + corpus.File + "::" + step.Marker}}}
		if !editProbeCorrect(oracle, facts, corpus, step, plan.Callee) {
			t.Fatalf("%s rejected its producer collection", oracle)
		}
		if editProbeCorrect(oracle, map[string]any{"guidance": facts}, corpus, step, plan.Callee) {
			t.Fatalf("%s accepted nested guidance", oracle)
		}
	}
	echo := map[string]any{"query": step.Marker, "target": map[string]any{"id": "sample/" + corpus.File + "::" + step.Marker}}
	for _, name := range []string{"summary", "editing_context", "search_visible", "text", "callers", "source"} {
		if editProbeCorrect(name, echo, corpus, step, plan.Callee) {
			t.Fatalf("%s accepted query echo", name)
		}
	}
	source := strings.ReplaceAll(corpus.EditTemplate, "{{marker}}", step.Marker)
	if !editProbeCorrect("source", map[string]any{"name": step.Marker, "source": source}, corpus, step, plan.Callee) {
		t.Fatal("actual source/call rejected")
	}
	edge := map[string]any{"edges": []any{map[string]any{"from": "sample/" + corpus.File + "::" + step.Marker, "to": "sample/" + corpus.Symbol, "kind": "calls"}}}
	if !editProbeCorrect("callers", edge, corpus, step, plan.Callee) {
		t.Fatal("actual intrafile call rejected")
	}
	edge["edges"].([]any)[0].(map[string]any)["to"] = "ext::" + plan.Callee
	if editProbeCorrect("callers", edge, corpus, step, plan.Callee) {
		t.Fatal("external/name-only relation became source proof")
	}
}

func TestEditProbeWindowRejectsMissingFractionalNanosecondsAndOverlap(t *testing.T) {
	check, err := checkEditProbeWindow(json.Number("0.000003"), map[string]any{"a": json.Number("0.000001"), "b": json.Number("0.000002")})
	if err != nil || check.WallNS != 3 || check.ResidualNS != 0 {
		t.Fatalf("exact fractional-ms check%+v %v", check, err)
	}
	for _, value := range []any{nil, json.Number("0.0000001"), json.Number("-1"), json.Number("999999999999999999999")} {
		if _, err := editProbeNS(value); err == nil {
			t.Fatalf("invalid scalar accepted%v", value)
		}
	}
	if _, err := checkEditProbeWindow(json.Number("1"), map[string]any{"a": json.Number("2")}); err == nil {
		t.Fatal("overlapping/excess scalar sum accepted")
	}
}

func TestEditProbeAccountingRejectsMissingApplicableACKAndWrongAttempt(t *testing.T) {
	fixture := func() map[string]any {
		ackPhases, ackStates := map[string]any{}, map[string]any{}
		for _, name := range []string{"caller_result", "disk_write", "edit_control", "middleware", "parse", "publish", "reindex_wait", "reply_prepare"} {
			ackPhases[name], ackStates[name] = json.Number("1"), "measured"
		}
		p, b := map[string]any{}, map[string]any{}
		for _, name := range []string{"gate_wait", "patch_parse", "apply", "search_lap", "resolve", "mtimes", "derived", "attempt_setup", "patch_inspection", "executor_control", "attempt_finalize", "cohort_admission", "lane_wait"} {
			p[name], b[name] = json.Number("1"), json.Number("1")
		}
		for _, name := range requestedBackgroundLapNamesForProbe {
			b[name] = json.Number("1")
		}
		return map[string]any{
			"receipt": "receipt-a", "reindex_generation": json.Number("1"), "applied_generation": json.Number("2"),
			"acknowledgement":       map[string]any{"rpc_id": "rpc-a", "disjoint_observed_intervals": true, "caller_response_ready_ms": json.Number("8"), "observed_execution_ms": ackPhases, "phase_states": ackStates},
			"publication_phases":    map[string]any{"key": "original"},
			"coalesced_publication": map[string]any{"requested_generation": json.Number("1"), "applied_generation": json.Number("2"), "scope": "independent_physical_winner_serving_coalesced_receipt", "publication_phases": map[string]any{"ticket": json.Number("2"), "source": "canonical_coalesced_attempt", "key": "winner"}, "timings": map[string]any{"phase_timing": map[string]any{"patch_wall_ms": json.Number("13"), "patch_accounting": map[string]any{"observed_partition_complete": true, "attempt_ticket": json.Number("2"), "attempt_retry": json.Number("0"), "phases_ms": p}}}},
			"requested_background":  map[string]any{"requested_generation": json.Number("1"), "applied_generation": json.Number("2"), "observed_partition_complete": true, "scope": "accepted_scheduling_call_entry_to_serving_attempt_return", "coverage": "one_canceled_timer_one_native_successor", "sum_state": "partial", "background_wall_ms": json.Number("20"), "phases_ms": b},
		}
	}
	if got := checkEditProbeAccounting(fixture(), "receipt-a"); got.State != "observed_single_window_checks_pass" {
		t.Fatalf("valid fixture: %+v", got)
	}
	for _, name := range []string{"missing_ack", "wrong_retry", "wrong_owner"} {
		t.Run(name, func(t *testing.T) {
			body := fixture()
			if name == "missing_ack" {
				delete(body["acknowledgement"].(map[string]any)["observed_execution_ms"].(map[string]any), "parse")
			} else {
				p := body["coalesced_publication"].(map[string]any)["timings"].(map[string]any)["phase_timing"].(map[string]any)["patch_accounting"].(map[string]any)
				if name == "wrong_retry" {
					p["attempt_retry"] = json.Number("1")
				} else {
					p["attempt_ticket"] = json.Number("9")
				}
			}
			if got := checkEditProbeAccounting(body, "receipt-a"); got.State == "observed_single_window_checks_pass" {
				t.Fatalf("invalid fixture accepted: %+v", got)
			}
		})
	}
}
