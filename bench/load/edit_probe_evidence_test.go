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
