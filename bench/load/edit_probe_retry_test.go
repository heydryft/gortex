package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type editProbeRetryStub struct{ calls int }

func (s *editProbeRetryStub) call(context.Context, string, any) (json.RawMessage, error) {
	s.calls++
	if s.calls == 1 {
		return json.RawMessage(`{"isError":true,"content":[{"type":"text","text":"{\"error_code\":\"identity_pending\",\"message\":\"wait\",\"retryable\":true}"}]}`), nil
	}
	return json.RawMessage(`{"content":[{"type":"text","text":"{\"ok\":true}"}]}`), nil
}

func TestEditProbeTypedRetryPolicyDoesNotChangeLegacyCalls(t *testing.T) {
	r := request{Tool: "edit", Operation: "file"}
	sleep := func(context.Context, time.Duration) error { return nil }
	legacy := &editProbeRetryStub{}
	_, rows, err := callTool(t.Context(), legacy, r, nil, "workload", 0, 0, 0, time.Second, sleep)
	if err == nil || legacy.calls != 1 || len(rows) != 1 || rows[0].Outcome != "error" {
		t.Fatalf("legacy retry behavior changed: %+v %v", rows, err)
	}
	scoped := &editProbeRetryStub{}
	_, rows, err = callEditProbeTool(t.Context(), scoped, r, nil, "workload", 0, 0, 0, time.Second, sleep)
	if err != nil || scoped.calls != 2 || len(rows) != 2 || rows[0].Outcome != "retryable" || rows[1].Outcome != "success" {
		t.Fatalf("typed probe retry missing: %+v %v", rows, err)
	}
	metrics := summarize(rows, 1, "workload")["edit.file"]
	if metrics.RetryableRefusals != 1 || metrics.BusyRefusals != 0 || metrics.Successes != 1 || len(metrics.Errors) != 0 {
		t.Fatalf("retry refusal misclassified: %+v", metrics)
	}
}
