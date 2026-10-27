package daemon

import (
	"context"
	"encoding/json"
	"testing"
)

func TestDispatchEnvelopeLeavesMalformedToolArgumentsToPublicValidation(t *testing.T) {
	for _, value := range []string{`[]`, `"invalid"`, `42`, `null`} {
		frame := []byte(`{"jsonrpc":"2.0","id":17,"method":"tools/call","params":{"name":"read","arguments":` + value + `}}`)
		var envelope mcpRequestEnvelope
		if err := json.Unmarshal(frame, &envelope); err != nil {
			t.Fatalf("admission consumed handler validation for arguments=%s: %v", value, err)
		}
		if string(envelope.ID) != "17" || string(envelope.Params.Arguments) != value {
			t.Fatalf("envelope identity/payload changed: %+v", envelope)
		}
	}
}

func TestSharedDispatchEntryReplayDoesNotAcquireAnotherSlot(t *testing.T) {
	slots := newMCPDispatchSlots(1)
	ctx := withMCPDispatchQueueWait(withMCPDispatchSlots(t.Context(), slots), 0)
	sess := &Session{ID: "same-session", LogicalSessionID: "same-logical-session"}
	frame := []byte(`{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"read_file"}}`)
	calls := 0
	dispatcher := testMCPDispatcherFunc(func(context.Context, *Session, []byte) ([]byte, error) {
		calls++
		return []byte(`{"jsonrpc":"2.0","id":9,"result":{"ok":true}}`), nil
	})
	first, err := DispatchMCPFrame(ctx, sess, frame, dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	holder := takeScheduled(t, mcpSchedulerFor(slots), MCPDispatchMutating, "holder")
	defer holder.complete()
	replay, err := DispatchMCPFrame(ctx, sess, frame, dispatcher)
	if err != nil || string(replay) != string(first) || calls != 1 || len(slots) != 1 {
		t.Fatalf("cached replay double-admitted: err=%v calls=%d held=%d first=%s replay=%s", err, calls, len(slots), first, replay)
	}
}
