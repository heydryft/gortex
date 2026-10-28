package mcp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zzet/gortex/internal/daemon"
)

func TestAcknowledgementDispatchSnapshotKeepsSeparateOriginAndInvalidStates(t *testing.T) {
	start := time.Now()
	a := &editAcknowledgement{entered: start.Add(10 * time.Millisecond), hasDispatch: true,
		dispatch: daemon.MCPDispatchTiming{Entered: start, Granted: start.Add(4 * time.Millisecond)}}
	a.finishActualAt(start.Add(25 * time.Millisecond))
	a.finishReplyAt(start.Add(30*time.Millisecond), "result")
	snapshot := a.snapshot()
	require.Equal(t, 4.0, *snapshot.DispatcherQueueMS)
	require.Equal(t, 6.0, *snapshot.DispatcherPreambleMS)
	require.Equal(t, 20.0, *snapshot.ReplyWallMS, "admission timestamps do not silently move the existing bound-call origin")
	*snapshot.DispatcherQueueMS = 999
	require.Equal(t, 4.0, *a.snapshot().DispatcherQueueMS)
	for _, tc := range []struct{ entered, granted time.Time }{
		{start.Add(5 * time.Millisecond), start.Add(4 * time.Millisecond)},
		{start, start.Add(11 * time.Millisecond)},
	} {
		a.dispatch = daemon.MCPDispatchTiming{Entered: tc.entered, Granted: tc.granted}
		invalid := a.snapshot()
		require.Equal(t, "invalid_interval", invalid.DispatcherQueueState)
		require.Equal(t, "invalid_interval", invalid.DispatcherState)
		require.Nil(t, invalid.DispatcherQueueMS)
		require.Nil(t, invalid.DispatcherPreambleMS)
	}
	a.hasDispatch = false
	missing := a.snapshot()
	require.Nil(t, missing.DispatcherQueueMS)
	require.Equal(t, "unavailable_dispatcher_interface", missing.DispatcherQueueState)
}
