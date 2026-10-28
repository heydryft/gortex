package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zzet/gortex/internal/daemon"
)

// Exercise the real pre-listen helper and public MCP socket before ANY warmup
// announcement, seeding or repository indexing. Starting registration by hand
// here would conceal the startup window this test is intended to cover.
func TestConfiguredLoadingPublishedBeforeServing(t *testing.T) {
	registered := t.TempDir()
	d, mi := trackedPathMCPSetup(t, registered)
	root := filepath.Join(t.TempDir(), "configured")
	admin := filepath.Join(root, ".git", "worktrees", "linked")
	linked := filepath.Join(t.TempDir(), "linked")
	require.NoError(t, os.MkdirAll(admin, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "pkg"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(linked, "pkg"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(admin, "commondir"), []byte("../..\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(linked, ".git"), []byte("gitdir: "+admin+"\n"), 0o600))
	c := probeController(t, fmt.Sprintf("repos:\n  - path: %q\n    name: startup\n", root))
	state := &daemonState{multiIndexer: mi, configManager: c.configManager}

	file, err := os.CreateTemp(os.TempDir(), "gx-startup-*.sock")
	require.NoError(t, err)
	socket := file.Name()
	require.NoError(t, file.Close())
	require.NoError(t, os.Remove(socket))
	t.Setenv("GORTEX_DAEMON_PIDFILE", filepath.Join(t.TempDir(), "daemon.pid"))
	t.Setenv("GORTEX_DAEMON_STATEFILE", filepath.Join(t.TempDir(), "daemon.state.json"))
	srv := daemon.New(socket, "test", zap.NewNop())
	srv.MCPDispatcher = d
	require.NoError(t, listenConfiguredDaemon(srv, state))
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown())
		select {
		case err := <-served:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("owned startup test server did not stop")
		}
	})

	for name, cwd := range map[string]string{
		"primary": root, "subdirectory": filepath.Join(root, "pkg"),
		"linked": linked, "linked subdirectory": filepath.Join(linked, "pkg"),
	} {
		t.Run(name, func(t *testing.T) {
			client, err := daemon.DialTo(socket, daemon.Handshake{Mode: daemon.ModeMCP, CWD: cwd})
			require.NoError(t, err)
			defer client.Close()
			require.NoError(t, client.Conn.SetDeadline(time.Now().Add(5*time.Second)))
			require.NoError(t, client.WriteMCPFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"startup-probe","version":"1"}}}`)))
			_, err = client.ReadMCPFrame()
			require.NoError(t, err)
			require.NoError(t, client.WriteMCPFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_symbols","arguments":{"query":"Startup"}}}`)))
			frame, err := client.ReadMCPFrame()
			require.NoError(t, err)
			var reply map[string]any
			require.NoError(t, json.Unmarshal(frame, &reply))
			data := errorData(t, reply)
			assert.Equal(t, "repo_loading", data["error_code"])
			assert.Equal(t, true, data["retryable"])
			assert.Contains(t, data, "phase")
			assert.Contains(t, data, "percent")
			assert.NotContains(t, string(frame), "gortex track")
			assert.NotContains(t, string(frame), "repos reconcile")
		})
	}
	assert.Empty(t, mi.GetMetadata("startup"), "pending declarations must never register or admit graph access")
	untracked := &daemon.Session{ID: "unconfigured", CWD: t.TempDir()}
	assert.Equal(t, "repo_not_tracked", errorCode(dispatchJSON(t, d, untracked,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_symbols","arguments":{"query":"Startup"}}}`)))
}
