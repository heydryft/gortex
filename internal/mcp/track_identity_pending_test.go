package mcp

import (
	"os"
	"path/filepath"
	"testing"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
	"github.com/zzet/gortex/internal/config"
)

func TestMCPColdAliasPendingIsStructuredAndRetryable(t *testing.T) {
	s, mi := newWorktreeMCPServer(t)
	root := filepath.Join(t.TempDir(), "owner")
	require.NoError(t, os.MkdirAll(root, 0o755))
	alias := filepath.Join(t.TempDir(), "pinned-spelling")
	if err := os.Symlink(root, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	require.NoError(t, s.configManager.Global().AddRepo(config.RepoEntry{Path: alias, Name: "pinned", Workspace: "configured-workspace"}))
	req := mcplib.CallToolRequest{}
	req.Params.Arguments = map[string]any{"path": root}
	result, err := s.handleTrackRepository(t.Context(), req)
	require.NoError(t, err)
	require.True(t, result.IsError)
	payload := result.StructuredContent.(map[string]any)
	require.Equal(t, "repo_loading", payload["error_code"])
	require.Equal(t, true, payload["retryable"])
	require.Equal(t, "identity_pending", payload["reason"])
	require.Empty(t, mi.AllMetadata())
	jobs := 0
	s.trackInFlight.Range(func(_, _ any) bool { jobs++; return true })
	require.Zero(t, jobs, "uncertain preflight starts no indexing producer")
	mi.ExpectRepos(s.configManager.Global().ReposSnapshot())
	result, err = s.handleTrackRepository(t.Context(), req)
	require.NoError(t, err)
	require.False(t, result.IsError, "%+v", result.Content)
	require.NotNil(t, mi.GetMetadata("pinned"))
	require.Equal(t, alias, mi.GetMetadata("pinned").RootPath)
	entries := s.configManager.Global().ReposSnapshot()
	require.Len(t, entries, 1, "a resolved alias must not append another physical-root config entry")
	require.Equal(t, alias, entries[0].Path)
	require.Equal(t, "configured-workspace", entries[0].Workspace)
}
