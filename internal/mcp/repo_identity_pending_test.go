package mcp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zzet/gortex/internal/config"
)

func TestStateAwareInstructions_ColdIdentityPendingAndRecovery(t *testing.T) {
	srv, mi, configured := newLoadingServer(t)
	physical := wsRepo(t, "physical", "alias-workspace", "AliasSymbol")
	alias := filepath.Join(t.TempDir(), "different-configured-name")
	require.NoError(t, os.Symlink(physical, alias))
	entries := []config.RepoEntry{{Path: configured}, {Path: alias, Name: "alias-repo"}}
	mi.DeclareRepos(entries)
	for _, cwd := range []string{physical, t.TempDir()} {
		for _, client := range []string{"claude-code", "codex", ""} {
			instructions := srv.stateAwareInstructionsForClient(cwd, client)
			require.Contains(t, instructions, "identity_pending")
			require.NotContains(t, instructions, "INACTIVE")
			require.NotContains(t, instructions, "already configured")
			require.NotContains(t, instructions, "alias-repo")
			require.NotContains(t, instructions, "gortex track")
		}
	}
	ctx := sessionCtx("unresolved-alias", physical)
	ws, _, bound := srv.sessionScope(ctx)
	require.True(t, bound)
	require.True(t, strings.HasPrefix(ws, unresolvedWorkspacePrefix), "uncertainty grants no graph scope")
	mi.ExpectRepos(entries)
	require.Contains(t, srv.stateAwareInstructions(physical), "LOADING alias-repo")
	_, err := mi.TrackRepoCtx(t.Context(), config.RepoEntry{Path: physical, Name: "alias-repo"})
	require.NoError(t, err)
	ws, _, bound = srv.sessionScope(ctx)
	require.True(t, bound)
	require.Equal(t, "alias-workspace", ws, "the same session re-resolves after identity and registration complete")
	require.NotContains(t, srv.stateAwareInstructions(physical), "identity_pending")
	require.Contains(t, srv.stateAwareInstructions(t.TempDir()), "INACTIVE", "only complete negative identity evidence permits inactive advice")
}

func TestIdentityPendingPreservesRegisteredAutomaticCheckout(t *testing.T) {
	v := newViewStack(t)
	v.srv.multiIndexer.DeclareRepos([]config.RepoEntry{{Path: v.repoRoot}, {Path: v.otherRoot}, {Path: filepath.Join(t.TempDir(), "unknown")}})
	require.True(t, v.srv.multiIndexer.RepoIdentityPending())
	served, err := v.srv.CheckoutServesCWDChecked(t.Context(), v.worktreeRoot)
	require.NoError(t, err)
	require.True(t, served)
	require.NotContains(t, v.srv.stateAwareInstructions(v.worktreeRoot), "identity_pending")
	ctx := sessionCtx("pending-other-root", v.worktreeRoot)
	ws, _, bound := v.srv.sessionScope(ctx)
	require.True(t, bound)
	require.Equal(t, "main-ws", ws)
}
