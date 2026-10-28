package indexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zzet/gortex/internal/config"
)

func TestTrackPreflightCollisionDoesNotPublishWorkspace(t *testing.T) {
	f := newLifecycleFixture(t)
	t.Cleanup(f.close)
	owner, loser := f.gitRepo("owner"), f.gitRepo("loser")
	_, err := f.lc.Register(t.Context(), config.RepoEntry{Path: owner, Name: "src"}, TrackSourceCLI)
	require.NoError(t, err)
	before, revision := f.cm.GetRepoConfig("src"), f.cm.Revision()
	writeFile(t, filepath.Join(loser, ".gortex.yaml"), "workspace: rejected\nexclude:\n  - rejected-only\n")
	var collision *PrefixCollisionError
	require.ErrorAs(t, f.lc.PreflightTrack(t.Context(), config.RepoEntry{Path: loser, Name: "src"}), &collision)
	require.Equal(t, owner, collision.ExistingPath)
	require.Equal(t, before, f.cm.GetRepoConfig("src"))
	require.Equal(t, revision, f.cm.Revision())
	require.NoError(t, f.lc.PreflightTrack(t.Context(), config.RepoEntry{Path: owner}), "a root registered under another name remains a no-op")
}

func TestTrackPreflightRetainsDedicatedAdminName(t *testing.T) {
	f := newLifecycleFixture(t)
	t.Cleanup(f.close)
	main := f.gitRepo("src")
	_, err := f.lc.Register(t.Context(), config.RepoEntry{Path: main}, TrackSourceCLI)
	require.NoError(t, err)
	linked := filepath.Join(t.TempDir(), "src")
	runGit(t, main, "worktree", "add", "-q", "-b", "review", linked)
	require.NoError(t, f.lc.PreflightTrack(t.Context(), config.RepoEntry{Path: linked}))
	prepared, err := f.lc.prepareRegistrationEntry(t.Context(), config.RepoEntry{Path: linked}, time.Now().Add(prefixOwnershipTimeout))
	require.NoError(t, err)
	require.Equal(t, f.lc.dedicatedPrefixFor(t.Context(), linked), prepared.Name)
	require.NotEmpty(t, prepared.Name)
	require.NotEqual(t, "src", prepared.Name)
}

func TestPreparedPrefixRejectsConfigurationReplacement(t *testing.T) {
	_, mi, cm := newWorktreeTestIndexer(t)
	t.Cleanup(func() { require.NoError(t, mi.Close(context.Background())) })
	root := t.TempDir()
	before := cm.Revision()
	check := func(string, string) error {
		return cm.Global().AddRepo(config.RepoEntry{Path: t.TempDir(), Name: "new-owner"})
	}
	entry := config.RepoEntry{Path: root, Name: "src"}
	_, err := mi.resolveTrackPrefixContext(t.Context(), &entry, root, nil, nil, check, time.Now().Add(prefixOwnershipTimeout), nil)
	require.ErrorIs(t, err, errPrefixConfigChanged)
	require.Equal(t, before, cm.Revision(), "rejected prepared workspace cannot publish a config revision")
	require.Nil(t, mi.GetMetadata("src"))
}

func TestConfiguredTrackAliasUsesBoundedIdentity(t *testing.T) {
	f := newLifecycleFixture(t)
	t.Cleanup(f.close)
	owner := f.gitRepo("src")
	alias := filepath.Join(t.TempDir(), "src")
	if err := os.Symlink(owner, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	require.NoError(t, f.cm.Global().AddRepo(config.RepoEntry{Path: owner, Name: "pinned"}))
	entry, err := f.lc.prepareRegistrationEntry(t.Context(), config.RepoEntry{Path: alias}, time.Now().Add(prefixOwnershipTimeout))
	require.NoError(t, err)
	require.Equal(t, "pinned", entry.Name)
}

func TestTrackPreflightDoesNotInspectUnrelatedConfiguredRoots(t *testing.T) {
	f := newLifecycleFixture(t)
	t.Cleanup(f.close)
	owner := f.gitRepo("owner")
	_, err := f.lc.Register(t.Context(), config.RepoEntry{Path: owner, Name: "src"}, TrackSourceCLI)
	require.NoError(t, err)
	entries := f.cm.Global().ReposSnapshot()
	offline := t.TempDir()
	for i := range 1000 {
		entries = append(entries, config.RepoEntry{Path: filepath.Join(offline, fmt.Sprintf("offline-%04d", i))})
	}
	f.cm.Global().Repos = entries
	require.NoError(t, f.cm.Global().Save())
	f.mi.ExpectRepos(entries) // startup resolves the absent roots before admission
	loser := t.TempDir()
	var foreign string
	f.mi.prefixStats.stat = func(root string) (os.FileInfo, error) {
		if root != owner && root != loser {
			foreign = root
		}
		return os.Stat(root)
	}
	var collision *PrefixCollisionError
	require.ErrorAs(t, f.lc.PreflightTrack(t.Context(), config.RepoEntry{Path: loser, Name: "src"}), &collision)
	require.Empty(t, foreign, "track admission must not stat the other 1000 configured roots")
}

func TestConfiguredCustomNameFollowsDifferentBasenameAlias(t *testing.T) {
	_, mi, cm := newWorktreeTestIndexer(t)
	t.Cleanup(func() { require.NoError(t, mi.Close(context.Background())) })
	owner := filepath.Join(t.TempDir(), "owner")
	require.NoError(t, os.MkdirAll(owner, 0o755))
	alias := filepath.Join(t.TempDir(), "different-name")
	if err := os.Symlink(owner, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: owner, Name: "pinned", Workspace: "configured-workspace"}))
	lifecycle := &CheckoutLifecycle{mi: mi, cfgMgr: cm} // no live catalog/registry
	entry, err := lifecycle.prepareRegistrationEntry(t.Context(), config.RepoEntry{Path: alias}, time.Now().Add(prefixOwnershipTimeout))
	require.NoError(t, err)
	require.Equal(t, "pinned", entry.Name)
	require.Equal(t, "configured-workspace", entry.Workspace)
	_, err = lifecycle.prepareRegistrationEntry(t.Context(), config.RepoEntry{Path: alias, Name: "other"}, time.Now().Add(prefixOwnershipTimeout))
	require.ErrorContains(t, err, "already configured")
	require.Empty(t, mi.AllMetadata())
}

func TestColdConfiguredAliasRequiresCompletedIdentityBeforeNewRoot(t *testing.T) {
	_, mi, cm := newWorktreeTestIndexer(t)
	t.Cleanup(func() { require.NoError(t, mi.Close(context.Background())) })
	owner := filepath.Join(t.TempDir(), "owner")
	require.NoError(t, os.MkdirAll(owner, 0o755))
	configured := filepath.Join(t.TempDir(), "pinned-spelling")
	if err := os.Symlink(owner, configured); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: configured, Name: "pinned", Workspace: "configured-workspace"}))
	lifecycle := &CheckoutLifecycle{mi: mi, cfgMgr: cm}
	require.ErrorIs(t, lifecycle.PreflightTrack(t.Context(), config.RepoEntry{Path: owner}), errPrefixIdentityUnknown)
	_, err := mi.TrackRepoCtx(t.Context(), config.RepoEntry{Path: owner})
	require.ErrorIs(t, err, errPrefixIdentityUnknown, "authoritative admission must not persist a second physical-root entry")
	require.Empty(t, mi.AllMetadata())
	require.Len(t, cm.Global().ReposSnapshot(), 1)
	mi.ExpectRepos(cm.Global().ReposSnapshot())
	entry, err := lifecycle.prepareRegistrationEntry(t.Context(), config.RepoEntry{Path: owner}, time.Now().Add(prefixOwnershipTimeout))
	require.NoError(t, err)
	require.Equal(t, "pinned", entry.Name)
	require.Equal(t, "configured-workspace", entry.Workspace)
	_, err = lifecycle.prepareRegistrationEntry(t.Context(), config.RepoEntry{Path: owner, Name: "other"}, time.Now().Add(prefixOwnershipTimeout))
	require.ErrorContains(t, err, "already configured")
	_, err = mi.TrackRepoCtx(t.Context(), config.RepoEntry{Path: owner})
	require.NoError(t, err)
	require.NotNil(t, mi.GetMetadata("pinned"))
	require.Equal(t, configured, mi.GetMetadata("pinned").RootPath)
	entries := cm.Global().ReposSnapshot()
	require.Len(t, entries, 1)
	require.Equal(t, configured, entries[0].Path)
}

func TestColdKnownConfiguredOwnerStillReturnsTypedCollision(t *testing.T) {
	_, mi, cm := newWorktreeTestIndexer(t)
	t.Cleanup(func() { require.NoError(t, mi.Close(context.Background())) })
	owner, other := filepath.Join(t.TempDir(), "owner"), filepath.Join(t.TempDir(), "other")
	for _, root := range []string{owner, other} {
		require.NoError(t, os.MkdirAll(root, 0o755))
	}
	require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: owner, Name: "src"}))
	_, err := mi.TrackRepoCtx(t.Context(), config.RepoEntry{Path: other, Name: "src"})
	var collision *PrefixCollisionError
	require.ErrorAs(t, err, &collision, "freshly proved distinct configured ownership is not downgraded to uncertainty")
	require.Equal(t, owner, collision.ExistingPath)
	require.Equal(t, "present", collision.OwnerState)
	require.Empty(t, mi.AllMetadata())
}
