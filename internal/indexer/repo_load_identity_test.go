package indexer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zzet/gortex/internal/config"
	"github.com/zzet/gortex/internal/pathkey"
)

func TestRepoIdentityColdAliasPublication(t *testing.T) {
	base := t.TempDir()
	physical := filepath.Join(base, "physical-repository")
	alias := filepath.Join(base, "different-configured-name")
	require.NoError(t, os.MkdirAll(filepath.Join(physical, ".git"), 0755))
	require.NoError(t, os.Symlink(physical, alias))
	mi := newRepoLoadTestMulti(t)
	entries := []config.RepoEntry{{Path: alias}}
	mi.DeclareRepos(entries)
	require.True(t, mi.RepoIdentityPending())
	_, known := mi.PendingRepoForPath(physical)
	require.False(t, known, "an unresolved declaration cannot invent ownership")
	mi.ExpectRepos(entries)
	require.False(t, mi.RepoIdentityPending())
	rec, known := mi.PendingRepoForPath(physical)
	require.True(t, known)
	require.Equal(t, alias, rec.Root)
}

func TestRepoIdentityDarwinTmpAliasPublication(t *testing.T) {
	canonical, err := filepath.EvalSymlinks("/tmp")
	require.NoError(t, err)
	if canonical != "/private/tmp" {
		t.Skip("host does not expose the /tmp -> /private/tmp alias")
	}
	root, err := os.MkdirTemp("/tmp", "repo-load-alias-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	mi := newRepoLoadTestMulti(t)
	entries := []config.RepoEntry{{Path: root}}
	mi.DeclareRepos(entries)
	physical := filepath.Join(canonical, filepath.Base(root))
	require.True(t, mi.RepoIdentityPending())
	_, known := mi.PendingRepoForPath(physical)
	require.False(t, known)
	mi.ExpectRepos(entries)
	rec, known := mi.PendingRepoForPath(physical)
	require.True(t, known)
	require.Equal(t, root, rec.Root)
}

func TestRepoIdentityPublicationDoesNotOverrideRegistrationOrRedeclaration(t *testing.T) {
	for _, transition := range []string{"registration", "redeclaration", "running"} {
		t.Run(transition, func(t *testing.T) {
			root := t.TempDir()
			mi := newRepoLoadTestMulti(t)
			entries := []config.RepoEntry{{Path: root}}
			mi.DeclareRepos(entries)
			before := mi.RepoLoads()[0]
			resolved, err := resolveRepoLoadIdentity(before)
			require.NoError(t, err)
			switch transition {
			case "registration":
				mi.markRepoRegistered("ready", root)
			case "redeclaration":
				mi.DeclareRepos(nil)
				mi.DeclareRepos(entries)
			case "running":
				mi.StartRepoLoad(root)
			}
			mi.publishRepoLoadIdentity(before, resolved)
			after, ok := mi.RepoLoadFor(root)
			require.True(t, ok)
			switch transition {
			case "registration":
				require.Equal(t, RepoLoadReady, after.State)
				require.False(t, mi.RepoIdentityPending())
			case "redeclaration":
				require.True(t, mi.RepoIdentityPending(), "stale owner result must not resolve a replacement declaration")
			case "running":
				require.Equal(t, RepoLoadRunning, after.State)
				require.False(t, mi.RepoIdentityPending())
			}
		})
	}
}

func TestRepoIdentityUncertainFilesystemRetainsNegativeFence(t *testing.T) {
	root := t.TempDir()
	// A directory where commondir should be a readable file produces a real
	// non-ENOENT I/O failure regardless of the test user's permission privileges.
	admin := filepath.Join(t.TempDir(), "admin")
	require.NoError(t, os.MkdirAll(filepath.Join(admin, "commondir"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: "+admin), 0644))
	mi := newRepoLoadTestMulti(t)
	entries := []config.RepoEntry{{Path: root}}
	mi.ExpectRepos(entries)
	require.True(t, mi.RepoIdentityPending())
	rec, _ := mi.RepoLoadFor(root)
	require.Equal(t, RepoLoadQueued, rec.State)
	_, err := resolveRepoLoadIdentity(rec)
	require.Error(t, err)
	require.False(t, os.IsNotExist(err))
	require.NoError(t, os.Remove(filepath.Join(root, ".git")))
	mi.ExpectRepos(entries)
	require.False(t, mi.RepoIdentityPending())
}

func TestRepoIdentitySeparateGitDirAndLinkedBoundary(t *testing.T) {
	primary, linked := t.TempDir(), t.TempDir()
	common := filepath.Join(t.TempDir(), "external-git")
	admin := filepath.Join(common, "worktrees", "linked")
	require.NoError(t, os.MkdirAll(admin, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(primary, ".git"), []byte("gitdir: "+common), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(linked, ".git"), []byte("gitdir: "+admin), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(admin, "commondir"), []byte("../.."), 0644))
	info := ResolveWorktree(primary)
	require.False(t, info.IsWorktree)
	require.Equal(t, primary, info.MainRepoPath)
	require.True(t, pathkey.EqualPaths(common, info.GitCommonDir))
	mi := newRepoLoadTestMulti(t)
	mi.ExpectRepos([]config.RepoEntry{{Path: primary}})
	rec, known := mi.PendingRepoForPath(linked)
	require.True(t, known)
	require.Equal(t, primary, rec.Root)
	nested := filepath.Join(linked, "nested")
	require.NoError(t, os.MkdirAll(filepath.Join(nested, ".git"), 0755))
	_, known = mi.PendingRepoForPath(nested)
	require.False(t, known, "the nearest unrelated git identity is a boundary")
}
