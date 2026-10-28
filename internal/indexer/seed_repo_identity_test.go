package indexer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zzet/gortex/internal/config"
)

func TestSeedIdentityPublicationKeepsIdentitySeparateFromReadiness(t *testing.T) {
	_, mi, cm := newWorktreeTestIndexer(t)
	t.Cleanup(func() { require.NoError(t, mi.Close(context.Background())) })
	root := t.TempDir()
	require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: root, Name: "src"}))
	witness := mi.captureSeedIdentity(root, cm.RepoTrackSnapshot()) // constructor client has no declaration yet
	facts := checkoutRootIdentity{known: true, canonicalRoot: root, commonDir: filepath.Join(root, ".git"), plainRoot: true}
	require.True(t, mi.publishSeedIdentity(witness, "src", facts))
	facts.canonicalRoot, facts.commonDir = "changed", "changed" // caller's values are not retained by reference
	rec, found := mi.RepoLoadFor(root)
	require.True(t, found)
	require.Equal(t, RepoLoadQueued, rec.State)
	require.False(t, rec.identityPending)
	require.Equal(t, filepath.Join(root, ".git"), rec.commonDir)
	require.Nil(t, mi.GetMetadata("src"), "identity facts do not register a graph")
}

func TestSeedIdentityPublicationRejectsNewerConfigurationAndOwnership(t *testing.T) {
	for _, change := range []string{"configuration", "registration", "removal", "failure", "resolved", "absent-ABA"} {
		t.Run(change, func(t *testing.T) {
			_, mi, cm := newWorktreeTestIndexer(t)
			t.Cleanup(func() { require.NoError(t, mi.Close(context.Background())) })
			root := t.TempDir()
			require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: root, Name: "src"}))
			entries := cm.Global().ReposSnapshot()
			if change != "absent-ABA" {
				mi.DeclareRepos(entries)
			}
			witness := mi.captureSeedIdentity(root, cm.RepoTrackSnapshot())
			switch change {
			case "configuration":
				require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: t.TempDir(), Name: "new"}))
			case "registration":
				mi.markRepoRegistered("src", root)
			case "removal":
				mi.forgetRepoLoad(root)
			case "failure":
				mi.FinishRepoLoad(root, errors.New("newer load failure"))
			case "resolved":
				mi.ExpectRepos(entries)
			case "absent-ABA":
				mi.DeclareRepos(entries)
				mi.forgetRepoLoad(root)
			}
			current := mi.repoLoads.snap.Load()
			require.False(t, mi.publishSeedIdentity(witness, "stale", checkoutRootIdentity{
				known: true, canonicalRoot: root, commonDir: filepath.Join(root, ".git"), plainRoot: true,
			}))
			require.Same(t, current, mi.repoLoads.snap.Load(), "a stale publication must not replace the newer snapshot")
		})
	}
}

func TestSeedIdentityWitnessUsesTheResolvedConfiguration(t *testing.T) {
	_, mi, cm := newWorktreeTestIndexer(t)
	t.Cleanup(func() { require.NoError(t, mi.Close(context.Background())) })
	root := t.TempDir()
	require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: root, Name: "old-prefix"}))
	configuration := cm.RepoTrackSnapshot()
	mi.DeclareRepos(configuration.TopLevelEntries())
	resolved := mi.ResolveConfiguredRepos(t.Context(), configuration.TopLevelEntries())
	require.Len(t, resolved, 1)
	// The namespace changes after whole-pass resolution but before this row's
	// recordCheckout witness is captured, just as a later row can be delayed.
	require.NoError(t, cm.Global().RemoveRepo(root))
	require.NoError(t, cm.Global().AddRepo(config.RepoEntry{Path: root, Name: "new-prefix"}))
	witness := mi.captureSeedIdentity(resolved[0].Path, configuration)
	current := mi.repoLoads.snap.Load()
	require.False(t, mi.publishSeedIdentity(witness, resolved[0].Name, checkoutRootIdentity{
		known: true, canonicalRoot: root, commonDir: filepath.Join(root, ".git"), plainRoot: true,
	}))
	require.Same(t, current, mi.repoLoads.snap.Load())
}
