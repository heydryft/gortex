package indexer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zzet/gortex/internal/config"
)

func TestRegisterCollisionNoCatalogNoWatcherNoSave(t *testing.T) {
	f := newLifecycleFixture(t)
	t.Cleanup(f.close)
	root := f.gitRepo("src")
	before, err := os.ReadFile(f.cfgPath)
	require.NoError(t, err)
	beforeConfig, beforeRevision := f.cm.GetRepoConfig("src"), f.cm.Revision()
	// A nil registry slot exercises the legacy nil track-result contract
	// without introducing a fake Track implementation. It is not proof that
	// any exact root was registered, whatever the requested basename says.
	f.mi.mu.Lock()
	f.mi.repos["src"] = nil
	f.mi.mu.Unlock()
	result, err := f.lc.Register(t.Context(), config.RepoEntry{Path: root, Name: "src"}, TrackSourceCLI)
	var collision *PrefixCollisionError
	require.ErrorAs(t, err, &collision)
	require.Equal(t, "src", collision.Prefix)
	require.Equal(t, "unknown", collision.OwnerState)
	require.Empty(t, collision.ExistingPath, "unknown ownership must not invent another root")
	require.Empty(t, result.Prefix)
	families, err := f.catalog.ListRepositoryFamilies(t.Context())
	require.NoError(t, err)
	require.Empty(t, families, "registration must stop before catalog allocation")
	after, err := os.ReadFile(f.cfgPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, beforeConfig, f.cm.GetRepoConfig("src"))
	require.Equal(t, beforeRevision, f.cm.Revision(), "nil track must not publish a prepared candidate config")
	require.Empty(t, f.cm.WorkspacePrefixes())
	f.watcher.mu.Lock()
	calls := append([]string(nil), f.watcher.calls...)
	f.watcher.mu.Unlock()
	require.Empty(t, calls)
	invalidated, analysed := f.notify.counts()
	require.Zero(t, invalidated)
	require.Zero(t, analysed)
}

func TestRecordImplicitRequiresExactRegisteredRoot(t *testing.T) {
	f := newLifecycleFixture(t)
	t.Cleanup(f.close)
	owner := f.gitRepo("owner")
	_, err := f.lc.Register(t.Context(), config.RepoEntry{Path: owner}, TrackSourceCLI)
	require.NoError(t, err)
	child := filepath.Join(owner, "nested")
	require.NoError(t, os.MkdirAll(child, 0o700))
	require.Equal(t, "owner", f.lc.ResolvePrefix(child), "containment remains useful for reads")
	before, err := os.ReadFile(f.cfgPath)
	require.NoError(t, err)
	graphs, err := f.catalog.ListDedicatedGraphs(t.Context(), f.familyOf("owner").FamilyID)
	require.NoError(t, err)
	nodes, edges := f.store.NodeCount(), f.store.EdgeCount()
	f.watcher.mu.Lock()
	watcherCalls := append([]string(nil), f.watcher.calls...)
	f.watcher.mu.Unlock()
	invalidated, analysed := f.notify.counts()
	err = f.lc.RecordImplicit(t.Context(), child)
	var collision *PrefixCollisionError
	require.ErrorAs(t, err, &collision, "an ancestor prefix is not exact registration authority")
	require.Equal(t, "unknown", collision.OwnerState)
	require.Equal(t, nodes, f.store.NodeCount())
	require.Equal(t, edges, f.store.EdgeCount())
	afterGraphs, err := f.catalog.ListDedicatedGraphs(t.Context(), f.familyOf("owner").FamilyID)
	require.NoError(t, err)
	require.Equal(t, graphs, afterGraphs)
	after, err := os.ReadFile(f.cfgPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	f.watcher.mu.Lock()
	afterWatcherCalls := append([]string(nil), f.watcher.calls...)
	f.watcher.mu.Unlock()
	require.Equal(t, watcherCalls, afterWatcherCalls)
	afterInvalidated, afterAnalysed := f.notify.counts()
	require.Equal(t, invalidated, afterInvalidated)
	require.Equal(t, analysed, afterAnalysed)
}
