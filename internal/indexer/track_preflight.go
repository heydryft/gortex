package indexer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/zzet/gortex/internal/config"
	"github.com/zzet/gortex/internal/pathkey"
)

// configuredTrackEntry uses requested/canonical basename candidates and
// completed startup alias facts. Unresolved arbitrary configured aliases mean
// retry, never permission to persist a duplicate physical root.
func (mi *MultiIndexer) configuredTrackEntry(ctx context.Context, root string, before time.Time) (*config.RepoEntry, error) {
	entry, err := mi.configuredTrackEntryWithCanonical(ctx, root, "", before)
	if err != nil || entry != nil || mi.configMgr == nil {
		return entry, err
	}
	budget, cancel := context.WithDeadline(ctx, before)
	defer cancel()
	facts, err := mi.prefixStats.naming(budget, root)
	if err != nil {
		return nil, err
	}
	return mi.configuredTrackEntryWithCanonical(ctx, root, facts.canonical, before)
}

func (mi *MultiIndexer) configuredTrackEntryWithCanonical(ctx context.Context, root, canonical string, before time.Time) (*config.RepoEntry, error) {
	if mi.configMgr == nil {
		return nil, nil
	}
	snapshot := mi.configMgr.RepoTrackSnapshot()
	entries := snapshot.RootCandidates(root)
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		seen[entry.Path] = true
	}
	appendCandidates := func(path string) {
		for _, entry := range snapshot.RootCandidates(path) {
			if !seen[entry.Path] {
				entries = append(entries, entry)
				seen[entry.Path] = true
			}
		}
	}
	if canonical != "" {
		appendCandidates(canonical)
		// Warmup may already know an alias configured under another basename.
		// Reading that completed identity never starts all-root discovery.
		if known, ok := mi.RepoLoadFor(canonical); ok {
			appendCandidates(known.Root)
		}
	}
	for _, entry := range entries {
		if entry.Path == root {
			return &entry, nil
		}
	}
	budget, cancel := context.WithDeadline(ctx, before)
	defer cancel()
	var candidate *prefixStatResult
	for _, entry := range entries {
		// Distinct roots in a basename bucket need no I/O unless one might
		// actually be an alias. Same-basename symlinks are intentionally kept.
		if candidate == nil {
			observed := mi.prefixStats.observe(budget, root)
			candidate = &observed
		}
		owner := mi.prefixStats.observe(budget, entry.Path)
		if sameObservedDirectory(*candidate, owner) {
			return &entry, nil
		}
		if pathkey.EqualPaths(entry.Path, root) && (candidate.err != nil || owner.err != nil) {
			return nil, errPrefixIdentityUnknown
		}
	}
	if canonical != "" {
		loads := mi.repoLoads.load()
		// This is a memory-only completeness check, paid only for a new root
		// that matched no configured entry. Known/configured tracks return
		// above; no operation here scans the filesystem of unrelated roots.
		for _, path := range snapshot.TopLevelRoots() {
			i, known := loads.find(absRepoRoot(path))
			if !known || loads.records[i].identityPending || loads.records[i].canonicalKey == "" && loads.records[i].State != RepoLoadMissing {
				return nil, errPrefixIdentityUnknown
			}
		}
	}
	return nil, nil
}

// prepareRegistrationEntry is shared by the advisory preflight and real
// registration. It reads names but never publishes workspace/config/catalog
// state, attaches a watcher, or admits an indexing producer.
func (l *CheckoutLifecycle) prepareRegistrationEntry(ctx context.Context, entry config.RepoEntry, before time.Time) (config.RepoEntry, error) {
	prepared, _, err := l.prepareRegistrationNaming(ctx, entry, before)
	return prepared, err
}

func (l *CheckoutLifecycle) prepareRegistrationNaming(ctx context.Context, entry config.RepoEntry, before time.Time) (config.RepoEntry, trackNamingFacts, error) {
	var facts trackNamingFacts
	abs, err := filepath.Abs(entry.Path)
	if err != nil {
		return entry, facts, fmt.Errorf("resolve path %s: %w", entry.Path, err)
	}
	entry.Path = pathkey.NormalizeVolume(abs)
	budget, cancel := context.WithDeadline(ctx, before)
	defer cancel()
	facts, err = l.mi.prefixStats.naming(budget, entry.Path)
	if err != nil {
		return entry, facts, err
	}
	configured, err := l.mi.configuredTrackEntryWithCanonical(budget, entry.Path, facts.canonical, before)
	if err != nil {
		return entry, facts, err
	}
	if configured != nil {
		if entry.Name != "" && entry.Name != config.ResolvePrefix(*configured) {
			return entry, facts, fmt.Errorf("%s is already configured; set name: %s in the global config and run `gortex daemon reload`", entry.Path, entry.Name)
		}
		if entry.Name == "" {
			entry.Name = configured.Name
		}
		if entry.Workspace == "" {
			entry.Workspace = configured.Workspace
		}
	}
	observed, ownershipErr := l.mi.observePrefixOwnershipBefore(budget, config.ResolvePrefix(entry), entry.Path, before, nil)
	if observed.registeredPrefix != "" {
		entry.Name = observed.registeredPrefix
		// A byte-exact registered root is already a stable cache key; aliases
		// still get bounded canonical discovery below.
	}
	if entry.Name != "" && ownershipErr != nil {
		return entry, facts, ownershipErr
	}
	if entry.Name == "" {
		prefix, err := l.dedicatedPrefixFromFacts(budget, entry.Path, facts, before)
		if err != nil {
			return entry, facts, err
		}
		entry.Name = prefix
	}
	return entry, facts, nil
}

// PreflightTrack rejects a known collision before a controller lock or MCP
// goroutine. It is advisory: registration still rechecks ownership in its
// mutation lane. A nil result is never permission to publish a checkout.
func (l *CheckoutLifecycle) PreflightTrack(ctx context.Context, entry config.RepoEntry) error {
	_, err := l.PreflightTrackRoot(ctx, entry)
	return err
}

// PreflightTrackRoot also returns a canonical root from bounded discovery, so
// deferred track outcomes never call EvalSymlinks on the serving goroutine.
func (l *CheckoutLifecycle) PreflightTrackRoot(ctx context.Context, entry config.RepoEntry) (root string, err error) {
	if l == nil || l.mi == nil {
		return "", nil // the owning surface reports its wiring error
	}
	defer func() {
		var collision *PrefixCollisionError
		if errors.As(err, &collision) {
			l.mi.logCollision(collision)
		}
	}()
	before := time.Now().Add(prefixOwnershipTimeout)
	entry, facts, err := l.prepareRegistrationNaming(ctx, entry, before)
	if err != nil {
		return facts.canonical, err
	}
	prefix := l.mi.trackPrefixFromFacts(entry, facts)
	observation, err := l.mi.observePrefixOwnershipBefore(ctx, prefix, entry.Path, before, nil)
	if entry.Name == "" && prefix != config.ResolvePrefix(entry) {
		var collision *PrefixCollisionError
		if errors.As(err, &collision) && collision.OwnerState != "unknown" {
			prefix = WorktreeInstanceCollisionName(prefix, entry.Path)
			observation, err = l.mi.observePrefixOwnershipBefore(ctx, prefix, entry.Path, before, nil)
		}
	}
	if err != nil {
		return facts.canonical, err
	}
	if observation.registeredPrefix != "" {
		return facts.canonical, nil
	}
	return facts.canonical, l.mi.configuredPrefixCollisionBefore(ctx, prefix, entry.Path, before)
}
