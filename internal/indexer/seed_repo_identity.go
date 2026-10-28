package indexer

import (
	"strings"
	"time"

	"github.com/zzet/gortex/internal/config"
	"github.com/zzet/gortex/internal/pathkey"
)

type seedIdentityWitness struct {
	root     string
	config   *config.RepoTrackSnapshot
	snapshot *repoLoadSnapshot
	token    *byte
}

func (mi *MultiIndexer) captureSeedIdentity(root string) seedIdentityWitness {
	witness := seedIdentityWitness{root: absRepoRoot(root), snapshot: mi.repoLoads.snap.Load()}
	if mi.configMgr != nil {
		witness.config = mi.configMgr.RepoTrackSnapshot()
	}
	if witness.snapshot != nil {
		if i, found := witness.snapshot.find(witness.root); found {
			witness.token = witness.snapshot.records[i].identityToken
		}
	}
	return witness
}

// publishSeedIdentity consumes only positive facts the lifecycle startup owner
// already obtained for this configured root. No additional filesystem work is
// done. It establishes identity, never registration or graph readiness.
func (mi *MultiIndexer) publishSeedIdentity(witness seedIdentityWitness, prefix string, facts checkoutRootIdentity) bool {
	if !facts.known || witness.root == "" || facts.canonicalRoot == "" {
		return false
	}
	published := false
	mi.repoLoads.update(func(records []RepoLoad) ([]RepoLoad, bool) {
		if mi.configMgr != nil && mi.configMgr.RepoTrackSnapshot() != witness.config {
			return records, false
		}
		i, found := newRepoLoadSnapshot(records).find(witness.root)
		if witness.token == nil {
			// A newer registration/removal, even an absent-row ABA, owns the
			// publication. An empty snapshot does not erase that generation.
			if found || mi.repoLoads.snap.Load() != witness.snapshot {
				return records, false
			}
			records = append(records, RepoLoad{Root: witness.root, Prefix: prefix, State: RepoLoadQueued,
				Since: time.Now(), key: pathkey.IdentityKey(witness.root), identityPending: true, identityToken: new(byte)})
			i = len(records) - 1
		} else if !found || records[i].identityToken != witness.token || !records[i].State.Loading() || !records[i].identityPending {
			return records, false
		}
		rec := &records[i]
		rec.canonicalKey = pathkey.IdentityKey(pathkey.NormalizeVolume(facts.canonicalRoot))
		rec.commonDir, rec.plainRoot = facts.commonDir, facts.plainRoot
		rec.Prefix, rec.prefixPinned, rec.identityPending = prefix, true, false
		if strings.HasPrefix(rec.Reason, "identity resolution pending: ") {
			rec.Reason = ""
		}
		published = true
		return records, true
	})
	return published
}
