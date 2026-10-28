package indexer

import (
	"crypto/sha1" // Git blob identity, not a security decision.
	"encoding/hex"
	"strconv"
	"time"
)

const repositoryReadRepairCauseCap = 8

// A reader is an admission cause, never the owner of the work performed by
// drain. In particular a session may reuse a wire ID during the same drain.
// The existing waiter generation distinguishes those admissions.
type repositoryReadRepairCause struct {
	session             string
	requestIDKey        string
	admissionGeneration uint64
}

type repositoryReadRepairRequest struct {
	session      string
	requestIDKey string
	complete     func(repositoryReadRepairObservation)
}

// repositoryReadRepairObservation is a value copy scoped to one coordinator
// incarnation. Its generation belongs to the executed drain, not any caller.
// This independent executor window is not additive with a receipt's quiet
// wait, acknowledgement, background window or later no-op watcher attempt.
type repositoryReadRepairObservation struct {
	generation     uint64
	path           string
	started        time.Time
	finished       time.Time
	causes         [repositoryReadRepairCauseCap]repositoryReadRepairCause
	causeCount     int
	causesComplete bool
	// Execution success alone is not content-publication proof. A later
	// accepted-read receipt must still match the pending commit's exact bytes.
	executionSucceeded bool
	acceptedBlobSHA    string
	contentAmbiguous   bool
}

func (o *repositoryReadRepairObservation) acceptFreshBlob(path, blobSHA string) {
	if o == nil || o.contentAmbiguous || path != o.path || blobSHA == "" {
		return
	}
	if o.acceptedBlobSHA != "" && o.acceptedBlobSHA != blobSHA {
		o.acceptedBlobSHA = ""
		o.contentAmbiguous = true
		return
	}
	o.acceptedBlobSHA = blobSHA
}

// Derive the ledger's Git blob identity from the exact untransformed buffer
// already read by extraction. This must not reopen the source or substitute
// the distinct SHA-256 content-proof identity used by immutable builds.
func readRepairBlobSHA(src []byte) string {
	h := sha1.New()
	_, _ = h.Write([]byte("blob " + strconv.Itoa(len(src)) + "\x00"))
	_, _ = h.Write(src)
	return hex.EncodeToString(h.Sum(nil))
}

// newRepositoryReadRepairObservation does not read a clock or allocate on the
// ordinary unobserved path. Full/multipath drains deliberately have no scoped
// observation; they cannot prove publication of this one pending edit.
func newRepositoryReadRepairObservation(generation uint64, paths []string, waiters []*repositoryMutationWaiter) *repositoryReadRepairObservation {
	if len(paths) != 1 {
		return nil
	}
	var observation *repositoryReadRepairObservation
	for _, waiter := range waiters {
		request := waiter.readRepair
		if request == nil || request.complete == nil {
			continue
		}
		if observation == nil {
			observation = &repositoryReadRepairObservation{
				generation: generation, path: paths[0], causesComplete: true,
			}
		}
		if observation.causeCount == len(observation.causes) {
			observation.causesComplete = false
			continue
		}
		observation.causes[observation.causeCount] = repositoryReadRepairCause{
			session: request.session, requestIDKey: request.requestIDKey,
			admissionGeneration: waiter.generation,
		}
		observation.causeCount++
	}
	return observation
}

// Call only after releasing coordinator, lane and batch locks. Each consumer
// receives a fresh value; no retained waiter or mutable observation escapes.
func completeRepositoryReadRepair(observation *repositoryReadRepairObservation, waiters []*repositoryMutationWaiter) {
	if observation == nil {
		return
	}
	for _, waiter := range waiters {
		if request := waiter.readRepair; request != nil && request.complete != nil {
			request.complete(*observation)
		}
	}
}
