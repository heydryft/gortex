package indexer

import "time"

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
