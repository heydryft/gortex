package indexer

import (
	"context"
	"errors"
	"testing"
)

func TestRepositoryReadRepairCausesKeepReusedWireIDsDistinct(t *testing.T) {
	request := &repositoryReadRepairRequest{session: "session", requestIDKey: "n:7", complete: func(repositoryReadRepairObservation) {}}
	waiters := []*repositoryMutationWaiter{
		{generation: 4, readRepair: request},
		{generation: 5, readRepair: request},
	}
	observation := newRepositoryReadRepairObservation(5, []string{"file.go"}, waiters)
	if observation == nil || observation.generation != 5 || observation.causeCount != 2 || !observation.causesComplete {
		t.Fatalf("missing drain/admission observations: %+v", observation)
	}
	if observation.causes[0].admissionGeneration != 4 || observation.causes[1].admissionGeneration != 5 {
		t.Fatalf("reused wire IDs aliased admissions: %+v", observation.causes)
	}
}

func TestRepositoryReadRepairUnobservedPathDoesNotAllocate(t *testing.T) {
	paths := []string{"file.go"}
	waiters := []*repositoryMutationWaiter{{generation: 1}}
	if allocations := testing.AllocsPerRun(100, func() {
		if newRepositoryReadRepairObservation(1, paths, waiters) != nil {
			panic("unobserved request acquired evidence")
		}
	}); allocations != 0 {
		t.Fatalf("unobserved allocations: %v", allocations)
	}
}

func TestRepositoryReadRepairCauseOverflowAndBroadScopesRemainUnavailable(t *testing.T) {
	request := &repositoryReadRepairRequest{complete: func(repositoryReadRepairObservation) {}}
	waiters := make([]*repositoryMutationWaiter, repositoryReadRepairCauseCap+1)
	for i := range waiters {
		waiters[i] = &repositoryMutationWaiter{generation: uint64(i + 1), readRepair: request}
	}
	observation := newRepositoryReadRepairObservation(uint64(len(waiters)), []string{"file.go"}, waiters)
	if observation == nil || observation.causesComplete || observation.causeCount != repositoryReadRepairCauseCap {
		t.Fatalf("overflow incorrectly complete: %+v", observation)
	}
	for _, paths := range [][]string{nil, {"a.go", "b.go"}} {
		if newRepositoryReadRepairObservation(1, paths, waiters) != nil {
			t.Fatalf("broad scope acquired single-file evidence: %v", paths)
		}
	}
}

func TestRepositoryReadRepairCompletionIsOutsideCoordinatorLocks(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			coordinator := newRepositoryMutationCoordinator(func([]string) (*IndexResult, error) {
				if failed {
					return nil, errors.New("failed execution")
				}
				return &IndexResult{StaleFileCount: 1}, nil
			})
			var snapshot repositoryReadRepairObservation
			_, err := coordinator.reconcileObserved(t.Context(), []string{"file.go"}, &repositoryReadRepairRequest{
				complete: func(observation repositoryReadRepairObservation) {
					// Both operations would deadlock if completion still owned
					// coordinator.mu or the repository mutation lane.
					coordinator.mu.Lock()
					coordinator.mu.Unlock()
					if laneErr := coordinator.runExclusive(context.Background(), func() error { return nil }); laneErr != nil {
						panic(laneErr)
					}
					snapshot = observation
					observation.causes[0].session = "caller mutation"
				},
			})
			coordinator.work.Wait()
			if (err != nil) != failed || snapshot.executionSucceeded == failed || snapshot.started.IsZero() || snapshot.finished.Before(snapshot.started) {
				t.Fatalf("execution window mismatch: failed=%v err=%v observation=%+v", failed, err, snapshot)
			}
			if snapshot.causes[0].session != "" {
				t.Fatal("mutable completion value escaped")
			}
		})
	}
}
