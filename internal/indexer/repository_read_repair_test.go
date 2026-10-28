package indexer

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestRepositoryReadRepairUsesGitBlobIdentity(t *testing.T) {
	for _, test := range []struct{ content, oid string }{
		{"", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"},
		{"hello\n", "ce013625030ba8dba906f756967f9e9ca394464a"},
	} {
		if got := readRepairBlobSHA([]byte(test.content)); got != test.oid {
			t.Fatalf("Git blob identity = %q, want %q", got, test.oid)
		}
	}
}

func TestRepositoryReadRepairAcceptedContentCannotChangeOwners(t *testing.T) {
	observation := &repositoryReadRepairObservation{path: "target.go"}
	older := readRepairBlobSHA([]byte("old bytes"))
	newer := readRepairBlobSHA([]byte("new bytes"))
	observation.acceptFreshBlob("other.go", older)
	if observation.acceptedBlobSHA != "" {
		t.Fatal("another path supplied target content")
	}
	observation.acceptFreshBlob("target.go", older)
	immutable := *observation
	observation.acceptFreshBlob("target.go", newer)
	observation.acceptFreshBlob("target.go", older)
	if observation.acceptedBlobSHA != "" || !observation.contentAmbiguous || immutable.acceptedBlobSHA != older {
		t.Fatal("ambiguous content regained authority or changed an earlier snapshot")
	}
}

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
			var batchGate sync.RWMutex
			coordinator.batchMutationGate = &batchGate
			var snapshot repositoryReadRepairObservation
			_, err := coordinator.reconcileObserved(t.Context(), []string{"file.go"}, &repositoryReadRepairRequest{
				complete: func(observation repositoryReadRepairObservation) {
					// Both operations would deadlock if completion still owned
					// coordinator.mu or the repository mutation lane.
					coordinator.mu.Lock()
					coordinator.mu.Unlock()
					batchGate.Lock()
					batchGate.Unlock()
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

func TestRepositoryReadRepairContentRequiresSuccessfulAuthorityAndResult(t *testing.T) {
	for _, test := range []struct {
		name      string
		result    *IndexResult
		authority bool
	}{
		{name: "missing_result"},
		{name: "full", result: &IndexResult{FullRetrack: true}},
		{name: "failed", result: &IndexResult{FailedFiles: []string{"file.go"}}},
		{name: "deleted", result: &IndexResult{DeletedFileCount: 1}},
		{name: "quarantined", result: &IndexResult{QuarantinedFiles: 1}},
		{name: "skipped", result: &IndexResult{SkippedFiles: 1}},
		{name: "authority_refusal", result: &IndexResult{StaleFileCount: 1}, authority: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			coordinator := newRepositoryMutationCoordinator(nil)
			coordinator.readRepairExecutor = func(_ []string, observation *repositoryReadRepairObservation) (*IndexResult, error) {
				observation.acceptFreshBlob("file.go", readRepairBlobSHA([]byte("accepted bytes")))
				return test.result, nil
			}
			if test.authority {
				coordinator.outputGeneration = func(fn func(func(*OutputSourceContent)) error) error {
					if err := fn(func(*OutputSourceContent) {}); err != nil {
						return err
					}
					return errors.New("superseded authority")
				}
			}
			var snapshot repositoryReadRepairObservation
			_, _ = coordinator.reconcileObserved(t.Context(), []string{"file.go"}, &repositoryReadRepairRequest{
				complete: func(observation repositoryReadRepairObservation) { snapshot = observation },
			})
			coordinator.work.Wait()
			if snapshot.executionSucceeded || snapshot.acceptedBlobSHA != "" {
				t.Fatalf("unsuccessful publisher retained content authority: %+v", snapshot)
			}
		})
	}
}
