//go:build darwin || linux

package store_sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// Observe the real native open, not a directory listing: SQLite unlinks its
// temporary files immediately after open. SQLite's supported VFS system-call
// API exposes the generated filename before that unlink, on Unix. No driver
// struct offsets or production hooks are added. Other OSes retain the portable
// FILE/MEMORY policy tests; this is actual Unix file-placement evidence only.
// _sqlite3RunVacuum copies the main schema cache_size into vacuum_db;
// _sqlite3PagerOpen delays opening a temporary file until its first OsWrite.
// Our real policy is 32MiB, so the fixture must exceed that cache to prove
// actual FILE spill rather than accepting a successful entirely cached copy.
const vacuumTempFixtureRows = 16384 // 64MiB payload at the production cache policy

var vacuumOpenTestMu sync.Mutex
var vacuumOpenOriginal atomic.Uintptr
var vacuumOpenObservation atomic.Pointer[vacuumOpenProbe]

type vacuumOpenProbe struct {
	directory string
	mu        sync.Mutex
	paths     []string
	err       error
	entered   chan struct{}
	release   <-chan struct{}
	once      sync.Once
}

var observeVacuumNativeOpen = func(tls *libc.TLS, name uintptr, flags, mode int32) int32 {
	original := vacuumOpenOriginal.Load()
	fd := (*(*func(*libc.TLS, uintptr, int32, int32) int32)(unsafe.Pointer(&struct{ uintptr }{original})))(tls, name, flags, mode)
	probe := vacuumOpenObservation.Load()
	if probe == nil || fd < 0 {
		return fd
	}
	path := libc.GoString(name)
	if !strings.HasPrefix(filepath.Base(path), "etilqs_") {
		return fd
	}
	directory, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil || directory != probe.directory {
		return fd
	}
	// The native open has succeeded, and SQLite has not yet unlinked it.
	// Confirm it is an actual regular file at that exact generated path.
	info, err := os.Stat(path)
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("temporary open is not a regular file: %s", path)
	}
	probe.mu.Lock()
	probe.paths = append(probe.paths, path)
	probe.err = errors.Join(probe.err, err)
	probe.mu.Unlock()
	if probe.entered != nil {
		probe.once.Do(func() { close(probe.entered); <-probe.release })
	}
	return fd
}

func TestVacuumPhysicallyOpensTempOnStoreVolume(t *testing.T) {
	vacuumOpenTestMu.Lock()
	defer vacuumOpenTestMu.Unlock()
	s, err := Open(filepath.Join(t.TempDir(), "physical.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.writerDB.Exec(fmt.Sprintf(`CREATE TABLE vacuum_fixture (id INTEGER PRIMARY KEY, payload BLOB);
		WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<%d)
		INSERT INTO vacuum_fixture SELECT n, randomblob(4096) FROM ids`, vacuumTempFixtureRows))
	require.NoError(t, err)
	directory, err := filepath.EvalSymlinks(filepath.Dir(s.Path()))
	require.NoError(t, err)
	probe := &vacuumOpenProbe{directory: directory}
	require.NoError(t, withVacuumOpenObservation(probe, func() error { return s.CompactContext(t.Context()) }))
	probe.mu.Lock()
	defer probe.mu.Unlock()
	require.NoError(t, probe.err)
	require.NotEmpty(t, probe.paths, "a successful pragma alone is not physical placement proof")
	want, err := filepath.EvalSymlinks(filepath.Dir(s.Path()))
	require.NoError(t, err)
	for _, path := range probe.paths {
		directory, err := filepath.EvalSymlinks(filepath.Dir(path))
		require.NoError(t, err)
		require.Equal(t, want, directory)
		t.Logf("VACUUM actual native temp open: %s", path)
	}
	var count int
	require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM vacuum_fixture`).Scan(&count))
	require.Equal(t, vacuumTempFixtureRows, count)
}

// SQLite's native VFS open signature is documented in the pinned generated
// _robust_open: (TLS, pathname, flags, mode). Restore its exact original
// pointer; no sandbox/environment/filename substitution is performed.
func withVacuumOpenObservation(probe *vacuumOpenProbe, run func() error) (result error) {
	tls := libc.NewTLS()
	defer tls.Close()
	vfs := sqlite3.Xsqlite3_vfs_find(tls, 0)
	if vfs == 0 || vfsAt(vfs).FxGetSystemCall == 0 || vfsAt(vfs).FxSetSystemCall == 0 {
		return errors.New("vacuum physical test: native VFS system-call API unavailable")
	}
	name, err := libc.CString("open")
	if err != nil {
		return err
	}
	defer libc.Xfree(tls, name)
	get, set := vfsAt(vfs).FxGetSystemCall, vfsAt(vfs).FxSetSystemCall
	original := (*(*func(*libc.TLS, uintptr, uintptr) uintptr)(unsafe.Pointer(&struct{ uintptr }{get})))(tls, vfs, name)
	if original == 0 {
		return errors.New("vacuum physical test: native open unavailable")
	}
	setOpen := func(pointer uintptr) int32 {
		return (*(*func(*libc.TLS, uintptr, uintptr, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{set})))(tls, vfs, name, pointer)
	}
	vacuumOpenOriginal.Store(original)
	vacuumOpenObservation.Store(probe)
	defer vacuumOpenObservation.Store(nil)
	if rc := setOpen(funcPointer(observeVacuumNativeOpen)); rc != sqlite3.SQLITE_OK {
		return fmt.Errorf("vacuum physical test: set native open rc=%d", rc)
	}
	defer func() {
		if rc := setOpen(original); rc != sqlite3.SQLITE_OK {
			result = errors.Join(result, fmt.Errorf("restore native open rc=%d", rc))
		}
	}()
	return run()
}

func TestCompactContextInterruptsInFlightVacuumAndRestoresDirectory(t *testing.T) {
	vacuumOpenTestMu.Lock()
	defer vacuumOpenTestMu.Unlock()
	path := filepath.Join(t.TempDir(), "cancel.sqlite")
	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.writerDB.Exec(fmt.Sprintf(`CREATE TABLE vacuum_fixture (id INTEGER PRIMARY KEY, payload BLOB);
		WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<%d)
		INSERT INTO vacuum_fixture SELECT n, randomblob(4096) FROM ids`, vacuumTempFixtureRows))
	require.NoError(t, err)
	require.NoError(t, s.CheckpointWAL())
	var firstPayload string
	require.NoError(t, s.db.QueryRow(`SELECT hex(payload) FROM vacuum_fixture WHERE id=1`).Scan(&firstPayload))
	writer, err := s.writerDB.Conn(t.Context())
	require.NoError(t, err)
	previous, err := vacuumTempDirectory(t.Context(), writer)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	directory, err := filepath.EvalSymlinks(filepath.Dir(path))
	require.NoError(t, err)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer unblock()
	probe := &vacuumOpenProbe{directory: directory, entered: make(chan struct{}), release: release}
	done := make(chan error, 1)
	go func() { done <- withVacuumOpenObservation(probe, func() error { return s.CompactContext(ctx) }) }()
	// Cleanup joins the operation before store.Close, even on an assertion
	// failure. This barrier is the actual successful native temp-file open
	// inside VACUUM; no cancellation sleep or canceled-before-Exec shortcut.
	var result error
	var joinOnce sync.Once
	join := func() { joinOnce.Do(func() { cancel(); unblock(); result = <-done }) }
	t.Cleanup(join)
	select {
	case <-probe.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("VACUUM never reached its physical temporary open")
	}
	join()
	require.ErrorIs(t, result, context.Canceled)
	probe.mu.Lock()
	require.NoError(t, probe.err)
	require.NotEmpty(t, probe.paths)
	probe.mu.Unlock()
	writer, err = s.writerDB.Conn(t.Context())
	require.NoError(t, err)
	after, err := vacuumTempDirectory(t.Context(), writer)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.Equal(t, previous, after)
	require.NoError(t, s.Close())
	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	var check, payload string
	var count, bytes int
	require.NoError(t, reopened.db.QueryRow(`PRAGMA quick_check`).Scan(&check))
	require.Equal(t, "ok", check)
	require.NoError(t, reopened.db.QueryRow(`SELECT count(*), sum(length(payload)) FROM vacuum_fixture`).Scan(&count, &bytes))
	require.Equal(t, vacuumTempFixtureRows, count)
	require.Equal(t, vacuumTempFixtureRows*4096, bytes)
	require.NoError(t, reopened.db.QueryRow(`SELECT hex(payload) FROM vacuum_fixture WHERE id=1`).Scan(&payload))
	require.Equal(t, firstPayload, payload, "canceled VACUUM retained durable survivor bytes")
}
