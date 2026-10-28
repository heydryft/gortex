//go:build darwin || linux

package store_sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
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
var vacuumOpenTestMu sync.Mutex
var vacuumOpenOriginal atomic.Uintptr
var vacuumOpenObservation atomic.Pointer[vacuumOpenProbe]

type vacuumOpenProbe struct {
	tls   *libc.TLS
	mu    sync.Mutex
	paths []string
	err   error
}

var observeVacuumNativeOpen = func(tls *libc.TLS, name uintptr, flags, mode int32) int32 {
	original := vacuumOpenOriginal.Load()
	fd := (*(*func(*libc.TLS, uintptr, int32, int32) int32)(unsafe.Pointer(&struct{ uintptr }{original})))(tls, name, flags, mode)
	probe := vacuumOpenObservation.Load()
	if probe == nil || probe.tls != tls || fd < 0 {
		return fd
	}
	path := libc.GoString(name)
	if !strings.HasPrefix(filepath.Base(path), "etilqs_") {
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
	return fd
}

func TestVacuumPhysicallyOpensTempOnStoreVolume(t *testing.T) {
	vacuumOpenTestMu.Lock()
	defer vacuumOpenTestMu.Unlock()
	s, err := Open(filepath.Join(t.TempDir(), "physical.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.writerDB.Exec(`CREATE TABLE vacuum_fixture (id INTEGER PRIMARY KEY, payload BLOB);
		WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<2048)
		INSERT INTO vacuum_fixture SELECT n, randomblob(4096) FROM ids`)
	require.NoError(t, err)
	probe := &vacuumOpenProbe{}
	require.NoError(t, s.withVacuumConnection(t.Context(), func(conn *sql.Conn) error {
		return conn.Raw(func(dc any) (result error) {
			_, tls, ok := driverConnHandles(dc)
			if !ok {
				return errors.New("vacuum physical test: actual driver TLS unavailable")
			}
			probe.tls = tls
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
			// Raw owns the physical connection here: execute through its driver
			// interface rather than recursively acquiring sql.Conn's lock.
			execer, ok := dc.(interface {
				ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error)
			})
			if !ok {
				return errors.New("vacuum physical test: context exec unavailable")
			}
			_, err = execer.ExecContext(t.Context(), `VACUUM`, nil)
			return err
		})
	}))
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
	require.Equal(t, 2048, count)
}
