package store_sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVacuumDedicatedConnectionKeepsPoolsInMemory(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "quote's store.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ctx := t.Context()
	var before string
	writer, err := s.writerDB.Conn(ctx)
	require.NoError(t, err)
	before, err = vacuumTempDirectory(ctx, writer)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	require.NoError(t, s.withVacuumConnection(ctx, func(conn *sql.Conn) error {
		var mode, pooledWriter, pooledReader int
		require.NoError(t, conn.QueryRowContext(ctx, `PRAGMA temp_store`).Scan(&mode))
		require.Equal(t, 1, mode, "only the dedicated connection uses FILE")
		require.NoError(t, s.writerDB.QueryRowContext(ctx, `PRAGMA temp_store`).Scan(&pooledWriter))
		require.NoError(t, s.db.QueryRowContext(ctx, `PRAGMA temp_store`).Scan(&pooledReader))
		require.Equal(t, 2, pooledWriter)
		require.Equal(t, 2, pooledReader)
		directory, err := vacuumTempDirectory(ctx, conn)
		require.NoError(t, err)
		require.Equal(t, filepath.Dir(s.Path()), directory)
		requireNoCloseCheckpoint(t, conn)
		return nil
	}))
	writer, err = s.writerDB.Conn(ctx)
	require.NoError(t, err)
	defer writer.Close()
	after, err := vacuumTempDirectory(ctx, writer)
	require.NoError(t, err)
	require.Equal(t, before, after, "process-global temp directory restored")
}

func TestVacuumDirectoryRestoredAfterCanceledSQL(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	conn, err := s.writerDB.Conn(t.Context())
	require.NoError(t, err)
	before, err := vacuumTempDirectory(t.Context(), conn)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	ctx, cancel := context.WithCancel(t.Context())
	err = s.withVacuumConnection(ctx, func(conn *sql.Conn) error {
		cancel()
		_, err := conn.ExecContext(ctx, `VACUUM`)
		return err
	})
	require.ErrorIs(t, err, context.Canceled)
	conn, err = s.writerDB.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	after, err := vacuumTempDirectory(t.Context(), conn)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestCompactContextCancelsHeldWriterAdmission(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.CompactContext(ctx) }()
	deadline := time.After(5 * time.Second)
	for s.writeMu.waiting() == 0 {
		select {
		case <-deadline:
			t.Fatal("compaction did not reach held write gate")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, ErrMaintenanceBusy)
	case <-time.After(5 * time.Second):
		t.Fatal("compaction ignored cancellation at write gate")
	}
}

func TestVacuumDirectoryGuardCancelsWithoutRunningCallback(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	vacuumTempDirectoryGate.Lock()
	defer vacuumTempDirectoryGate.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.withVacuumConnection(ctx, func(*sql.Conn) error {
			return errors.New("unexpected callback after canceled guard wait")
		})
	}()
	deadline := time.After(5 * time.Second)
	for vacuumTempDirectoryGate.waiting() == 0 {
		select {
		case <-deadline:
			t.Fatal("compaction did not reach directory guard")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("directory guard ignored cancellation")
	}
}

func TestVacuumUnconfirmedDirectoryDoesNotRun(t *testing.T) {
	s, err := Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	called := false
	err = s.withVacuumConnection(t.Context(), func(*sql.Conn) error { called = true; return nil })
	require.ErrorIs(t, err, ErrVacuumTempDirectory)
	require.False(t, called)
	require.NoError(t, s.CompactContext(t.Context()), "memory compatibility path does not open a second database")
	require.Error(t, s.CompactContext(nil))
}
