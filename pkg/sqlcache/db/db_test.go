package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestDynamicPool(t *testing.T) {
	var prepareConnCalls int
	opts := dynamicPoolOptions{
		Flags:              sqlite.OpenReadWrite | sqlite.OpenCreate,
		MaxIdleConnections: 1,
		PrepareConn: func(conn *sqlite.Conn) error {
			prepareConnCalls++
			return nil
		},
	}
	pool, err := newDynamicPool(":memory:", opts)
	require.NoError(t, err)
	defer pool.Close()

	conn1, err := pool.Take(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn1)

	err = sqlitex.ExecuteTransient(conn1, "CREATE TABLE test (id INT)", nil)
	require.NoError(t, err)

	pool.Put(conn1)

	assert.Len(t, pool.idleConns, 1, "Connection should be in idle channel")

	conn2, err := pool.Take(t.Context())
	require.NoError(t, err)
	assert.Equal(t, conn1, conn2, "Should reuse the idle connection")
	assert.Len(t, pool.idleConns, 0, "Idle channel should be empty")
	assert.Equal(t, 1, prepareConnCalls, "PrepareConn should be called once")

	pool.Put(conn2)
}

func TestDynamicPool_MaxIdle(t *testing.T) {
	opts := dynamicPoolOptions{
		Flags:              sqlite.OpenReadWrite | sqlite.OpenMemory,
		MaxIdleConnections: 1, // Only keep 1
	}
	pool, err := newDynamicPool(":memory:", opts)
	require.NoError(t, err)
	defer pool.Close()

	ctx := t.Context()

	conn1, err := pool.Take(ctx)
	require.NoError(t, err)
	conn2, err := pool.Take(ctx)
	require.NoError(t, err)

	pool.Put(conn1) // will be reused
	assert.Len(t, pool.idleConns, 1)

	pool.Put(conn2) // should be discarded
	assert.Len(t, pool.idleConns, 1, "max idle connections was exceeded")
}

func TestDynamicPool_DirtyConnectionsShouldBeDiscarded(t *testing.T) {
	pool, err := newDynamicPool(":memory:", dynamicPoolOptions{MaxIdleConnections: 1})
	require.NoError(t, err)
	defer pool.Close()

	conn, err := pool.Take(t.Context())
	require.NoError(t, err)

	// 1. Create a "dirty" state: A prepared statement that hasn't been reset/finalized.
	// We Prepare it, Step it once, but do NOT finalize it.
	stmt, err := conn.Prepare("SELECT 1")
	require.NoError(t, err)
	_, err = stmt.Step() // Step leaves it active
	require.NoError(t, err)

	// Intentionally NOT finalizing it.
	// stmt.Finalize()

	pool.Put(conn)

	// Since the connection had an active statement, CheckReset() should fail,
	// and the pool should close it rather than adding it to idleConns.
	assert.Len(t, pool.idleConns, 0, "Dirty connection should be discarded")
	assert.ErrorContains(t, conn.Close(), "already closed")
}

func TestDynamicPool_ContextCancellation(t *testing.T) {
	pool, err := newDynamicPool(":memory:", dynamicPoolOptions{})
	require.NoError(t, err)
	defer pool.Close()

	ctx, cancel := context.WithCancel(t.Context())

	conn, err := pool.Take(ctx)
	require.NoError(t, err)

	cancel()
	time.Sleep(100 * time.Millisecond)

	err = sqlitex.ExecuteTransient(conn, "SELECT 1", nil)
	assert.ErrorContains(t, err, "interrupt")

	pool.Put(conn)

	// Verify that interrupted connections can still be reused
	assert.Len(t, pool.idleConns, 1, "Connection should be reused")
	conn2, err := pool.Take(t.Context())
	require.NoError(t, err)

	err = sqlitex.ExecuteTransient(conn2, "SELECT 1", nil)
	assert.NoError(t, err, "Interrupt should be cleared on reuse")
	pool.Put(conn2)
}

func TestDynamicPool_Close(t *testing.T) {
	pool, err := newDynamicPool(":memory:", dynamicPoolOptions{})
	require.NoError(t, err)

	c1, err := pool.Take(t.Context())
	require.NoError(t, err)
	c2, err := pool.Take(t.Context())
	require.NoError(t, err)

	// 1 active, 1 idle connections
	pool.Put(c1)
	assert.Len(t, pool.idleConns, 1)

	err = pool.Close()
	require.NoError(t, err)

	// Idle pool should be drained
	assert.Len(t, pool.idleConns, 0)

	err = sqlitex.ExecuteTransient(c2, "SELECT 1", nil)
	assert.Error(t, err, "active connection should not be usable after pool.Close")

	// 5. Subsequent Take should fail
	_, err = pool.Take(t.Context())
	assert.ErrorIs(t, err, ErrPoolClosed)
}
