package db

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func setupTestDB(t *testing.T) *sqlite.Conn {
	conn, err := sqlite.OpenConn(":memory:", 0)
	require.NoError(t, err)

	// Fixed formatting as requested
	err = sqlitex.ExecuteTransient(conn, `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT,
		age INTEGER,
		active BOOLEAN,
		data BLOB,
		score REAL
	);`, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

func TestTxClient_ExecStmt(t *testing.T) {
	conn := setupTestDB(t)
	client := newTxClientFromConn(conn, nil)

	q := VirtualStmt("INSERT INTO users (name) VALUES (?)")

	res, err := client.ExecStmt(q, "TestUser")
	require.NoError(t, err)

	aff, _ := res.RowsAffected()
	assert.Equal(t, int64(1), aff)
}

func TestTxClient_InsertThenUpdate(t *testing.T) {
	conn := setupTestDB(t)
	client := newTxClientFromConn(conn, nil)

	res, err := client.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "Alice", 30)
	require.NoError(t, err)

	lastInsertId, _ := res.LastInsertId()
	assert.Equal(t, int64(1), lastInsertId)

	rowsAffected, _ := res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected)

	res, err = client.Exec("UPDATE users SET age = ? WHERE name = ?", 31, "Alice")
	require.NoError(t, err)
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected)
}

func TestTxClient_Query(t *testing.T) {
	conn := setupTestDB(t)
	client := newTxClientFromConn(conn, nil)

	blobData := []byte("hello world")

	// Insert various types
	_, err := client.Exec("INSERT INTO users (name, age, active, score, data) VALUES (?, ?, ?, ?, ?)",
		"Charlie",
		25,       // Integer
		true,     // Boolean
		99.9,     // Float
		blobData, // Data
	)
	require.NoError(t, err)

	// Insert NULLs
	_, err = client.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "NullUser", nil)
	require.NoError(t, err)

	q := VirtualStmt("SELECT name, age, active, score, data FROM users ORDER BY id ASC")
	rows, err := client.QueryStmt(q)
	require.NoError(t, err)
	defer rows.Close()

	// First row, complete data
	assert.True(t, rows.Next())

	var (
		name   string
		age    int
		active bool
		score  float64
		data   []byte
	)
	err = rows.Scan(&name, &age, &active, &score, &data)
	require.NoError(t, err)

	assert.Equal(t, "Charlie", name)
	assert.Equal(t, 25, age)
	assert.True(t, active)
	assert.Equal(t, 99.9, score)
	assert.Equal(t, blobData, data)

	// Second row contains null data
	assert.True(t, rows.Next())

	var ageNullable sql.Null[int]
	var activeNullable sql.Null[bool]
	var scoreNullable sql.Null[float64]
	var dataNullable sql.Null[sql.RawBytes]
	err = rows.Scan(&name, &ageNullable, &activeNullable, &scoreNullable, &dataNullable)
	require.NoError(t, err)

	assert.Equal(t, "NullUser", name)
	assert.False(t, ageNullable.Valid)
	assert.False(t, activeNullable.Valid)
	assert.False(t, scoreNullable.Valid)
	assert.False(t, dataNullable.Valid)
}
