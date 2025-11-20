package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRows_Scan_Errors(t *testing.T) {
	conn := setupTestDB(t)
	client := newTxClientFromConn(conn, nil)
	_, err := client.Exec("INSERT INTO users (name) VALUES (?)", "Tester")
	assert.NoError(t, err)

	rows, err := client.Query("SELECT name FROM users")
	assert.NoError(t, err)
	defer rows.Close()

	// 1. Scan before Next
	var name string
	assert.ErrorIs(t, rows.Scan(&name), scanBeforeNext)

	// 2. Scan argument mismatch
	rows.Next()
	assert.ErrorIs(t, rows.Scan(&name, &name), scanArgsMismatch)

	// 3. Incompatible types
	var intVar int
	assert.ErrorIs(t, rows.Scan(&intVar), scanIncompatibleType)
}
