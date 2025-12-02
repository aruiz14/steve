package db

import (
	"database/sql"
)

// TxClient exposes a subset of a transaction's interface
// rationale 1: explicitly forbid direct access to Commit and Rollback functionality
// rationale 2: ease mocking
type TxClient interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecStmt(query VirtualStmt, args ...any) (sql.Result, error)
	Query(query string, args ...any) (Rows, error)
	QueryStmt(query VirtualStmt, args ...any) (Rows, error)
}

// Rows represents sql.Rows. It exposes method to navigate the rows, read their outputs, and close them.
type Rows interface {
	Next() bool
	Err() error
	Close() error
	Scan(dest ...any) error
}
