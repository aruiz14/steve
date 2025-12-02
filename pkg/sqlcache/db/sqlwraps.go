package db

import (
	"database/sql"
)

// Stmt is an interface over a subset of sql.Stmt methods
// rationale: allow mocking
type Stmt interface {
	// SQLStmt unwraps the original sql.Stmt
	SQLStmt() *sql.Stmt

	// GetQueryString returns the original text used to prepare this statement
	GetQueryString() string
}

// row wraps a sql.Rows, keeping track of the original query used to produce it
type rows struct {
	*sql.Rows
	queryString string
}

// Err wraps the original *sql.Rows's Err() with a QueryError
func (r rows) Err() error {
	if err := r.Rows.Err(); err != nil {
		return &QueryError{QueryString: r.queryString, Err: err}
	}
	return nil
}

// stmt is a wrapper around sql.Stmt, wrapping a sql.Stmt and keeping track of the original query string
// Most of the methods will wrap original errors with a QueryError
type stmt struct {
	*sql.Stmt
	queryString string
}

func (s *stmt) SQLStmt() *sql.Stmt {
	return s.Stmt
}

func (s *stmt) GetQueryString() string {
	return s.queryString
}
