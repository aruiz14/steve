package db

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/db/logging"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type txClient struct {
	conn        *sqlite.Conn
	queryLogger logging.QueryLogger
}

func newTxClientFromConn(conn *sqlite.Conn, queryLogger logging.QueryLogger) TxClient {
	if queryLogger == nil {
		queryLogger = &logging.NoopQueryLogger{}
	}
	c := &txClient{conn: conn, queryLogger: queryLogger}
	return c
}

func (c txClient) Exec(query string, args ...any) (sql.Result, error) {
	defer c.queryLogger.Log(time.Now(), query, args)
	opts := sqlitex.ExecOptions{Args: args}
	if err := sqlitex.ExecuteTransient(c.conn, query, &opts); err != nil {
		return nil, &QueryError{
			QueryString: query,
			Err:         err,
		}
	}
	return &result{
		lastInsertID: c.conn.LastInsertRowID(),
		rowsAffected: c.conn.Changes(),
	}, nil
}

func (c txClient) ExecStmt(query VirtualStmt, args ...any) (sql.Result, error) {
	defer c.queryLogger.Log(time.Now(), string(query), args)
	opts := sqlitex.ExecOptions{Args: args}
	if err := sqlitex.Execute(c.conn, string(query), &opts); err != nil {
		return nil, &QueryError{
			QueryString: string(query),
			Err:         err,
		}
	}
	return &result{
		lastInsertID: c.conn.LastInsertRowID(),
		rowsAffected: c.conn.Changes(),
	}, nil
}

func (c txClient) Query(query string, args ...any) (Rows, error) {
	prepared, _, err := c.conn.PrepareTransient(query)
	if err != nil {
		return nil, err
	}
	return toStmtWrapper(prepared, true).Query(args...)
}

func (c txClient) QueryStmt(query VirtualStmt, args ...any) (Rows, error) {
	prepared, err := c.conn.Prepare(string(query))
	if err != nil {
		return nil, err
	}
	return toStmtWrapper(prepared, false).Query(args...)
}

func toStmtWrapper(pstmt *sqlite.Stmt, transient bool) *stmtWrapper {
	return &stmtWrapper{pstmt: pstmt, transient: transient}
}

type stmtWrapper struct {
	pstmt     *sqlite.Stmt
	transient bool
}

func (s *stmtWrapper) bindArgs(args ...any) error {
	if got, want := len(args), s.pstmt.BindParamCount(); got != want {
		return fmt.Errorf("expected %d arguments, got %d", want, got)
	}
	for i, arg := range args {
		paramPos := i + 1 // params starts with 1
		switch v := arg.(type) {
		case int:
			s.pstmt.BindInt64(paramPos, int64(v))
		case int32:
			s.pstmt.BindInt64(paramPos, int64(v))
		case int64:
			s.pstmt.BindInt64(paramPos, v)
		case float32:
			s.pstmt.BindFloat(paramPos, float64(v))
		case float64:
			s.pstmt.BindFloat(paramPos, v)
		case bool:
			s.pstmt.BindBool(paramPos, v)
		case string:
			s.pstmt.BindText(paramPos, v)
		case []byte:
			s.pstmt.BindBytes(paramPos, v)
		case sql.RawBytes:
			s.pstmt.BindBytes(paramPos, v)
		case nil:
			s.pstmt.BindNull(paramPos)
		default:
			return fmt.Errorf("unsupported type %T", v)
		}
	}
	return nil
}

func (s *stmtWrapper) Query(args ...any) (res Rows, err error) {
	defer func() {
		// Only close on error
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	if err := s.bindArgs(args...); err != nil {
		return nil, err
	}
	return newRows(s.pstmt, s.Close), nil
}

func (s *stmtWrapper) Close() error {
	var errs []error
	if err := s.pstmt.Reset(); err != nil {
		errs = append(errs, err)
	}
	if s.transient {
		if err := s.pstmt.Finalize(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
