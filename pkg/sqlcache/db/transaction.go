package db

import (
	"database/sql"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/db/logging"
)

// txClient is the main implementation of TxClient, delegates to sql.Tx
// other implementations exist for testing purposes
type txClient struct {
	tx          *sql.Tx
	queryLogger logging.QueryLogger
}

type TxClientOption func(*txClient)

func NewTxClient(tx *sql.Tx, opts ...TxClientOption) TxClient {
	c := &txClient{tx: tx, queryLogger: &logging.NoopQueryLogger{}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c txClient) Exec(query string, args ...any) (sql.Result, error) {
	defer c.queryLogger.Log(time.Now(), query, args)
	res, err := c.tx.Exec(query, args...)
	if err != nil {
		err = &QueryError{
			QueryString: query,
			Err:         err,
		}
	}
	return res, err
}

func (c txClient) ExecStmt(stmt VirtualStmt, args ...any) (sql.Result, error) {
	defer c.queryLogger.Log(time.Now(), stmt.GetQueryString(), args)
	res, err := c.tx.Stmt(stmt.SQLStmt()).Exec(args...)
	if err != nil {
		err = &QueryError{
			QueryString: stmt.GetQueryString(),
			Err:         err,
		}
	}
	return res, err
}

func (c txClient) Query(query string, args ...any) (Rows, error) {
	res, err := c.tx.Query(query, args...)
	if err != nil {
		return nil, &QueryError{
			QueryString: query,
			Err:         err,
		}
	}
	return rows{Rows: res, queryString: query}, nil
}

func (c txClient) QueryStmt(stmt VirtualStmt, args ...any) (Rows, error) {
	return c.tx.Stmt(stmt.SQLStmt()).Query(args...)
}

func (c txClient) Prepare(query string) (Stmt, error) {
	prepared, err := c.tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmt{
		Stmt:        prepared,
		queryString: query,
	}, nil
}

func WithQueryLogger(logger logging.QueryLogger) TxClientOption {
	return func(c *txClient) {
		c.queryLogger = logger
	}
}
