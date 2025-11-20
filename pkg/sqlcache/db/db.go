package db

import (
	"context"
	"fmt"

	"zombiezen.com/go/sqlite/sqlitex"
)

// VirtualStmt is not really a statement, as they are bound to a connection.
// Every "constant" query meant to be executed multiple times are meant to create a VirtualStatement and use the "Stmt" methods in TxClient
// Using a specific type reminds us to prepare statements on every connection
type VirtualStmt string

func (c *client) ReadOnlyTransaction(ctx context.Context, f WithTransactionFunction) error {
	conn, err := c.readPool.Take(ctx)
	if err != nil {
		return fmt.Errorf("getting write connection %w", err)
	}
	defer c.readPool.Put(conn)

	endTx := sqlitex.Transaction(conn)
	defer endTx(&err)

	err = f(newTxClientFromConn(conn, c.queryLogger))
	return err
}

func (c *client) WriteTransaction(ctx context.Context, f WithTransactionFunction) error {
	conn, err := c.writePool.Take(ctx)
	if err != nil {
		return fmt.Errorf("getting write connection %w", err)
	}
	defer c.writePool.Put(conn)

	endTx, err := sqlitex.ImmediateTransaction(conn)
	if err != nil {
		return fmt.Errorf("starting write transaction: %w", err)
	}
	defer endTx(&err)

	err = f(newTxClientFromConn(conn, c.queryLogger))
	return err
}
