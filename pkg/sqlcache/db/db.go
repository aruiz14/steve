package db

import (
	"context"
	"fmt"
)

// VirtualStmt is not really a statement, as they are bound to a connection.
// Every "constant" query meant to be executed multiple times are meant to create a VirtualStatement and use the "Stmt" methods in TxClient
// Using a specific type reminds us to prepare statements on every connection
type VirtualStmt Stmt

func (c *client) ReadOnlyTransaction(ctx context.Context, f WithTransactionFunction) error {
	if err := c.withTransaction(ctx, false, f); err != nil {
		return fmt.Errorf("ReadOnlyTransaction: %w", err)
	}
	return nil
}

func (c *client) WriteTransaction(ctx context.Context, f WithTransactionFunction) error {
	if err := c.withTransaction(ctx, true, f); err != nil {
		return fmt.Errorf("WriteTransaction: %w", err)
	}
	return nil
}
