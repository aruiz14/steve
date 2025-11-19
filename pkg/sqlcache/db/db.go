package db

import (
	"context"
	"fmt"
)

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
