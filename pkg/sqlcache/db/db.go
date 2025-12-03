package db

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rancher/lasso/pkg/log"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var ErrPoolClosed = errors.New("connection pool is closed")

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

type dbPool interface {
	Take(ctx context.Context) (*sqlite.Conn, error)
	Put(conn *sqlite.Conn)
	Close() error
}

type dynamicPoolOptions struct {
	Flags              sqlite.OpenFlags
	PrepareConn        sqlitex.ConnPrepareFunc
	MaxIdleConnections int
}

func newDynamicPool(dbURI string, opts dynamicPoolOptions) (*dynamicPool, error) {
	newConnection := func() (*sqlite.Conn, error) {
		conn, err := sqlite.OpenConn(dbURI, opts.Flags)
		if err != nil {
			return nil, err
		}
		if opts.PrepareConn != nil {
			if err := opts.PrepareConn(conn); err != nil {
				conn.Close()
				return nil, err
			}
		}
		return conn, nil
	}
	if opts.MaxIdleConnections <= 0 {
		opts.MaxIdleConnections = 2
	}

	return &dynamicPool{
		idleConns:         make(chan *sqlite.Conn, opts.MaxIdleConnections),
		closed:            make(chan struct{}),
		inUse:             make(map[*sqlite.Conn]context.CancelFunc),
		newConnectionFunc: newConnection,
	}, nil
}

type dynamicPool struct {
	idleConns         chan *sqlite.Conn
	closed            chan struct{}
	newConnectionFunc func() (*sqlite.Conn, error)

	mu    sync.Mutex
	inUse map[*sqlite.Conn]context.CancelFunc
}

func (d *dynamicPool) isClosed() bool {
	select {
	case <-d.closed:
		return true
	default:
		return false
	}
}

func (d *dynamicPool) Take(ctx context.Context) (*sqlite.Conn, error) {
	var conn *sqlite.Conn
	var err error
	select {
	case <-d.closed:
		return nil, ErrPoolClosed
	case conn = <-d.idleConns:
	default:
		conn, err = d.newConnectionFunc()
		if err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	conn.SetInterrupt(ctx.Done())

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.isClosed() {
		cancel()
		conn.Close()
		return nil, ErrPoolClosed
	}
	d.inUse[conn] = cancel

	return conn, nil
}

func (d *dynamicPool) Put(conn *sqlite.Conn) {
	if conn == nil {
		return
	}
	if query := conn.CheckReset(); query != "" {
		log.Errorf("released connection has active statement: %q", query)
		conn.Close()
		return
	}

	conn.SetInterrupt(nil)

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.isClosed() {
		conn.Close()
		return
	}
	if cancel, ok := d.inUse[conn]; ok {
		cancel()
		delete(d.inUse, conn)
	}

	// Allow reusing the connection
	select {
	case d.idleConns <- conn:
		return
	default:
		conn.Close()
		return
	}
}

func (d *dynamicPool) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.isClosed() {
		return nil
	}
	close(d.closed)

	close(d.idleConns)
	var errs []error
	for conn := range d.idleConns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	for _, cancel := range d.inUse {
		cancel()
	}
	d.inUse = nil

	return errors.Join(errs...)
}
