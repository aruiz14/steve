package db

import (
	"database/sql"
	"errors"
	"fmt"

	"zombiezen.com/go/sqlite"
)

var (
	scanBeforeNext       = errors.New("scan: called before Next()")
	scanAfterDone        = errors.New("scan: called after rows finished or closed")
	scanArgsMismatch     = errors.New("scan: incorrect number of arguments")
	scanIncompatibleType = errors.New("scan: incompatible types")
)

// result implements the sql.Result interface
type result struct {
	lastInsertID int64
	rowsAffected int
}

func (r result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r result) RowsAffected() (int64, error) {
	return int64(r.rowsAffected), nil
}

// rowsImpl mimics the methods from sql.Rows
type rowsImpl struct {
	pstmt     *sqlite.Stmt
	closeFunc func() error

	started, done bool
	err           error
}

func newRows(pstmt *sqlite.Stmt, closeFunc func() error) Rows {
	return &rowsImpl{
		pstmt:     pstmt,
		closeFunc: closeFunc,
	}
}

func (r *rowsImpl) Close() error {
	return r.closeFunc()
}

func (r *rowsImpl) Err() error {
	return r.err
}

func (r *rowsImpl) Next() bool {
	if r.done {
		return false
	}

	if rowFound, err := r.pstmt.Step(); err != nil {
		r.err = err
		r.done = true
		return false
	} else if !rowFound {
		r.done = true
		return false
	}
	r.started = true

	return true
}

func (r *rowsImpl) Scan(args ...any) error {
	if r.done {
		return scanAfterDone
	}
	if !r.started {
		return scanBeforeNext
	}
	if got, want := len(args), r.pstmt.ColumnCount(); got != want {
		return fmt.Errorf("%w, got %d, expected %d", scanArgsMismatch, got, want)
	}
	for i, dest := range args {
		if dest == nil {
			// nil passed, ignore
			continue
		}
		// We switch on the SQLITE Column Type to know how to read
		colType := r.pstmt.ColumnType(i)
		switch colType {
		case sqlite.TypeNull:
			switch dest := dest.(type) {
			case sql.Scanner:
				if err := dest.Scan(nil); err != nil {
					return err
				}
			case *any:
				*dest = nil
			case *[]byte:
				*dest = nil
			case *sql.RawBytes:
				*dest = nil
			default:
				return fmt.Errorf("%w at argument %d, provided %T but value is null", scanIncompatibleType, i, dest)
			}
		case sqlite.TypeInteger:
			value := r.pstmt.ColumnInt64(i)
			switch dest := dest.(type) {
			case sql.Scanner:
				if err := dest.Scan(value); err != nil {
					return err
				}
			case *int64:
				*dest = value
			case *int:
				*dest = int(value)
			case *int32:
				*dest = int32(value)
			case *uint32:
				*dest = uint32(value)
			case *uint64:
				*dest = uint64(value)
			case *bool:
				*dest = value != 0
			default:
				return fmt.Errorf("%w at argument %d, provided %T cannot be converted from %T", scanIncompatibleType, i, dest, value)
			}
		case sqlite.TypeFloat:
			value := r.pstmt.ColumnFloat(i)
			switch dest := dest.(type) {
			case sql.Scanner:
				if err := dest.Scan(value); err != nil {
					return err
				}
			case *float64:
				*dest = value
			case *float32:
				*dest = float32(value)
			default:
				return fmt.Errorf("%w at argument %d, provided %T cannot be converted from %T", scanIncompatibleType, i, dest, value)
			}
		case sqlite.TypeText:
			value := r.pstmt.ColumnText(i)
			switch dest := dest.(type) {
			case sql.Scanner:
				if err := dest.Scan(value); err != nil {
					return err
				}
			case *string:
				*dest = value
			case *[]byte:
				*dest = []byte(value)
			case *sql.RawBytes:
				*dest = []byte(value)
			default:
				return fmt.Errorf("%w at argument %d, provided %T cannot be converted from %T", scanIncompatibleType, i, dest, value)
			}
		case sqlite.TypeBlob:
			dataLen := r.pstmt.ColumnLen(i)
			value := make([]byte, dataLen)
			if dataLen > 0 {
				if _, err := r.pstmt.ColumnReader(i).Read(value); err != nil {
					return err
				}
			}
			switch dest := dest.(type) {
			case *[]byte:
				*dest = value
			case *sql.RawBytes:
				*dest = value
			case sql.Scanner:
				if err := dest.Scan(value); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%w at argument %d, provided %T cannot be converted from blob", scanIncompatibleType, i, dest)
			}
		}
	}
	return nil
}
