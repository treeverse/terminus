// Package sql provides a store.Store that keeps data on a SQL database.
package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/treeverse/terminus/pkg/store"
)

func NewSQLStore(db *sql.DB, defaultQuotaBytes int64) (store.Store, error) {
	return &SQLStore{db: db, DefaultQuotaBytes: defaultQuotaBytes}, nil
}

// SQLStore is a Store that keeps results in a SQL database.
type SQLStore struct {
	db                *sql.DB
	DefaultQuotaBytes int64
}

func (s *SQLStore) transact(ctx context.Context, fn func(tx *sql.Tx) (interface{}, error)) (interface{}, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	ret, err := fn(tx)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			err = fmt.Errorf("%w; additionally during rollback: %s", err, rollbackErr)
		}
		return nil, err
	}
	commitErr := tx.Commit()
	return ret, commitErr
}

func (s *SQLStore) Get(ctx context.Context, key string) (store.Value, error) {
	ret, err := s.transact(ctx, func(tx *sql.Tx) (interface{}, error) {
		var value store.Value
		row := tx.QueryRowContext(ctx, `SELECT size_bytes FROM usage WHERE key = $1`, key)
		err := row.Scan(&value.SizeBytes)
		return value, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return store.Value{}, store.ErrNotFound
	}
	if err != nil {
		return store.Value{}, err
	}
	return ret.(store.Value), nil
}

// checkQuota returns true if key is still within quota.
func (s *SQLStore) checkQuota(ctx context.Context, tx *sql.Tx, key string) (bool, error) {
	// TODO(ariels): Add "next check" backoff.
	row := tx.QueryRowContext(ctx, `
                SELECT NULL FROM usage WHERE key=$1 AND size_bytes > COALESCE(quota, $2)`,
		key, s.DefaultQuotaBytes)
	var ignore *string
	err := row.Scan(&ignore)
	if err == nil {
		// Bad keys return a single (NULL) row, good keys return no rows.
		return false, nil
	}
	ok := errors.Is(err, sql.ErrNoRows)
	if ok {
		return true, nil
	}
	return true, err
}

func (s *SQLStore) Set(ctx context.Context, key string, value store.Value) error {
	ok, err := s.transact(ctx, func(tx *sql.Tx) (interface{}, error) {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO usage (key, size_bytes) VALUES ($1, $2)
			ON CONFLICT (key) DO UPDATE SET size_bytes=$2`,
			key, value.SizeBytes)
		if err != nil {
			return nil, err
		}
		return s.checkQuota(ctx, tx, key)
	})
	if err != nil {
		return err
	}
	if !ok.(bool) {
		return store.ErrQuotaExceeded
	}
	return nil
}

func (s *SQLStore) AddSizeBytes(ctx context.Context, key string, numBytes int64) error {
	ok, err := s.transact(ctx, func(tx *sql.Tx) (interface{}, error) {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO usage VALUES ($1, $2)
			ON CONFLICT (key) DO UPDATE SET size_bytes=usage.size_bytes+$2`,
			key, numBytes)
		if err != nil {
			return nil, err
		}
		return s.checkQuota(ctx, tx, key)
	})
	if err != nil {
		return err
	}
	if !ok.(bool) {
		return store.ErrQuotaExceeded
	}
	return nil
}

func (s *SQLStore) GetExceeded(ctx context.Context) ([]store.Record, error) {
	ret, err := s.transact(ctx, func(tx *sql.Tx) (interface{}, error) {
		rows, err := tx.QueryContext(ctx, `
			SELECT key, size_bytes, quota FROM (
				SELECT key, size_bytes, COALESCE(quota, $1) quota FROM usage
			) s WHERE size_bytes > quota`,
			s.DefaultQuotaBytes)
		if err != nil {
			return nil, fmt.Errorf("select keys over quota: %w", err)
		}
		var records []store.Record
		for rows.Next() {
			var r store.Record
			if err := rows.Scan(&r.Key, &r.Info.UsageBytes, &r.Info.QuotaBytes); err != nil {
				return nil, fmt.Errorf("parse result #%d: %w", len(records)+1, err)
			}
			records = append(records, r)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close query with #%d results: %w", len(records), err)
		}
		return records, nil
	})
	if err != nil {
		return nil, err
	}
	return ret.([]store.Record), nil
}
