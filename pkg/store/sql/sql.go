// Package sql provides a store.Store that keeps data on a SQL database.
package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/treeverse/terminus/pkg/store"
)

func NewSQLStore(db *sql.DB) (store.Store, error) {
	return SQLStore{db: db}, nil
}

// SQLStore is a Store that keeps results in a SQL database.
type SQLStore struct {
	db *sql.DB
}

func (s SQLStore) transact(ctx context.Context, fn func(tx *sql.Tx) (interface{}, error)) (interface{}, error) {
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

func (s SQLStore) Get(ctx context.Context, key string) (store.Value, error) {
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

func (s SQLStore) Set(ctx context.Context, key string, value store.Value) error {
	_, err := s.transact(ctx, func(tx *sql.Tx) (interface{}, error) {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO usage (key, size_bytes) VALUES ($1, $2)
			ON CONFLICT (key) DO UPDATE SET size_bytes=$2`,
			key, value.SizeBytes)
		return nil, err
	})
	return err
}

func (s SQLStore) AddSizeBytes(ctx context.Context, key string, numBytes int64) error {
	_, err := s.transact(ctx, func(tx *sql.Tx) (interface{}, error) {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO usage VALUES ($1, $2)
			ON CONFLICT (key) DO UPDATE SET size_bytes=usage.size_bytes+$2`,
			key, numBytes)
		return nil, err
	})
	return err
}
