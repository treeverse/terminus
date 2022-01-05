// Package postgres provides a store.Store that keeps data on a Postgres
// database.
package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/treeverse/terminus/pkg/store"
)

func NewPostgresStore(driverName string, dsn string) (store.Store, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	return PostgresStore{conn: conn}, nil
}

// PostgresStore is a Store that keeps results in a Postgres database.
type PostgresStore struct {
	conn *sql.Conn
}

func (ps PostgresStore) Get(key string) (store.Value, error) {
	row := ps.conn.QueryRow()
}
