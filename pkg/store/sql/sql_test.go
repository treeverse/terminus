package sql_test

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"log"
	"os"
	"testing"
	"time"

	"github.com/treeverse/terminus/pkg/ddl"
	"github.com/treeverse/terminus/pkg/store"
	"github.com/treeverse/terminus/pkg/store/sql"

	"github.com/go-test/deep"
	"github.com/ory/dockertest/v3"
)

const (
	dbTestTimeout      = 5 * time.Second
	dbSetupTimeout     = 15 * time.Second
	dbContainerTimeout = 10 * time.Minute
	dbName             = "terminusdb"
)

var (
	pool        *dockertest.Pool
	databaseURI string
	db          *dbsql.DB
)

func runDBInstance(dockerPool *dockertest.Pool) (string, func()) {
	ctx, _ := context.WithTimeout(context.Background(), dbSetupTimeout)
	resource, err := dockerPool.Run("postgres", "11", []string{
		"POSTGRES_USER=terminus",
		"POSTGRES_PASSWORD=testing",
		fmt.Sprintf("POSTGRES_DB=%s", dbName),
	})
	if err != nil {
		log.Fatalf("Could not start postgresql: %s", err)
	}

	// set cleanup
	closer := func() {
		err := dockerPool.Purge(resource)
		if err != nil {
			log.Fatalf("Kill postgres container: %s", err)
		}
	}

	// expire, just to make sure
	err = resource.Expire(uint(dbContainerTimeout.Seconds() + 0.5))
	if err != nil {
		log.Fatalf("Expire postgres container: %s", err)
	}

	log.Printf("Port: %v", resource.GetPort("5432/tcp"))

	// create connection
	uri := fmt.Sprintf("postgres://terminus:testing@localhost:%s/"+dbName+"?sslmode=disable", resource.GetPort("5432/tcp"))
	err = dockerPool.Retry(func() error {
		var err error
		db, err = dbsql.Open("pgx", uri)
		if err != nil {
			fmt.Printf("Open: %s", err)
			return err
		}
		err = db.PingContext(ctx)
		if err != nil {
			return fmt.Errorf("Ping DB: %w", err)
		}
		log.Printf("DDL: %s", ddl.DDL)
		_, err = db.ExecContext(ctx, ddl.DDL)
		if err != nil {
			return fmt.Errorf("Create DB schema: %w", err)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("could not connect to postgres: %s", err)
	}

	// return DB URI
	return uri, closer
}

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	pool.MaxWait = dbSetupTimeout

	var cleanup func()
	databaseURI, cleanup = runDBInstance(pool)

	code := m.Run()
	cleanup()
	os.Exit(code)
}

func TestSetGet(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), dbSetupTimeout)
	s, err := sql.NewSQLStore(db)
	if err != nil {
		t.Fatalf("Open SQL store: %s", err)
	}

	key := "set:a"
	expected := store.Value{17}
	err = s.Set(ctx, key, expected)
	if err != nil {
		t.Errorf("Set %s: %s", key, err)
	}

	cases := []struct {
		Name     string
		Key      string
		Expected *store.Value
		Err      error
	}{
		{"Found key", key, &expected, nil},
		{"Missing key", key + "-missing", nil, store.ErrNotFound},
		{"Empty key", "", nil, store.ErrNotFound},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			actual, err := s.Get(ctx, c.Key)
			if !errors.Is(err, c.Err) {
				t.Errorf("Get %s: Expected error %s, got %s", key, c.Err, err)
			}
			if c.Expected != nil {
				if diffs := deep.Equal(c.Expected, &actual); diffs != nil {
					t.Errorf("Get %s: wrong values: %s", key, diffs)
				}
			}
		})
	}
}

func TestAddSizeBytes(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), dbSetupTimeout)
	s, err := sql.NewSQLStore(db)
	if err != nil {
		t.Fatalf("Open SQL store: %s", err)
	}

	keyInitialized, keyUsed := "set:a", "set:b"
	if err = s.Set(ctx, keyInitialized, store.Value{1}); err != nil {
		t.Fatalf("Set %s: %s", keyInitialized, err)
	}

	var value store.Value

	if err = s.AddSizeBytes(ctx, keyUsed, 2); err != nil {
		t.Errorf("AddSizeBytes %s: %s", keyUsed, err)
	}

	if value, err = s.Get(ctx, keyInitialized); err != nil {
		t.Errorf("Get %s: %s", keyInitialized, err)
	}
	if value.SizeBytes != 1 {
		t.Errorf("Get %s: Got %v expected 1", keyInitialized, value)
	}

	if value, err = s.Get(ctx, keyUsed); err != nil {
		t.Errorf("Get %s: %s", keyUsed, err)
	}
	if value.SizeBytes != 2 {
		t.Errorf("Get %s: Got %v expected 2", keyUsed, value)
	}

	if err = s.AddSizeBytes(ctx, keyInitialized, 3); err != nil {
		t.Errorf("AddSizeBytes %s: %s", keyInitialized, err)
	}

	if value, err = s.Get(ctx, keyInitialized); err != nil {
		t.Errorf("Get %s: %s", keyInitialized, err)
	}
	if value.SizeBytes != 4 {
		t.Errorf("Get %s: Got %v expected 4", keyUsed, value)
	}

	if err = s.AddSizeBytes(ctx, keyUsed, 4); err != nil {
		t.Errorf("AddSizeBytes %s: %s", keyUsed, err)
	}
}
