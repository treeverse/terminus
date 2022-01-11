package sql_test

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"log"
	"os"
	"sort"
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
	pool *dockertest.Pool
	db   *dbsql.DB
)

func runDBInstance() (string, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), dbSetupTimeout)
	defer cancel()
	resource, err := pool.Run("postgres", "11", []string{
		"POSTGRES_USER=terminus",
		"POSTGRES_PASSWORD=testing",
		fmt.Sprintf("POSTGRES_DB=%s", dbName),
	})
	if err != nil {
		log.Fatalf("Could not start postgresql: %s", err)
	}

	// set cleanup
	closer := func() {
		err := pool.Purge(resource)
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
	err = pool.Retry(func() error {
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
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	pool.MaxWait = dbSetupTimeout

	code := m.Run()
	os.Exit(code)
}

func value(sizeBytes int64) store.Value {
	return store.Value{SizeBytes: sizeBytes}
}

const defaultQuota = 50

func TestSet(t *testing.T) {
	_, cleanup := runDBInstance()
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), dbTestTimeout)
	defer cancel()
	s, err := sql.NewSQLStore(db, defaultQuota)
	if err != nil {
		t.Fatalf("Open SQL store: %s", err)
	}

	key := "set:a"
	cases := []struct {
		Name      string
		Key       string
		SizeBytes int64
		Err       error
	}{
		{"Zero", key, 0, nil},
		{"OK", key, defaultQuota * 3 / 4, nil},
		{"Exceeded", key, defaultQuota + 1, store.ErrQuotaExceeded},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			err := s.Set(ctx, c.Key, value(c.SizeBytes))
			if !errors.Is(err, c.Err) {
				t.Errorf("Set %s: Expected error %s, got %s", key, c.Err, err)
			}
		})
	}
}

func TestSetGet(t *testing.T) {
	_, cleanup := runDBInstance()
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), dbTestTimeout)
	defer cancel()
	s, err := sql.NewSQLStore(db, defaultQuota)
	if err != nil {
		t.Fatalf("Open SQL store: %s", err)
	}

	key := "set:a"
	expected := value(17)
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
	_, cleanup := runDBInstance()
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), dbTestTimeout)
	defer cancel()
	s, err := sql.NewSQLStore(db, defaultQuota)
	if err != nil {
		t.Fatalf("Open SQL store: %s", err)
	}

	keyInitialized, keyUsed := "add:initialized", "add:used"
	if err = s.Set(ctx, keyInitialized, value(1)); err != nil {
		t.Fatalf("Set %s: %s", keyInitialized, err)
	}

	var value store.Value

	// Not table-driven cases -- the sequence is important here to keep
	// developing the state.

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

	if err = s.AddSizeBytes(ctx, keyUsed, defaultQuota); !errors.Is(err, store.ErrQuotaExceeded) {
		t.Errorf("AddSizeBytes %s: expected quota exceeded, got %s", keyUsed, err)
	}
}

// ByKey is a sort.Interface for sorting store.Record by keys.
type ByKey []store.Record

func (s ByKey) Len() int           { return len(s) }
func (s ByKey) Less(i, j int) bool { return s[i].Key < s[j].Key }
func (s ByKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func TestExceeded(t *testing.T) {
	_, cleanup := runDBInstance()
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), dbTestTimeout)
	defer cancel()
	s, err := sql.NewSQLStore(db, defaultQuota)
	if err != nil {
		t.Fatalf("Open SQL store: %s", err)
	}

	const (
		keyOKDefault    = "exceeded: ok under default"
		keyOKSpecific   = "exceeded: ok under specific quota"
		keyOverDefault  = "exceeded: over default"
		keyOverSpecific = "exceeded: over specific quota"

		specificQuota = defaultQuota + 20
	)

	bytes := []struct {
		Key   string
		Bytes int64
	}{
		{keyOKDefault, defaultQuota},
		{keyOKSpecific, defaultQuota + 5},
		{keyOverDefault, defaultQuota + 10},
		{keyOverSpecific, specificQuota + 15},
	}

	// setup quotas
	if _, err := db.Exec(
		`INSERT INTO usage (key, size_bytes, quota) VALUES ($1, 0, $3), ($2, 0, $3)`,
		keyOKSpecific, keyOverSpecific, specificQuota); err != nil {
		t.Fatalf("Set specific quotas for %s, %s: %s", keyOKSpecific, keyOverSpecific, err)
	}

	// setup values, ignoring over-quota messages from Set.
	for _, b := range bytes {
		if err = s.Set(ctx, b.Key, value(b.Bytes)); err != nil && !errors.Is(err, store.ErrQuotaExceeded) {
			t.Fatalf("Set %s to %d: %s", b.Key, b.Bytes, err)
		}
	}

	expected := []store.Record{
		{Key: keyOverDefault, Info: store.Info{UsageBytes: defaultQuota + 10, QuotaBytes: defaultQuota}},
		{Key: keyOverSpecific, Info: store.Info{UsageBytes: specificQuota + 15, QuotaBytes: specificQuota}},
	}
	sort.Sort(ByKey(expected))

	exceeded, err := s.GetExceeded(ctx)
	if err != nil {
		t.Fatalf("Get quota exceeded: %s", err)
	}

	sort.Sort(ByKey(exceeded))

	if diffs := deep.Equal(exceeded, expected); diffs != nil {
		t.Error("Unexpected results for GetExceeded ", diffs)
		t.Log("Got:", exceeded)
		t.Log("Expected:", expected)
	}
}
