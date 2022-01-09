package queue_handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-test/deep"
	"regexp"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/treeverse/terminus/pkg/queue_handler"
	"github.com/treeverse/terminus/pkg/store"
)

type Store struct {
	mu sync.Mutex
	V  map[string]int64
}

func makeStore() *Store {
	ret := &Store{}
	ret.V = make(map[string]int64)
	return ret
}

func (s *Store) Get(_ context.Context, key string) (store.Value, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if size, ok := s.V[key]; !ok {
		return store.Value{}, fmt.Errorf("%s: %w", key, store.ErrNotFound)
	} else {
		return store.Value{SizeBytes: size}, nil
	}
}

func (s *Store) Set(_ context.Context, key string, value store.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.V[key] = value.SizeBytes
	return nil
}

func (s *Store) AddSizeBytes(_ context.Context, key string, numBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.V[key] += numBytes
	return nil
}

func (s *Store) Diff(expectedV map[string]int64) []string {
	s.mu.Lock()
	if len(expectedV) == 0 && len(s.V) == 0 {
		return nil
	}
	actualV := make(map[string]int64, len(s.V))
	for k, v := range s.V {
		actualV[k] = v
	}
	s.mu.Unlock()
	return deep.Equal(expectedV, actualV)
}

func ptr(s string) *string {
	return &s
}

type bucket struct {
	Name string `json:"name"`
}

type object struct {
	Key  string `json:"key"`
	Size *int64 `json:"size"`
}

type s3Body struct {
	Version string `json:"s3SchemaVersion"`
	Bucket  bucket `json:"bucket"`
	Object  object `json:"object"`
}

// event is structured representation of an S3 event record for Object*.
type event struct {
	Version string `json:"eventVersion"`
	Name    string `json:"eventName"`
	S3      s3Body `json:"s3"`
}

// makeEvent returns an event with some useful defaults.
func makeEvent() *event {
	return &event{
		Version: "2.1",
		S3: s3Body{
			Version: "1.0",
		},
	}
}

// WithVersion returns event with version set.
func (e *event) WithVersion(version string) *event {
	e.Version = version
	return e
}

// WithType returns event with *name* set.
func (e *event) WithType(name string) *event {
	e.Name = name
	return e
}

// WithBucket returns event with the bucket name set.
func (e *event) WithBucket(name string) *event {
	e.S3.Bucket.Name = name
	return e
}

// WithKey returns event with the object key name set.
func (e *event) WithKey(key string) *event {
	e.S3.Object.Key = key
	return e
}

// WithSize returns event with the object size set.
func (e *event) WithSize(size int64) *event {
	e.S3.Object.Size = &size
	return e
}

// makeMessage returns a message by JSONifying all the records.
func makeMessage(records ...interface{}) *sqs.Message {
	type body struct {
		Records []interface{} `json:"Records"`
	}
	jsonBody, err := json.Marshal(body{Records: records})
	if err != nil {
		panic(err)
	}
	return &sqs.Message{Body: ptr(string(jsonBody))}
}

// verifyError returns a function that returns an error that cannot be
// repeatedly Unwrapped to some error that errors.Is target.  The returned
// function correctly searches multierror.Error.
func verifyError(target error) func(err error) error {
	return func(err error) error {
		for curErr := errors.Unwrap(err); curErr != nil; curErr = errors.Unwrap(curErr) {
			if errors.Is(curErr, target) {
				return nil
			}
		}
		return fmt.Errorf("expecting error \"%s\"", target)
	}
}

func TestUpdateDB(t *testing.T) {
	cases := []struct {
		Name string
		In   *sqs.Message

		Out          map[string]int64
		ErrPredicate func(err error) error
	}{
		{
			Name: "SingleObjectAddedAndNotRemoved",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Put").WithBucket("bbb").WithKey("user/foo").WithSize(17),
				makeEvent().WithType("ObjectCreated:Put").WithBucket("bbb").WithKey("user/foo").WithSize(18),
				makeEvent().WithType("ObjectRemoved:Delete").WithBucket("bbb").WithKey("user/foo"),
			),
			Out: map[string]int64{"b:bbb u:user": 35},
		}, {
			Name: "MultipleObjectsAdded",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user/foo").WithSize(11),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user2/bar").WithSize(22),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user/baz").WithSize(33),
			),
			Out: map[string]int64{"b:a u:user": 44, "b:a u:user2": 22},
		}, {
			Name: "Hello",
			In:   makeMessage(makeEvent().WithType("TestEvent")),
		}, {
			Name:         "UnknownType",
			In:           makeMessage(makeEvent().WithType("NotARealType").WithKey("(ignored)")),
			ErrPredicate: verifyError(queue_handler.ErrUnknownEvent),
		}, {
			Name: "ObjectCreatedWithNoKey",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithSize(11),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user2/bar").WithSize(22),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user/baz").WithSize(33),
			),
			ErrPredicate: verifyError(queue_handler.ErrMissingField),
		}, {
			Name: "ObjectCreatedWithNoBucket",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Copy").WithKey("user/foo").WithSize(11),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user2/bar").WithSize(22),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user/baz").WithSize(33),
			),
			ErrPredicate: verifyError(queue_handler.ErrMissingField),
		}, {
			Name: "ObjectCreatedWithNoSize",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user/foo"),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user2/bar").WithSize(22),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user/baz").WithSize(33),
			),
			ErrPredicate: verifyError(queue_handler.ErrMissingField),
		}, {
			Name: "BadMajorMessageVersion",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Copy").WithVersion("9.0"),
			),
			ErrPredicate: verifyError(queue_handler.ErrBadVersion),
		}, {
			Name: "BadMinorMessageVersion",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Copy").WithVersion("1.1"),
			),
			ErrPredicate: verifyError(queue_handler.ErrBadVersion),
		}, {
			Name: "GoodMinorMessageVersion",
			In: makeMessage(
				makeEvent().WithType("TestEvent").WithVersion("2.0.99"),
			),
			ErrPredicate: verifyError(queue_handler.ErrBadVersion),
		}, {
			Name: "MultipleErrorsReported",
			In: makeMessage(
				makeEvent().WithType("ObjectCreated:Copy").WithKey("user/foo"),
				makeEvent().WithType("ObjectCreated:Copy").WithVersion("9.0"),
				makeEvent().WithType("ObjectCreated:Copy").WithBucket("a").WithKey("user/baz").WithSize(33),
			),
			ErrPredicate: func(err error) error {
				if vErr := verifyError(queue_handler.ErrMissingField)(err); vErr != nil {
					return vErr
				}
				return verifyError(queue_handler.ErrBadVersion)(err)
			},
		},
	}

	ctx := context.Background()

	keyPattern := regexp.MustCompile(`s3://(\w+)/(\w+)/.*`)
	keyReplace := `b:$1 u:$2`

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			s := makeStore()
			err := queue_handler.UpdateStore(ctx, tc.In, keyPattern, keyReplace, s)
			if tc.ErrPredicate != nil {
				testErr := tc.ErrPredicate(err)
				if testErr != nil {
					t.Errorf("got error %s: %s", err, testErr)
				}
			} else {
				if err != nil {
					t.Errorf("UpdateDB failed on %s: %s", *tc.In.Body, err)
				}
				if diffs := s.Diff(tc.Out); diffs != nil {
					t.Errorf("Unexpected values for %s: %v", *tc.In.Body, diffs)
				}
			}
		})
	}
}
