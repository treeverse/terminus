package queue_handler

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

const (
	SupportedEventVersion      = "v2.1"
	SupportedEventMajorVersion = "v2"

	EventTypeTest                = "s3:TestEvent"
	EventTypeObjectCreatedPrefix = "s3:ObjectCreated:"
	EventTypeObjectRemovedPrefix = "s3:ObjectRemoved:"
)

// S3EventRecord is the Go-ish version of the JSON object sent as an S3 event.
type S3EventRecord struct {
	EventVersion string    `json:"eventVersion"`
	EventTime    time.Time `json:"eventTime"`
	EventName    string    `json:"eventName"`
	S3           struct {
		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
		Object struct {
			Key       string `json:"key"`
			Size      *int64 `json:"size"`
			ETag      string `json:"eTag"`
			Sequencer string `json:"sequencer"`
		} `json:"object"`
	} `json:"s3"`
}

type S3Events struct {
	Records []S3EventRecord `json:"records"`
}

type ObjectPathAndDelta struct {
	// Path is the complete S3 path to the object, "s3://...".
	Path string
	// DeltaBytes is the number of bytes changed by this path.
	DeltaBytes int64
}

var (
	ErrBadVersion   = errors.New("version incompatible with " + SupportedEventVersion)
	ErrNotAChange   = errors.New("not a change")
	ErrUnknownEvent = errors.New("unknown event name")
	ErrMissingField = errors.New("field missing")
)

func checkVersion(version string) error {
	if version == SupportedEventVersion {
		return nil
	}
	if !strings.HasPrefix(version, "v") {
		// AWS version strings don't start with "v", Go semver strings do...
		version = "v" + version
	}
	major := semver.Major(version)
	if major != SupportedEventMajorVersion || semver.Compare(version, SupportedEventVersion) < 0 {
		return fmt.Errorf("%s: %w", version, ErrBadVersion)
	}
	return nil
}

// ComputeDelta extracts ObjectPathAndDelta from an S3EventRecord.  It
// returns ErrNotAChange if there are no delta bytes to extract, or
// ErrUnknownEvent if it could not even recognize the event type.
func ComputeDelta(r *S3EventRecord) (ObjectPathAndDelta, error) {
	if err := checkVersion(r.EventVersion); err != nil {
		return ObjectPathAndDelta{}, err
	}
	if r.EventName == EventTypeTest || strings.HasPrefix(r.EventName, EventTypeObjectRemovedPrefix) {
		return ObjectPathAndDelta{}, ErrNotAChange
	}
	if !strings.HasPrefix(r.EventName, EventTypeObjectCreatedPrefix) {
		return ObjectPathAndDelta{}, fmt.Errorf("%s: %w", r.EventName, ErrUnknownEvent)
	}

	bucket := r.S3.Bucket.Name
	if bucket == "" {
		return ObjectPathAndDelta{}, fmt.Errorf("bucket.name %w", ErrMissingField)
	}
	key := r.S3.Object.Key
	if key == "" {
		return ObjectPathAndDelta{}, fmt.Errorf("object.key %w", ErrMissingField)
	}
	if r.S3.Object.Size == nil {
		return ObjectPathAndDelta{}, fmt.Errorf("object.size %w", ErrMissingField)
	}
	size := *r.S3.Object.Size

	return ObjectPathAndDelta{
		Path:       fmt.Sprint("s3://" + bucket + "/" + key),
		DeltaBytes: size,
	}, nil
}
