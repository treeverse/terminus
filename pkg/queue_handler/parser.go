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

type ObjectPathAndSize struct {
	// Path is the complete S3 path to the object, "s3://...".
	Path string
	// SizeBytes is the number of bytes changed by this path.
	SizeBytes int64
}

var (
	ErrBadVersion   = errors.New("version incompatible with " + SupportedEventVersion)
	ErrNotAChange   = errors.New("not a change")
	ErrUnknownEvent = errors.New("unknown event name")
	ErrMissingField = errors.New("field missing")
)

func checkEventVersion(version string) error {
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
func ComputeDelta(r *S3EventRecord) (ObjectPathAndSize, error) {
	if err := checkEventVersion(r.EventVersion); err != nil {
		return ObjectPathAndSize{}, err
	}
	if r.EventName == EventTypeTest || strings.HasPrefix(r.EventName, EventTypeObjectRemovedPrefix) {
		return ObjectPathAndSize{}, ErrNotAChange
	}
	if !strings.HasPrefix(r.EventName, EventTypeObjectCreatedPrefix) {
		return ObjectPathAndSize{}, fmt.Errorf("%s: %w", r.EventName, ErrUnknownEvent)
	}

	bucket := r.S3.Bucket.Name
	if bucket == "" {
		return ObjectPathAndSize{}, fmt.Errorf("bucket.name %w", ErrMissingField)
	}
	key := r.S3.Object.Key
	if key == "" {
		return ObjectPathAndSize{}, fmt.Errorf("object.key %w", ErrMissingField)
	}
	if r.S3.Object.Size == nil {
		return ObjectPathAndSize{}, fmt.Errorf("object.size %w", ErrMissingField)
	}
	size := *r.S3.Object.Size

	return ObjectPathAndSize{
		Path:      fmt.Sprint("s3://" + bucket + "/" + key),
		SizeBytes: size,
	}, nil
}
