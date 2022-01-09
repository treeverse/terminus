// Package store provides a simple persistent atomic store for usage and
// quota.
package store

import (
	"context"
	"errors"
)

type Value struct {
	SizeBytes int64
}

var (
	ErrNotFound      = errors.New("not found")
	ErrQuotaExceeded = errors.New("quota exceeded")
)

type Store interface {
	// Get returns the value associated with key.
	Get(ctx context.Context, key string) (Value, error)
	// Set associates value with key and returns ErrQuotaExceeded if
	// that key exceeds quota.
	Set(ctx context.Context, key string, value Value) error
	// AddSizeBytes adds to the SizeBytes field of the Value associated
	// with key and returns ErrQuotaExceeded if that key exceeds quota.
	// It creates a new blank Value if needed.
	AddSizeBytes(ctx context.Context, key string, numBytes int64) error
}
