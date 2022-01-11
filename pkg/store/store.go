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

// Info holds information about a key.
type Info struct {
	UsageBytes int64
	QuotaBytes int64
}

// Record associates a key with its Info
type Record struct {
	Key  string
	Info Info
}

// Store holds per-key usage and configured quota.
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
	// GetExceeded returns information about quota usage of all keys exceeding quota.
	GetExceeded(ctx context.Context) ([]Record, error)
}
