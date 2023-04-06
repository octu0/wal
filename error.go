package wal

import (
	"github.com/pkg/errors"
)

var (
	ErrClosed         = errors.New("closed")
	ErrCompactRunning = errors.New("compat already in progress")
	ErrNotFound       = errors.New("not found")
	ErrSegmentNotOpen = errors.New("segment not open")
)
