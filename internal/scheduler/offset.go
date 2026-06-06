package scheduler

import (
	"context"
	"time"
)

// waitOffset sleeps for the scheduler's start-up phase offset before its first
// tick. Staggering the offsets across schedulers spreads their upstream
// requests over the polling window instead of bursting them together on every
// shared interval boundary (e.g. all the 5s schedulers firing at :00, :05,
// :10). It returns false if ctx is cancelled while waiting.
func waitOffset(ctx context.Context, offset time.Duration) bool {
	if offset <= 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(offset):
		return true
	}
}
