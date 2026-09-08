package main

import (
	"context"
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// storageCloseFunc adapts a func to the Close() method set ownedResources
// requires, so shutdown order can be verified with fakes.
type storageCloseFunc func()

func (f storageCloseFunc) Close() { f() }

// BASE-008: shutdown must stop inflow (messenger) and then release
// storage exactly once, even when invoked twice. messenger.Close on a
// real context is not safe to call twice, hence the guard under test.
func TestShutdownIsOrderedAndIdempotent(t *testing.T) {
	is := is.New(t)

	var order []string
	storageCloses := 0
	messenger := &messaging.MsgContextMock{
		CloseFunc: func() { order = append(order, "messenger") },
	}

	owned := &ownedResources{
		messenger: messenger,
		storage: storageCloseFunc(func() {
			storageCloses++
			order = append(order, "storage")
		}),
	}

	ctx := context.Background()
	owned.close(ctx)
	owned.close(ctx)

	is.Equal(storageCloses, 1)
	is.Equal(order, []string{"messenger", "storage"})
}

// BASE-008: shutdown with no initialized resources (e.g. failed OnInit)
// must be a safe no-op.
func TestShutdownWithoutResourcesIsSafe(t *testing.T) {
	owned := &ownedResources{}

	owned.close(context.Background())
	owned.close(context.Background())
}
