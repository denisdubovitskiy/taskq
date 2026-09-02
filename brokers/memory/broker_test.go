package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
)

func TestBroker_PublishAndConsume(t *testing.T) {
	t.Parallel()

	// Проверяем публикацию и получение задачи.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := NewBroker()
		job := &taskq.Job{ID: "job-1", Name: "test"}

		received := make(chan *taskq.Delivery, 1)
		consumeCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		go func() {
			_ = broker.Consume(consumeCtx, "default", &testDeliveryHandler{
				onHandle: func(ctx context.Context, delivery *taskq.Delivery) taskq.AckType {
					received <- delivery
					return taskq.AckAck
				},
			})
		}()

		// act
		err := broker.Publish(ctx, job)
		require.NoError(t, err)

		// assert
		select {
		case delivery := <-received:
			require.NotNil(t, delivery)
			assert.Equal(t, job.ID, delivery.ID)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for delivery")
		}
	})

	// Проверяем публикацию в разные очереди.
	t.Run("different queues", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := NewBroker()

		receivedDefault := make(chan *taskq.Delivery, 1)
		receivedCritical := make(chan *taskq.Delivery, 1)
		consumeCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		go func() {
			_ = broker.Consume(consumeCtx, "default", &testDeliveryHandler{
				onHandle: func(ctx context.Context, delivery *taskq.Delivery) taskq.AckType {
					receivedDefault <- delivery
					return taskq.AckAck
				},
			})
		}()

		go func() {
			_ = broker.Consume(consumeCtx, "critical", &testDeliveryHandler{
				onHandle: func(ctx context.Context, delivery *taskq.Delivery) taskq.AckType {
					receivedCritical <- delivery
					return taskq.AckAck
				},
			})
		}()

		// act
		err := broker.Publish(ctx, &taskq.Job{ID: "job-1", Queue: "default"})
		require.NoError(t, err)
		err = broker.Publish(ctx, &taskq.Job{ID: "job-2", Queue: "critical"})
		require.NoError(t, err)

		// assert
		select {
		case delivery := <-receivedDefault:
			assert.Equal(t, "job-1", delivery.ID)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for default delivery")
		}

		select {
		case delivery := <-receivedCritical:
			assert.Equal(t, "job-2", delivery.ID)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for critical delivery")
		}
	})
}

func TestBroker_Close(t *testing.T) {
	t.Parallel()

	// Проверяем, что после закрытия публикация возвращает ошибку.
	t.Run("prevents publish", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := NewBroker()
		err := broker.Close(ctx)
		require.NoError(t, err)

		// act
		err = broker.Publish(ctx, &taskq.Job{ID: "job-1"})

		// assert
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestBroker_ConcurrentPublish(t *testing.T) {
	t.Parallel()

	// Проверяем, что Publish и Consume не взаимоблокируются.
	t.Run("no deadlock", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := NewBroker()

		consumeCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		var receivedCount int
		var mu sync.Mutex

		go func() {
			_ = broker.Consume(consumeCtx, "default", &testDeliveryHandler{
				onHandle: func(ctx context.Context, delivery *taskq.Delivery) taskq.AckType {
					mu.Lock()
					receivedCount++
					mu.Unlock()
					return taskq.AckAck
				},
			})
		}()

		// act
		const count = 10
		for i := range count {
			err := broker.Publish(ctx, &taskq.Job{ID: "job-" + string(rune('0'+i))})
			require.NoError(t, err)
		}

		// assert
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return receivedCount == count
		}, 2*time.Second, 10*time.Millisecond)
	})
}

type testDeliveryHandler struct {
	onHandle func(context.Context, *taskq.Delivery) taskq.AckType
}

func (h *testDeliveryHandler) Handle(ctx context.Context, delivery *taskq.Delivery) taskq.AckType {
	return h.onHandle(ctx, delivery)
}
