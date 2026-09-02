package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/stretchr/testify/require"
)

// TestBroker_PublishConsume проверяет базовый сценарий:
// публикация -> доставка с исходным телом и заголовками -> Ack.
func TestBroker_PublishConsume(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	handler := newCaptureHandler(1)
	startConsume(t, broker, "default", handler)

	job := testJob("job-1", "default")
	job.Headers = map[string]string{"trace": "abc"}
	require.NoError(t, broker.Publish(ctx, job))

	d := expectJob(t, handler.deliveries, "job-1")
	require.Equal(t, map[string]string{"trace": "abc"}, d.Headers)
	require.NoError(t, d.Ack(ctx))
}

// TestBroker_NackRequeue проверяет, что Nack(requeue) возвращает
// сообщение в очередь и оно доставляется повторно.
func TestBroker_NackRequeue(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	handler := newCaptureHandler(2)
	startConsume(t, broker, "default", handler)

	require.NoError(t, broker.Publish(ctx, testJob("job-1", "default")))

	d1 := expectJob(t, handler.deliveries, "job-1")
	require.NoError(t, d1.Nack(ctx, true))

	d2 := expectJob(t, handler.deliveries, "job-1")
	require.NoError(t, d2.Ack(ctx))
}

// TestBroker_NackDrop проверяет, что Nack(drop) отбрасывает сообщение
// без повторной доставки.
func TestBroker_NackDrop(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	handler := newCaptureHandler(1)
	startConsume(t, broker, "default", handler)

	require.NoError(t, broker.Publish(ctx, testJob("job-1", "default")))

	d := expectJob(t, handler.deliveries, "job-1")
	require.NoError(t, d.Nack(ctx, false))

	expectNoDelivery(t, handler.deliveries, 500*time.Millisecond)
}

// TestBroker_DelayedDelivery проверяет, что задача с будущим ETA
// не доставляется раньше времени и доставляется после наступления задержки.
func TestBroker_DelayedDelivery(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	handler := newCaptureHandler(1)
	startConsume(t, broker, "default", handler)

	job := testJob("job-1", "default")
	eta := time.Now().Add(300 * time.Millisecond)
	job.ETA = &eta
	require.NoError(t, broker.Publish(ctx, job))

	// До наступления ETA доставки не должно быть.
	expectNoDelivery(t, handler.deliveries, 150*time.Millisecond)

	d := expectJob(t, handler.deliveries, "job-1")
	require.NoError(t, d.Ack(ctx))
}

// TestBroker_QueueIsolation проверяет, что очереди независимы:
// потребитель одной очереди не видит задачи другой.
func TestBroker_QueueIsolation(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	handlerA := newCaptureHandler(1)
	handlerB := newCaptureHandler(1)
	startConsume(t, broker, "a", handlerA)
	startConsume(t, broker, "b", handlerB)

	require.NoError(t, broker.Publish(ctx, testJob("job-a", "a")))
	require.NoError(t, broker.Publish(ctx, testJob("job-b", "b")))

	da := expectJob(t, handlerA.deliveries, "job-a")
	db := expectJob(t, handlerB.deliveries, "job-b")
	require.NoError(t, da.Ack(ctx))
	require.NoError(t, db.Ack(ctx))
}

// TestBroker_MultipleJobs проверяет доставку и подтверждение партии задач.
func TestBroker_MultipleJobs(t *testing.T) {
	const n = 50

	broker := freshBroker(t)
	ctx := context.Background()

	handler := newCaptureHandler(n)
	startConsume(t, broker, "default", handler)

	for i := 0; i < n; i++ {
		id := "job-" + strconv.Itoa(i)
		require.NoError(t, broker.Publish(ctx, testJob(id, "default")))
	}

	for i := 0; i < n; i++ {
		d := expectJob(t, handler.deliveries, "job-"+strconv.Itoa(i))
		require.NoError(t, d.Ack(ctx))
	}
}

// TestBroker_RepublishSameJob имитирует retry ядра: повторная публикация
// задачи с тем же ID доставляется как новое сообщение.
func TestBroker_RepublishSameJob(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	handler := newCaptureHandler(2)
	startConsume(t, broker, "default", handler)

	job := testJob("job-1", "default")
	require.NoError(t, broker.Publish(ctx, job))

	d1 := expectJob(t, handler.deliveries, "job-1")
	require.NoError(t, d1.Ack(ctx))

	job.Attempt = 1
	job.State = taskq.StateRetry
	eta := time.Now().Add(50 * time.Millisecond)
	job.ETA = &eta
	require.NoError(t, broker.Publish(ctx, job))

	d2 := expectJob(t, handler.deliveries, "job-1")
	var retryJob taskq.Job
	require.NoError(t, json.Unmarshal(d2.Body, &retryJob))
	require.Equal(t, uint32(1), retryJob.Attempt)
	require.NoError(t, d2.Ack(ctx))
}

// TestBroker_PublishAfterClose проверяет, что публикация после Close
// возвращается ошибкой.
func TestBroker_PublishAfterClose(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Close(ctx))
	require.ErrorIs(t, broker.Publish(ctx, testJob("job-1", "default")), errBrokerClosed)
}

// TestBroker_ConsumeAfterClose проверяет, что Consume после Close
// возвращается ошибкой.
func TestBroker_ConsumeAfterClose(t *testing.T) {
	broker := freshBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Close(ctx))

	handler := newCaptureHandler(1)
	err := broker.Consume(ctx, "default", handler)
	require.ErrorIs(t, err, errBrokerClosed)
}
