package redis

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// testAddr возвращает адрес тестового Redis:
// env TASKQ_TEST_REDIS_ADDR, по умолчанию localhost:6379.
func testAddr() string {
	if addr := os.Getenv("TASKQ_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6380"
}

// requireRedis подключается к тестовому Redis и пропускает тест,
// если сервер недоступен (интеграционные тесты требуют docker compose).
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: testAddr()})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("тестовый Redis недоступен на %s (docker compose up -d): %v", testAddr(), err)
	}
	return client
}

// freshBroker создает брокер на чистом Redis (FLUSHALL).
func freshBroker(t *testing.T) *Broker {
	t.Helper()

	client := requireRedis(t)
	require.NoError(t, client.FlushAll(context.Background()).Err())

	broker, err := NewBroker(client)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = broker.Close(ctx)
	})
	return broker
}

// freshBackend создает backend на чистом Redis (FLUSHALL).
func freshBackend(t *testing.T, opts ...Option) *Backend {
	t.Helper()

	client := requireRedis(t)
	require.NoError(t, client.FlushAll(context.Background()).Err())

	backend, err := NewBackend(client, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = backend.Close(ctx)
	})
	return backend
}

// freshLocker создает locker на чистом Redis (FLUSHALL).
func freshLocker(t *testing.T) *Locker {
	t.Helper()

	client := requireRedis(t)
	require.NoError(t, client.FlushAll(context.Background()).Err())

	locker, err := NewLocker(client)
	require.NoError(t, err)
	return locker
}

// testJob строит тестовую задачу.
func testJob(id, queue string) *taskq.Job {
	return &taskq.Job{
		ID:      id,
		Name:    "test",
		Queue:   queue,
		Payload: []byte(`{"n":1}`),
		State:   taskq.StatePending,
	}
}

// captureHandler кладет каждую доставку в канал. Подтверждение (Ack/Nack)
// выполняет тест — так же, как это делает реальный воркер.
type captureHandler struct {
	deliveries chan *taskq.Delivery
}

// Handle реализует taskq.DeliveryHandler.
func (h captureHandler) Handle(ctx context.Context, d *taskq.Delivery) taskq.AckType {
	h.deliveries <- d
	return taskq.AckAck
}

// newCaptureHandler создает перехватчик на n ожидаемых доставок.
func newCaptureHandler(n int) captureHandler {
	return captureHandler{deliveries: make(chan *taskq.Delivery, n)}
}

// expectJob ждет доставку и проверяет, что тело — исходная задача.
func expectJob(t *testing.T, ch <-chan *taskq.Delivery, wantID string) *taskq.Delivery {
	t.Helper()

	select {
	case d := <-ch:
		var job taskq.Job
		require.NoError(t, json.Unmarshal(d.Body, &job))
		require.Equal(t, wantID, job.ID)
		require.Equal(t, wantID, d.ID)
		return d
	case <-time.After(5 * time.Second):
		t.Fatalf("доставка задачи %s не получена", wantID)
		return nil
	}
}

// expectNoDelivery проверяет, что за d новых доставок не было.
func expectNoDelivery(t *testing.T, ch <-chan *taskq.Delivery, d time.Duration) {
	t.Helper()

	select {
	case got := <-ch:
		t.Fatalf("неожиданная доставка: id=%s", got.ID)
	case <-time.After(d):
	}
}

// startConsume запускает Consume в горутине; отмена происходит в t.Cleanup.
func startConsume(t *testing.T, broker *Broker, queue string, handler taskq.DeliveryHandler) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = broker.Consume(ctx, queue, handler)
	}()
}
