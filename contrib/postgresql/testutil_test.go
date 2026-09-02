package postgresql

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// testDSN возвращает DSN тестовой PostgreSQL:
// env TASKQ_TEST_PG_DSN, по умолчанию postgres://taskq:taskq@localhost:5432/taskq?sslmode=disable.
func testDSN() string {
	if dsn := os.Getenv("TASKQ_TEST_PG_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://taskq:taskq@localhost:5432/taskq?sslmode=disable"
}

// requireConn подключается к тестовой PostgreSQL и пропускает тест,
// если сервер недоступен (интеграционные тесты требуют docker compose).
func requireConn(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), testDSN())
	if err != nil {
		t.Skipf("не удалось разобрать DSN %s: %v", testDSN(), err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("тестовая PostgreSQL недоступна (docker compose up -d): %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// testTables — таблицы taskq (в порядке удаления).
var testTables = []string{"tq_jobs", "tq_job_states", "tq_job_results", "tq_locks"}

// truncate очищает все таблицы taskq.
func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx := context.Background()
	for _, table := range testTables {
		if _, err := pool.Exec(ctx, "TRUNCATE public."+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}

// freshBroker создает брокер на чистой базе.
func freshBroker(t *testing.T) *Broker {
	t.Helper()

	pool := requireConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	broker, err := NewBroker(ctx, testDSN())
	require.NoError(t, err)
	truncate(t, pool)

	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = broker.Close(closeCtx)
	})
	return broker
}

// freshBackend создает backend на чистой базе.
func freshBackend(t *testing.T) *Backend {
	t.Helper()

	pool := requireConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	backend, err := NewBackend(ctx, testDSN())
	require.NoError(t, err)
	truncate(t, pool)

	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = backend.Close(closeCtx)
	})
	return backend
}

// freshLocker создает locker на чистой базе.
func freshLocker(t *testing.T) *Locker {
	t.Helper()

	pool := requireConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	locker, err := NewLocker(ctx, testDSN())
	require.NoError(t, err)
	truncate(t, pool)

	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = locker.Close(closeCtx)
	})
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

// expectAnyDelivery ждет любую доставку.
func expectAnyDelivery(t *testing.T, ch <-chan *taskq.Delivery) *taskq.Delivery {
	t.Helper()

	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatalf("доставка не получена")
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
