package postgresql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/stretchr/testify/require"
)

// addTask — тестовая задача сложения.
var addTask = taskq.NewTask[AddArgs, AddResult]("add")

// AddArgs — аргументы тестовой задачи.
type AddArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

// AddResult — результат тестовой задачи.
type AddResult struct {
	Sum int `json:"sum"`
}

// newTestComponents поднимает брокер и backend на чистой базе.
func newTestComponents(t *testing.T) (*Broker, *Backend) {
	t.Helper()

	pool := requireConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	broker, err := NewBroker(ctx, testDSN())
	require.NoError(t, err)
	backend, err := NewBackend(ctx, testDSN())
	require.NoError(t, err)
	truncate(t, pool)

	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = backend.Close(closeCtx)
		_ = broker.Close(closeCtx)
	})
	return broker, backend
}

// startWorker запускает воркер по очереди default и возвращает отмену.
func startWorker(t *testing.T, broker *Broker, backend *Backend, reg *taskq.Registry) {
	t.Helper()

	worker, err := taskq.NewWorker(reg, broker, backend)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = worker.Shutdown(context.Background())
	})

	go func() {
		_ = worker.Run(ctx, "")
	}()
}

// TestE2E_SubmitAndResult проверяет сквозной сценарий:
// Submit -> доставка из PostgreSQL -> исполнение -> Future.Get с результатом.
func TestE2E_SubmitAndResult(t *testing.T) {
	broker, backend := newTestComponents(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reg := taskq.NewRegistry()
	require.NoError(t, taskq.Register(reg, addTask, func(_ context.Context, a AddArgs) (AddResult, error) {
		return AddResult{Sum: a.A + a.B}, nil
	}))
	startWorker(t, broker, backend, reg)

	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)

	future, err := taskq.Submit[AddArgs, AddResult](ctx, client, addTask, AddArgs{A: 2, B: 3})
	require.NoError(t, err)

	res, err := future.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, 5, res.Sum)
}

// TestE2E_Retry проверяет retry-цикл под реальным брокером:
// два сбоя -> задержка (ETA) -> повтор -> успех.
func TestE2E_Retry(t *testing.T) {
	broker, backend := newTestComponents(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var attempts atomic.Int32
	flaky := taskq.NewTask[struct{}, AddResult]("flaky")

	reg := taskq.NewRegistry()
	require.NoError(t, taskq.Register(reg, flaky, func(_ context.Context, _ struct{}) (AddResult, error) {
		if attempts.Add(1) <= 2 {
			return AddResult{}, errors.New("временный сбой")
		}
		return AddResult{Sum: 42}, nil
	}))
	startWorker(t, broker, backend, reg)

	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)

	future, err := taskq.Submit[struct{}, AddResult](ctx, client, flaky, struct{}{},
		taskq.WithRetry(taskq.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     200 * time.Millisecond,
		}),
	)
	require.NoError(t, err)

	res, err := future.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, 42, res.Sum)
	require.Equal(t, int32(3), attempts.Load())
}

// TestE2E_Failure проверяет окончательный сбой: без retry-политики
// задача переходит в failure, и Future.Get возвращает ошибку.
func TestE2E_Failure(t *testing.T) {
	broker, backend := newTestComponents(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	failing := taskq.NewTask[struct{}, struct{}]("failing")
	reg := taskq.NewRegistry()
	require.NoError(t, taskq.Register(reg, failing, func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, errors.New("фатальный сбой")
	}))
	startWorker(t, broker, backend, reg)

	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)

	future, err := taskq.Submit[struct{}, struct{}](ctx, client, failing, struct{}{})
	require.NoError(t, err)

	_, err = future.Get(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "фатальный сбой")
}
