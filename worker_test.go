package taskq_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
	membackend "github.com/denisdubovitskiy/taskq/backends/memory"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
)

func TestWorker_Handle(t *testing.T) {
	t.Parallel()

	// Проверяем успешную обработку задачи.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")

		err := taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(ctx, client, addTask, addArgs{A: 2, B: 3})
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)

		result, err := future.GetWithTimeout(ctx, 2*time.Second)
		require.NoError(t, err)
		assert.Equal(t, 5, result.Sum)
	})

	// Проверяем поведение при отсутствии обработчика.
	t.Run("handler not found", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		unknownTask := taskq.NewTask[addArgs, addResult]("unknown")
		future, err := taskq.Submit(ctx, client, unknownTask, addArgs{A: 1, B: 2})
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)

		_, err = future.GetWithTimeout(ctx, 2*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "handler not found")
	})
}

func TestWorker_Hooks(t *testing.T) {
	t.Parallel()

	// Проверяем pre-execute hook.
	t.Run("pre execute hook", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")

		err := taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		var capturedJobName string
		var mu sync.Mutex

		worker, err := taskq.NewWorker(
			registry,
			broker,
			backend,
			taskq.WithPreExecuteHook(func(ctx context.Context, job *taskq.Job) error {
				mu.Lock()
				capturedJobName = job.Name
				mu.Unlock()
				return nil
			}),
		)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		_, err = taskq.Submit(ctx, client, addTask, addArgs{A: 1, B: 2})
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)
		mu.Lock()
		assert.Equal(t, "add", capturedJobName)
		mu.Unlock()
	})

	// Проверяем post-execute hook при успехе.
	t.Run("post execute hook success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")

		err := taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		stateCh := make(chan taskq.State, 1)
		worker, err := taskq.NewWorker(
			registry,
			broker,
			backend,
			taskq.WithPostExecuteHook(func(ctx context.Context, job *taskq.Job, state taskq.State, err error) {
				stateCh <- state
			}),
		)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		_, err = taskq.Submit(ctx, client, addTask, addArgs{A: 1, B: 2})
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)
		select {
		case state := <-stateCh:
			assert.Equal(t, taskq.StateSuccess, state)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for post execute hook")
		}
	})

	// Проверяем post-execute hook при ошибке.
	t.Run("post execute hook failure", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()
		failTask := taskq.NewTask[struct{}, struct{}]("fail")
		wantErr := errors.New("boom")

		err := taskq.Register(registry, failTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, wantErr
		})
		require.NoError(t, err)

		stateCh := make(chan taskq.State, 1)
		errCh := make(chan error, 1)
		worker, err := taskq.NewWorker(
			registry,
			broker,
			backend,
			taskq.WithPostExecuteHook(func(ctx context.Context, job *taskq.Job, state taskq.State, err error) {
				stateCh <- state
				errCh <- err
			}),
		)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		_, err = taskq.Submit(ctx, client, failTask, struct{}{})
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)
		select {
		case state := <-stateCh:
			assert.Equal(t, taskq.StateFailure, state)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for post execute hook")
		}
		select {
		case err := <-errCh:
			assert.Equal(t, wantErr, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for error")
		}
	})
}

func TestWorker_ErrorHandler(t *testing.T) {
	t.Parallel()

	// Проверяем вызов обработчика ошибок.
	t.Run("called on failure", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()
		failTask := taskq.NewTask[struct{}, struct{}]("fail")
		wantErr := errors.New("boom")

		err := taskq.Register(registry, failTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, wantErr
		})
		require.NoError(t, err)

		errCh := make(chan error, 1)
		worker, err := taskq.NewWorker(
			registry,
			broker,
			backend,
			taskq.WithErrorHandler(func(ctx context.Context, job *taskq.Job, err error) error {
				errCh <- err
				return nil
			}),
		)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		_, err = taskq.Submit(ctx, client, failTask, struct{}{})
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)
		select {
		case err := <-errCh:
			assert.Equal(t, wantErr, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for error handler")
		}
	})
}

func TestWorker_Concurrency(t *testing.T) {
	t.Parallel()

	// Проверяем, что concurrency не может быть меньше 1.
	t.Run("minimum concurrency", func(t *testing.T) {
		t.Parallel()

		// arrange
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		// act
		worker, err := taskq.NewWorker(registry, broker, backend, taskq.WithConcurrency(0))

		// assert
		require.NoError(t, err)
		require.NotNil(t, worker)
	})
}

// runWorkerOnce запускает worker на одну задачу и возвращает ack.
func runWorkerOnce(ctx context.Context, t *testing.T, worker *taskq.Worker, broker *membroker.Broker, queue string) taskq.AckType {
	t.Helper()

	ackCh := make(chan taskq.AckType, 1)
	consumeCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() {
		err := broker.Consume(consumeCtx, queue, &singleHandler{
			worker: worker,
			ackCh:  ackCh,
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("consume: %v", err)
		}
	}()

	select {
	case ack := <-ackCh:
		cancel()
		return ack
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for ack")
		return -1
	}
}

type singleHandler struct {
	worker *taskq.Worker
	ackCh  chan<- taskq.AckType
}

func (h *singleHandler) Handle(ctx context.Context, delivery *taskq.Delivery) taskq.AckType {
	ack := h.worker.Handle(ctx, delivery)
	h.ackCh <- ack
	return ack
}

func TestWorker_Stop(t *testing.T) {
	t.Parallel()

	// Проверяем, что Stop завершает Run и дожидается активных задач.
	t.Run("stops gracefully", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- worker.Run(ctx, "default")
		}()

		// Даем worker возможность начать consume.
		time.Sleep(50 * time.Millisecond)

		// act
		err = worker.Stop()

		// assert
		require.NoError(t, err)

		select {
		case runErr := <-runErrCh:
			require.Error(t, runErr)
			assert.ErrorIs(t, runErr, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Run to return")
		}
	})
}

func TestWorker_Shutdown(t *testing.T) {
	t.Parallel()

	// Проверяем graceful shutdown с таймаутом.
	t.Run("waits for active tasks", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		slowTask := taskq.NewTask[struct{}, struct{}]("slow")
		err := taskq.Register(registry, slowTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			select {
			case <-ctx.Done():
				return struct{}{}, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return struct{}{}, nil
			}
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- worker.Run(ctx, "default")
		}()

		time.Sleep(50 * time.Millisecond)

		_, err = taskq.Submit(ctx, client, slowTask, struct{}{})
		require.NoError(t, err)

		// act
		shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		err = worker.Shutdown(shutdownCtx)

		// assert
		require.NoError(t, err)

		select {
		case runErr := <-runErrCh:
			require.Error(t, runErr)
			assert.ErrorIs(t, runErr, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Run to return")
		}
	})

	// Проверяем, что Shutdown возвращает ошибку по таймауту.
	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		startedCh := make(chan struct{})
		blockCh := make(chan struct{})
		slowTask := taskq.NewTask[struct{}, struct{}]("slow")
		err := taskq.Register(registry, slowTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			close(startedCh)
			<-blockCh
			return struct{}{}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- worker.Run(ctx, "default")
		}()

		time.Sleep(50 * time.Millisecond)

		_, err = taskq.Submit(ctx, client, slowTask, struct{}{})
		require.NoError(t, err)

		// Ждем, пока handler действительно начнет выполняться.
		select {
		case <-startedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for handler to start")
		}

		// act
		shutdownCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err = worker.Shutdown(shutdownCtx)

		// assert
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		// Очищаем: разблокируем handler, чтобы worker завершился.
		close(blockCh)
		select {
		case <-runErrCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Run to return after cleanup")
		}
	})
}

func TestWorker_Close(t *testing.T) {
	t.Parallel()

	// Проверяем, что Close выполняет graceful shutdown и закрывает брокер.
	t.Run("closes gracefully", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- worker.Run(ctx, "default")
		}()

		time.Sleep(50 * time.Millisecond)

		// act
		closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		err = worker.Close(closeCtx)

		// assert
		require.NoError(t, err)

		select {
		case runErr := <-runErrCh:
			require.Error(t, runErr)
			assert.ErrorIs(t, runErr, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Run to return")
		}

		// Брокер закрыт — публикация должна вернуть ошибку.
		assert.ErrorIs(t, broker.Publish(ctx, &taskq.Job{ID: "job-1"}), context.Canceled)
	})
}
