package taskq_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
	membackend "github.com/denisdubovitskiy/taskq/backends/memory"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
)

type addArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addResult struct {
	Sum int `json:"sum"`
}

func TestNewClient(t *testing.T) {
	t.Parallel()

	// Проверяем, что клиент создается с валидными зависимостями.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		// act
		client, err := taskq.NewClient(broker, backend)

		// assert
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	// Проверяем валидацию обязательных параметров.
	t.Run("broker required", func(t *testing.T) {
		t.Parallel()

		// arrange
		backend := membackend.NewBackend()

		// act
		client, err := taskq.NewClient(nil, backend)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "broker")
		assert.Nil(t, client)
	})

	t.Run("backend required", func(t *testing.T) {
		t.Parallel()

		// arrange
		broker := membroker.NewBroker()

		// act
		client, err := taskq.NewClient(broker, nil)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backend")
		assert.Nil(t, client)
	})
}

func TestNewWorker(t *testing.T) {
	t.Parallel()

	// Проверяем, что воркер создается с валидными зависимостями.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		// act
		worker, err := taskq.NewWorker(registry, broker, backend)

		// assert
		require.NoError(t, err)
		require.NotNil(t, worker)
	})

	// Проверяем валидацию обязательных параметров.
	t.Run("registry required", func(t *testing.T) {
		t.Parallel()

		// arrange
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		// act
		worker, err := taskq.NewWorker(nil, broker, backend)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "registry")
		assert.Nil(t, worker)
	})

	t.Run("broker required", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()
		backend := membackend.NewBackend()

		// act
		worker, err := taskq.NewWorker(registry, nil, backend)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "broker")
		assert.Nil(t, worker)
	})

	t.Run("backend required", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()
		broker := membroker.NewBroker()

		// act
		worker, err := taskq.NewWorker(registry, broker, nil)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backend")
		assert.Nil(t, worker)
	})
}

func TestTask(t *testing.T) {
	t.Parallel()

	// Проверяем создание задачи и привязку к очереди.
	t.Run("new task", func(t *testing.T) {
		t.Parallel()

		// act
		addTask := taskq.NewTask[addArgs, addResult]("add")

		// assert
		require.NotNil(t, addTask)
		assert.Equal(t, "add", addTask.Name)
		assert.Empty(t, addTask.Queue)
	})

	t.Run("with queue", func(t *testing.T) {
		t.Parallel()

		// arrange
		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		queued := addTask.WithQueue("critical")

		// assert
		require.NotNil(t, queued)
		assert.Equal(t, "add", queued.Name)
		assert.Equal(t, "critical", queued.Queue)

		// Исходная задача не изменяется.
		assert.Empty(t, addTask.Queue)
	})
}

func TestRegistry_Register(t *testing.T) {
	t.Parallel()

	// Проверяем успешную регистрацию обработчика.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		err := taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})

		// assert
		require.NoError(t, err)
	})

	// Проверяем валидацию параметров регистрации.
	t.Run("registry nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		err := taskq.Register(nil, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{}, nil
		})

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "registry")
	})

	t.Run("task nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()

		// act
		err := taskq.Register(registry, nil, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{}, nil
		})

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task")
	})

	t.Run("task name empty", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()
		emptyTask := taskq.NewTask[addArgs, addResult]("")

		// act
		err := taskq.Register(registry, emptyTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{}, nil
		})

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "name")
	})

	t.Run("handler nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		err := taskq.Register(registry, addTask, nil)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "handler")
	})

	// Проверяем защиту от дублирования регистрации.
	t.Run("already registered", func(t *testing.T) {
		t.Parallel()

		// arrange
		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		err := taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{}, nil
		})
		require.NoError(t, err)

		// assert
		err = taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{}, nil
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")
	})
}

func TestSubmit(t *testing.T) {
	t.Parallel()

	// Проверяем успешную отправку задачи и получение результата.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")
		err = taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		workerCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() {
			_ = worker.Run(workerCtx, "default")
		}()

		// act
		future, err := taskq.Submit(ctx, client, addTask, addArgs{A: 2, B: 3})

		// assert
		require.NoError(t, err)
		require.NotNil(t, future)

		result, err := future.GetWithTimeout(ctx, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, 5, result.Sum)
	})

	// Проверяем обработку ошибки в воркере.
	t.Run("handler returns error", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		registry := taskq.NewRegistry()
		failTask := taskq.NewTask[struct{}, struct{}]("fail")
		wantErr := errors.New("boom")
		err = taskq.Register(registry, failTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, wantErr
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		workerCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() {
			_ = worker.Run(workerCtx, "default")
		}()

		// act
		future, err := taskq.Submit(ctx, client, failTask, struct{}{})

		// assert
		require.NoError(t, err)

		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		require.Error(t, err)
		assert.Equal(t, wantErr.Error(), err.Error())
	})

	// Проверяем валидацию параметров Submit.
	t.Run("client nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		future, err := taskq.Submit[addArgs, addResult](ctx, nil, addTask, addArgs{A: 1, B: 2})

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "client")
		assert.Nil(t, future)
	})

	t.Run("task nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		// act
		future, err := taskq.Submit[addArgs, addResult](ctx, client, nil, addArgs{A: 1, B: 2})

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task")
		assert.Nil(t, future)
	})
}

func TestSubmitOneWay(t *testing.T) {
	t.Parallel()

	// Проверяем fire-and-forget отправку.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		registry := taskq.NewRegistry()
		logTask := taskq.NewTask[addArgs, struct{}]("log")
		err = taskq.Register(registry, logTask, func(ctx context.Context, args addArgs) (struct{}, error) {
			return struct{}{}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		workerCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() {
			_ = worker.Run(workerCtx, "default")
		}()

		// act
		err = taskq.SubmitOneWay(ctx, client, logTask, addArgs{A: 1, B: 2})

		// assert
		require.NoError(t, err)
	})
}

func TestFuture_Get(t *testing.T) {
	t.Parallel()

	// Проверяем таймаут ожидания результата.
	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		registry := taskq.NewRegistry()
		slowTask := taskq.NewTask[struct{}, struct{}]("slow")
		err = taskq.Register(registry, slowTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			select {
			case <-ctx.Done():
				return struct{}{}, ctx.Err()
			case <-time.After(10 * time.Second):
				return struct{}{}, nil
			}
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		workerCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() {
			_ = worker.Run(workerCtx, "default")
		}()

		future, err := taskq.Submit(ctx, client, slowTask, struct{}{})
		require.NoError(t, err)

		// act
		_, err = future.GetWithTimeout(ctx, 50*time.Millisecond)

		// assert
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestSubmitOptions(t *testing.T) {
	t.Parallel()

	// Проверяем установку заголовков.
	t.Run("with header", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("add")
		err = taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		headersCh := make(chan map[string]string, 1)
		worker, err := taskq.NewWorker(
			registry,
			broker,
			backend,
			taskq.WithPreExecuteHook(func(ctx context.Context, job *taskq.Job) error {
				headersCh <- job.Headers
				return nil
			}),
		)
		require.NoError(t, err)

		workerCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() {
			_ = worker.Run(workerCtx, "default")
		}()

		// act
		_, err = taskq.Submit(
			ctx,
			client,
			addTask,
			addArgs{A: 1, B: 2},
			taskq.WithHeader("x-trace-id", "abc123"),
		)
		require.NoError(t, err)

		// assert
		select {
		case capturedHeaders := <-headersCh:
			require.NotEmpty(t, capturedHeaders)
			assert.Equal(t, "abc123", capturedHeaders["x-trace-id"])
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for headers")
		}
	})

	// Проверяем, что WithDelay устанавливает ETA в будущем.
	t.Run("with delay", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		future, err := taskq.Submit(
			ctx,
			client,
			addTask,
			addArgs{A: 1, B: 2},
			taskq.WithDelay(5*time.Second),
		)

		// assert
		require.NoError(t, err)
		require.NotNil(t, future)

		result, state, err := future.Touch(ctx)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, taskq.StatePending, state.State)
		assert.Zero(t, result)
	})
}

func TestClientOptions(t *testing.T) {
	t.Parallel()

	// Проверяем, что пользовательский кодек используется для сериализации.
	t.Run("with codec", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()

		codec := &testCodec{contentType: "application/test"}
		client, err := taskq.NewClient(broker, backend, taskq.WithCodec(codec))
		require.NoError(t, err)

		addTask := taskq.NewTask[addArgs, addResult]("add")

		// act
		_, err = taskq.Submit(ctx, client, addTask, addArgs{A: 1, B: 2})

		// assert
		require.NoError(t, err)
		assert.True(t, codec.encodeCalled)
	})
}

type testCodec struct {
	contentType  string
	encodeCalled bool
}

func (c *testCodec) Encode(v any) ([]byte, error) {
	c.encodeCalled = true
	return []byte(`{}`), nil
}

func (c *testCodec) Decode(data []byte, v any) error {
	return nil
}

func (c *testCodec) ContentType() string {
	return c.contentType
}

func TestRetryPolicy(t *testing.T) {
	t.Parallel()

	// Проверяем, что задача повторяется указанное количество раз и переводится в dead-letter.
	t.Run("stops after max attempts", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		failTask := taskq.NewTask[struct{}, struct{}]("fail")
		var attempts atomic.Int32
		err := taskq.Register(registry, failTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			attempts.Add(1)
			return struct{}{}, errors.New("expected failure")
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		workerCtx, cancelWorker := context.WithCancel(ctx)
		defer cancelWorker()

		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- worker.Run(workerCtx, "default")
		}()

		time.Sleep(50 * time.Millisecond)

		future, err := taskq.Submit(
			ctx,
			client,
			failTask,
			struct{}{},
			taskq.WithRetry(taskq.RetryPolicy{
				MaxAttempts:  3,
				InitialDelay: 50 * time.Millisecond,
				MaxDelay:     200 * time.Millisecond,
				Multiplier:   2.0,
			}),
		)
		require.NoError(t, err)

		// act
		_, err = future.GetWithTimeout(ctx, 5*time.Second)

		// assert
		require.Error(t, err)
		assert.Equal(t, int32(3), attempts.Load())

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateDead, state.State)

		cancelWorker()
		select {
		case <-runErrCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for worker to stop")
		}
	})

	// Проверяем, что успешный retry возвращает результат.
	t.Run("succeeds on retry", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		flakyTask := taskq.NewTask[struct{}, int]("flaky")
		var attempts atomic.Int32
		err := taskq.Register(registry, flakyTask, func(ctx context.Context, _ struct{}) (int, error) {
			if attempts.Add(1) < 2 {
				return 0, errors.New("temporary failure")
			}
			return 42, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		workerCtx, cancelWorker := context.WithCancel(ctx)
		defer cancelWorker()

		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- worker.Run(workerCtx, "default")
		}()

		time.Sleep(50 * time.Millisecond)

		future, err := taskq.Submit(
			ctx,
			client,
			flakyTask,
			struct{}{},
			taskq.WithRetry(taskq.RetryPolicy{
				MaxAttempts:  3,
				InitialDelay: 50 * time.Millisecond,
				MaxDelay:     200 * time.Millisecond,
				Multiplier:   2.0,
			}),
		)
		require.NoError(t, err)

		// act
		result, err := future.GetWithTimeout(ctx, 5*time.Second)

		// assert
		require.NoError(t, err)
		assert.Equal(t, 42, result)
		assert.Equal(t, int32(2), attempts.Load())

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateSuccess, state.State)

		cancelWorker()
		select {
		case <-runErrCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for worker to stop")
		}
	})
}
