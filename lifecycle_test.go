package taskq_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	filebackend "github.com/denisdubovitskiy/taskq/backends/file"
	membackend "github.com/denisdubovitskiy/taskq/backends/memory"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskTimeout(t *testing.T) {
	t.Parallel()

	// Таймаут без ретраев: задача падает с ошибкой таймаута.
	t.Run("timeout without retries fails the job", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("sleepy")
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			select {
			case <-time.After(500 * time.Millisecond):
				return struct{}{}, nil
			case <-ctx.Done():
				return struct{}{}, ctx.Err()
			}
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(ctx, client, task, struct{}{}, taskq.WithTimeout(50*time.Millisecond))
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)
		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task timeout")

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateFailure, state.State)
	})

	// Таймаут с ретраями: все попытки упираются в таймаут, задача уходит в dead-letter.
	t.Run("timeout with retries ends in dead", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("sleepy")
		var attempts atomic.Int32
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			attempts.Add(1)
			select {
			case <-time.After(500 * time.Millisecond):
				return struct{}{}, nil
			case <-ctx.Done():
				return struct{}{}, ctx.Err()
			}
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(
			ctx,
			client,
			task,
			struct{}{},
			taskq.WithTimeout(30*time.Millisecond),
			taskq.WithRetry(taskq.RetryPolicy{
				MaxAttempts:  2,
				InitialDelay: 10 * time.Millisecond,
			}),
		)
		require.NoError(t, err)

		workerCtx, cancelWorker := context.WithCancel(ctx)
		defer cancelWorker()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = future.GetWithTimeout(workerCtx, 10*time.Second)
		}()
		go func() {
			_ = worker.Run(workerCtx, "default")
		}()

		// assert
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for job to settle")
		}

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateDead, state.State)
		assert.Equal(t, int32(2), attempts.Load())

		cancelWorker()
	})

	// Дедлайн уже в прошлом: запуск сразу завершается ошибкой таймаута.
	t.Run("past deadline fails immediately", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("late")
		var ran bool
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			ran = true
			return struct{}{}, ctx.Err() // дедлайн в прошлом — ctx уже отменен
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(
			ctx,
			client,
			task,
			struct{}{},
			taskq.WithDeadline(time.Now().Add(-time.Hour)),
		)
		require.NoError(t, err)

		// act
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)
		assert.True(t, ran, "handler must be invoked with a canceled ctx")
		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task timeout")
	})
}

func TestJobCancellation(t *testing.T) {
	t.Parallel()

	// Отмена выполняющейся задачи через Worker.Cancel: контекст задачи отменяется,
	// состояние — canceled, Future возвращает ErrJobCanceled.
	t.Run("cancel in-flight job", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("blocking")
		started := make(chan struct{})
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			close(started)
			<-ctx.Done()
			return struct{}{}, ctx.Err()
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(ctx, client, task, struct{}{})
		require.NoError(t, err)

		go func() {
			_ = worker.Run(ctx, "default")
		}()

		// Дождёмся старта, затем отменим.
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("task did not start")
		}

		// act
		require.True(t, worker.Cancel(future.ID()), "job must be in-flight")

		// assert
		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		assert.ErrorIs(t, err, taskq.ErrJobCanceled)

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateCanceled, state.State)
	})

	// Отмена до старта через Client.Cancel: воркер задачу пропускает.
	t.Run("cancel before start", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("idle")
		var ran bool
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			ran = true
			return struct{}{}, nil
		})
		require.NoError(t, err)

		// Задача публикуется, воркер пока не запущен.
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(ctx, client, task, struct{}{})
		require.NoError(t, err)

		// act
		require.NoError(t, client.Cancel(ctx, future.ID()))

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)
		ack := runWorkerOnce(ctx, t, worker, broker, "default")

		// assert
		assert.Equal(t, taskq.AckAck, ack)
		assert.False(t, ran, "canceled job must not be executed")

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateCanceled, state.State)

		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		assert.ErrorIs(t, err, taskq.ErrJobCanceled)
	})

	// Отмена терминальной задачи запрещена.
	t.Run("cancel terminal job is rejected", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("ok")
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(ctx, client, task, struct{}{})
		require.NoError(t, err)

		_ = runWorkerOnce(ctx, t, worker, broker, "default")
		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		require.NoError(t, err)

		// act
		err = client.Cancel(ctx, future.ID())

		// assert
		assert.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrStateConflict)
	})

	// Client.Cancel неизвестной задачи — ErrJobNotFound.
	t.Run("cancel unknown job", func(t *testing.T) {
		t.Parallel()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		// act
		err = client.Cancel(t.Context(), "unknown")

		// assert
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrJobNotFound)
	})
}

func TestDeadLetterAndRescue(t *testing.T) {
	t.Parallel()

	// Ретраи исчерпаны: dead-letter, хук вызывается один раз.
	// Rescue возвращает задачу в очередь, она выполняется заново.
	t.Run("dead letter then rescue", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("flaky")
		var calls atomic.Int32
		var deadHookCalls atomic.Int32
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			n := calls.Add(1)
			if n <= 2 {
				return struct{}{}, errors.New("always fails first two times")
			}
			return struct{}{}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(
			registry,
			broker,
			backend,
			taskq.WithOnDeadHook(func(ctx context.Context, job *taskq.Job, cause error) {
				deadHookCalls.Add(1)
			}),
		)
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(
			ctx,
			client,
			task,
			struct{}{},
			taskq.WithRetry(taskq.RetryPolicy{
				MaxAttempts:  2,
				InitialDelay: 10 * time.Millisecond,
			}),
		)
		require.NoError(t, err)

		workerCtx, cancelWorker := context.WithCancel(ctx)
		defer cancelWorker()
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			_ = worker.Run(workerCtx, "default")
		}()

		// Дожидаемся dead-letter.
		require.Eventually(t, func() bool {
			st, err := backend.GetState(ctx, future.ID())
			return err == nil && st.State == taskq.StateDead
		}, 10*time.Second, 25*time.Millisecond)

		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job is dead")
		assert.Equal(t, int32(1), deadHookCalls.Load(), "dead hook must fire exactly once")

		// act: rescue
		require.NoError(t, client.Rescue(ctx, future.ID()))

		// Запущенный воркер подхватит перепубликованную задачу.
		_, err = future.GetWithTimeout(ctx, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int32(3), calls.Load())

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateSuccess, state.State)

		cancelWorker()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not stop")
		}
	})

	// Rescue работает и из failure (ретрай-политика не задана).
	t.Run("rescue failed job", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("flaky")
		var calls atomic.Int32
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			if calls.Add(1) == 1 {
				return struct{}{}, errors.New("first try fails")
			}
			return struct{}{}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		future, err := taskq.Submit(ctx, client, task, struct{}{})
		require.NoError(t, err)
		_ = runWorkerOnce(ctx, t, worker, broker, "default")

		state, err := backend.GetState(ctx, future.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StateFailure, state.State)

		// act
		require.NoError(t, client.Rescue(ctx, future.ID()))

		// Воркер подхватит перепубликованную задачу.
		_ = runWorkerOnce(ctx, t, worker, broker, "default")

		_, err = future.GetWithTimeout(ctx, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	// Rescue из непавшего состояния запрещен.
	t.Run("rescue healthy job is rejected", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("ok")
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, nil
		})
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)
		future, err := taskq.Submit(ctx, client, task, struct{}{})
		require.NoError(t, err)

		// act
		err = client.Rescue(ctx, future.ID())

		// assert
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrStateConflict)
	})
}

// inspectorEnv — окружение с выбранным backend для проверки JobInspector.
type inspectorEnv struct {
	backend taskq.JobInspector
	client  *taskq.Client
	broker  *membroker.Broker
}

func newInspectorEnv(t *testing.T, useFile bool) inspectorEnv {
	t.Helper()

	broker := membroker.NewBroker()
	var base taskq.Backend
	var err error
	if useFile {
		base, err = filebackend.New(t.TempDir())
	} else {
		base = membackend.NewBackend()
	}
	require.NoError(t, err)

	inspector, ok := base.(taskq.JobInspector)
	require.True(t, ok, "test backend must implement JobInspector")

	client, err := taskq.NewClient(broker, base)
	require.NoError(t, err)
	return inspectorEnv{backend: inspector, client: client, broker: broker}
}

func TestJobInspector(t *testing.T) {
	t.Parallel()

	runCases := func(t *testing.T, env inspectorEnv) {
		ctx := t.Context()

		taskA := taskq.NewTask[struct{}, struct{}]("task-a")
		taskB := taskq.NewTask[struct{}, struct{}]("task-b")

		var ids [3]string
		for i := 0; i < 2; i++ {
			f, err := taskq.Submit(ctx, env.client, taskA, struct{}{}, taskq.WithJobID(fmt.Sprintf("insp-a-%d", i)))
			require.NoError(t, err)
			ids[i] = f.ID()
		}
		f, err := taskq.Submit(ctx, env.client, taskB, struct{}{}, taskq.WithJobID("insp-b-0"))
		require.NoError(t, err)
		ids[2] = f.ID()

		// Inspect: полный документ задачи.
		job, err := env.backend.Inspect(ctx, ids[0])
		require.NoError(t, err)
		assert.Equal(t, "task-a", job.Name)
		assert.Equal(t, taskq.StatePending, job.State)
		assert.Equal(t, ids[0], job.ID)

		// Inspect неизвестной задачи.
		_, err = env.backend.Inspect(ctx, "ghost")
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrJobNotFound)

		// List без фильтров.
		res, err := env.backend.List(ctx, taskq.ListQuery{})
		require.NoError(t, err)
		assert.Len(t, res.Items, 3)
		assert.True(t, res.Done)

		// List с фильтром по state.
		res, err = env.backend.List(ctx, taskq.ListQuery{State: taskq.StatePending})
		require.NoError(t, err)
		assert.Len(t, res.Items, 3)

		res, err = env.backend.List(ctx, taskq.ListQuery{State: taskq.StateSuccess})
		require.NoError(t, err)
		assert.Len(t, res.Items, 0)

		// List с фильтром по задаче.
		res, err = env.backend.List(ctx, taskq.ListQuery{Task: "task-a"})
		require.NoError(t, err)
		assert.Len(t, res.Items, 2)

		// Пагинация: страница 2, затем следующая.
		res, err = env.backend.List(ctx, taskq.ListQuery{Limit: 2})
		require.NoError(t, err)
		assert.Len(t, res.Items, 2)
		assert.False(t, res.Done)
		require.NotEmpty(t, res.Cursor)

		res, err = env.backend.List(ctx, taskq.ListQuery{Limit: 2, Cursor: res.Cursor})
		require.NoError(t, err)
		assert.Len(t, res.Items, 1)
		assert.True(t, res.Done)

		// Reset из непавшего состояния запрещен.
		err = env.backend.Reset(ctx, ids[0])
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrStateConflict)

		// Reset упавшей задачи возвращает ее в pending.
		failTask := taskq.NewTask[struct{}, struct{}]("fail").WithQueue("failq")
		registry := taskq.NewRegistry()
		err = taskq.Register(registry, failTask, func(ctx context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, errors.New("boom")
		})
		require.NoError(t, err)
		worker, err := taskq.NewWorker(registry, env.broker, env.backend.(taskq.Backend))
		require.NoError(t, err)

		failFuture, err := taskq.Submit(ctx, env.client, failTask, struct{}{}, taskq.WithJobID("insp-fail"))
		require.NoError(t, err)
		_ = runWorkerOnce(ctx, t, worker, env.broker, "failq")

		err = env.backend.Reset(ctx, failFuture.ID())
		require.NoError(t, err)

		state, err := env.backend.Inspect(ctx, failFuture.ID())
		require.NoError(t, err)
		assert.Equal(t, taskq.StatePending, state.State)
		assert.Empty(t, state.Error)
		assert.Zero(t, state.Attempt)

		// Delete: задача исчезает, повторный Delete — ошибка.
		require.NoError(t, env.backend.Delete(ctx, ids[1]))
		_, err = env.backend.Inspect(ctx, ids[1])
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrJobNotFound)

		err = env.backend.Delete(ctx, ids[1])
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrJobNotFound)

		// List больше не видит удаленную задачу.
		res, err = env.backend.List(ctx, taskq.ListQuery{})
		require.NoError(t, err)
		assert.Len(t, res.Items, 3)
	}

	t.Run("memory backend", func(t *testing.T) {
		t.Parallel()
		runCases(t, newInspectorEnv(t, false))
	})

	t.Run("file backend", func(t *testing.T) {
		t.Parallel()
		runCases(t, newInspectorEnv(t, true))
	})
}

func TestIdempotentSubmit(t *testing.T) {
	t.Parallel()

	// Повторный Submit с тем же WithJobID не публикует задачу повторно.
	t.Run("duplicate job id is not republished", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("once")
		var calls atomic.Int32
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			calls.Add(1)
			return struct{}{}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		// act
		first, err := taskq.Submit(ctx, client, task, struct{}{}, taskq.WithJobID("idem-1"))
		require.NoError(t, err)
		second, err := taskq.Submit(ctx, client, task, struct{}{}, taskq.WithJobID("idem-1"))
		require.NoError(t, err)

		assert.Equal(t, first.ID(), second.ID())

		// Воркер обработает единственную опубликованную копию.
		_ = runWorkerOnce(ctx, t, worker, broker, "default")

		_, err = first.GetWithTimeout(ctx, 5*time.Second)
		require.NoError(t, err)

		// assert
		assert.Equal(t, int32(1), calls.Load(), "job must run exactly once")
	})

	// Разные явные ID — разные задачи.
	t.Run("different ids are distinct jobs", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		registry := taskq.NewRegistry()

		task := taskq.NewTask[struct{}, struct{}]("twice")
		var calls atomic.Int32
		err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			calls.Add(1)
			return struct{}{}, nil
		})
		require.NoError(t, err)

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		first, err := taskq.Submit(ctx, client, task, struct{}{}, taskq.WithJobID("idem-a"))
		require.NoError(t, err)
		second, err := taskq.Submit(ctx, client, task, struct{}{}, taskq.WithJobID("idem-b"))
		require.NoError(t, err)

		assert.NotEqual(t, first.ID(), second.ID())
	})
}

func TestGroupWithCanceledJob(t *testing.T) {
	t.Parallel()

	// Группа с отмененной задачей завершается: AllSucceeded=false,
	// для отмененной задачи — ErrJobCanceled.
	ctx := t.Context()
	broker := membroker.NewBroker()
	backend := membackend.NewBackend()
	registry := taskq.NewRegistry()

	task := taskq.NewTask[struct{}, struct{}]("grouped")
	err := taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})
	require.NoError(t, err)

	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)

	group, err := taskq.SubmitGroup(ctx, client, task, []struct{}{{}, {}})
	require.NoError(t, err)

	// Находим опубликованные задачи группы и отменяем вторую до старта воркера.
	res, err := client.List(ctx, taskq.ListQuery{Task: "grouped"})
	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.NoError(t, client.Cancel(ctx, res.Items[1].ID))

	worker, err := taskq.NewWorker(registry, broker, backend)
	require.NoError(t, err)
	go func() {
		_ = worker.Run(ctx, "default")
	}()

	// act
	result, err := group.GetWithTimeout(ctx, 5*time.Second)
	require.NoError(t, err)

	// assert: ровно одна задача отменена, остальные — успех.
	assert.False(t, result.AllSucceeded())
	canceledCount := 0
	for _, e := range result.Errors {
		if errors.Is(e, taskq.ErrJobCanceled) {
			canceledCount++
		}
	}
	assert.Equal(t, 1, canceledCount, "exactly one job must be canceled")
}
