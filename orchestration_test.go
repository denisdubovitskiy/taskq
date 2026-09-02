package taskq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
	membackend "github.com/denisdubovitskiy/taskq/backends/memory"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
)

type doubleArgs struct {
	Value int `json:"value"`
}

type doubleResult struct {
	Value int `json:"value"`
}

// plainBackend — обертка над backend без поддержки аккордов.
type plainBackend struct {
	inner *membackend.Backend
}

func (b *plainBackend) SetState(ctx context.Context, jobID string, state taskq.State) error {
	return b.inner.SetState(ctx, jobID, state)
}

func (b *plainBackend) SetResult(ctx context.Context, jobID string, data []byte) error {
	return b.inner.SetResult(ctx, jobID, data)
}

func (b *plainBackend) SetError(ctx context.Context, jobID string, err string) error {
	return b.inner.SetError(ctx, jobID, err)
}

func (b *plainBackend) GetState(ctx context.Context, jobID string) (*taskq.JobState, error) {
	return b.inner.GetState(ctx, jobID)
}

func (b *plainBackend) GetResult(ctx context.Context, jobID string) (*taskq.JobResult, error) {
	return b.inner.GetResult(ctx, jobID)
}

func (b *plainBackend) Purge(ctx context.Context, jobID string) error {
	return b.inner.Purge(ctx, jobID)
}

func (b *plainBackend) Close(ctx context.Context) error {
	return b.inner.Close(ctx)
}

// startTestWorker запускает воркер на очереди default и останавливает его после теста.
func startTestWorker(t *testing.T, registry *taskq.Registry, broker *membroker.Broker, backend *membackend.Backend) {
	t.Helper()

	worker, err := taskq.NewWorker(registry, broker, backend)
	require.NoError(t, err)

	workerCtx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go func() {
		_ = worker.Run(workerCtx, "default")
	}()
}

// newOrchestrationEnv создает клиент и backend с запущенным воркером.
func newOrchestrationEnv(t *testing.T, registry *taskq.Registry) (*taskq.Client, *membackend.Backend) {
	t.Helper()

	broker := membroker.NewBroker()
	backend := membackend.NewBackend()

	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)

	startTestWorker(t, registry, broker, backend)
	return client, backend
}

func TestSubmitGroup(t *testing.T) {
	t.Parallel()

	// Проверяем успешное выполнение группы и сохранение порядка результатов.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		addTask := taskq.NewTask[addArgs, addResult]("group-add")
		err := taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		client, _ := newOrchestrationEnv(t, registry)

		// act
		group, err := taskq.SubmitGroup(ctx, client, addTask, []addArgs{
			{A: 1, B: 2},
			{A: 3, B: 4},
			{A: 5, B: 6},
		})
		require.NoError(t, err)

		result, err := group.GetWithTimeout(ctx, 5*time.Second)

		// assert
		require.NoError(t, err)
		assert.True(t, result.AllSucceeded())
		assert.Equal(t, []addResult{{Sum: 3}, {Sum: 7}, {Sum: 11}}, result.Results)
		require.Len(t, result.Errors, 3)
		assert.Nil(t, result.Errors[0])
		assert.Nil(t, result.Errors[1])
		assert.Nil(t, result.Errors[2])
		assert.Len(t, result.JobIDs(), 3)
		assert.NotEmpty(t, result.ID())
	})

	// Проверяем, что отдельные сбои не прерывают ожидание и фиксируются в Errors.
	t.Run("partial failure", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		faddTask := taskq.NewTask[addArgs, addResult]("group-fadd")
		err := taskq.Register(registry, faddTask, func(ctx context.Context, args addArgs) (addResult, error) {
			if args.A < 0 {
				return addResult{}, errors.New("negative not allowed")
			}
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		client, _ := newOrchestrationEnv(t, registry)

		// act
		group, err := taskq.SubmitGroup(ctx, client, faddTask, []addArgs{
			{A: 1, B: 2},
			{A: -1, B: 2},
			{A: 3, B: 4},
		})
		require.NoError(t, err)

		result, err := group.GetWithTimeout(ctx, 5*time.Second)

		// assert
		require.NoError(t, err)
		assert.False(t, result.AllSucceeded())
		assert.Equal(t, []addResult{{Sum: 3}, {}, {Sum: 7}}, result.Results)
		require.Len(t, result.Errors, 3)
		assert.Nil(t, result.Errors[0])
		require.Error(t, result.Errors[1])
		assert.Contains(t, result.Errors[1].Error(), "negative not allowed")
		assert.Nil(t, result.Errors[2])
	})

	// Проверяем валидацию параметров SubmitGroup.
	t.Run("empty payloads", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		client, _ := newOrchestrationEnv(t, registry)
		addTask := taskq.NewTask[addArgs, addResult]("group-add-empty")

		// act
		group, err := taskq.SubmitGroup(ctx, client, addTask, nil)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "payloads")
		assert.Nil(t, group)
	})

	t.Run("client nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		addTask := taskq.NewTask[addArgs, addResult]("group-add-nil")

		// act
		group, err := taskq.SubmitGroup[addArgs, addResult](ctx, nil, addTask, []addArgs{{A: 1, B: 2}})

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "client")
		assert.Nil(t, group)
	})

	t.Run("task nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		client, _ := newOrchestrationEnv(t, registry)

		// act
		group, err := taskq.SubmitGroup[addArgs, addResult](ctx, client, nil, []addArgs{{A: 1, B: 2}})

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task")
		assert.Nil(t, group)
	})
}

func TestChain(t *testing.T) {
	t.Parallel()

	// Проверяем передачу результата предыдущего шага в следующий и тип результата.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()

		firstTask := taskq.NewTask[doubleArgs, doubleResult]("chain-first")
		err := taskq.Register(registry, firstTask, func(ctx context.Context, args doubleArgs) (doubleResult, error) {
			return doubleResult{Value: args.Value * 2}, nil
		})
		require.NoError(t, err)

		secondTask := taskq.NewTask[doubleResult, doubleResult]("chain-second")
		err = taskq.Register(registry, secondTask, func(ctx context.Context, args doubleResult) (doubleResult, error) {
			return doubleResult{Value: args.Value + 8}, nil
		})
		require.NoError(t, err)

		client, _ := newOrchestrationEnv(t, registry)

		// act
		builder, err := taskq.NewChain(client)
		require.NoError(t, err)
		future, err := taskq.Add(
			taskq.Add(builder, firstTask, doubleArgs{Value: 21}),
			secondTask,
		).Send(ctx)
		require.NoError(t, err)

		result, err := future.GetWithTimeout(ctx, 5*time.Second)

		// assert: 21*2=42, 42+8=50
		require.NoError(t, err)
		assert.Equal(t, doubleResult{Value: 50}, result)
		assert.Len(t, builder.StepIDs(), 2)
	})

	// Проверяем, что сбой шага прерывает цепочку и помечает остальные шаги failed.
	t.Run("failure interrupts remaining steps", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()

		firstTask := taskq.NewTask[doubleArgs, doubleResult]("chain-fail-first")
		err := taskq.Register(registry, firstTask, func(ctx context.Context, args doubleArgs) (doubleResult, error) {
			return doubleResult{}, errors.New("boom")
		})
		require.NoError(t, err)

		var secondRan atomic.Bool
		secondTask := taskq.NewTask[doubleResult, doubleResult]("chain-fail-second")
		err = taskq.Register(registry, secondTask, func(ctx context.Context, args doubleResult) (doubleResult, error) {
			secondRan.Store(true)
			return doubleResult{Value: args.Value}, nil
		})
		require.NoError(t, err)

		client, backend := newOrchestrationEnv(t, registry)

		builder, err := taskq.NewChain(client)
		require.NoError(t, err)
		future, err := taskq.Add(
			taskq.Add(builder, firstTask, doubleArgs{Value: 1}),
			secondTask,
		).Send(ctx)
		require.NoError(t, err)

		// act
		_, err = future.GetWithTimeout(ctx, 5*time.Second)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "boom")
		assert.False(t, secondRan.Load())

		stepIDs := builder.StepIDs()
		require.Len(t, stepIDs, 2)
		state, err := backend.GetState(ctx, stepIDs[1])
		require.NoError(t, err)
		assert.Equal(t, taskq.StateFailure, state.State)
		assert.Contains(t, state.Error, "chain interrupted")
	})

	// Проверяем цепочку из одного шага.
	t.Run("single step", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		oneTask := taskq.NewTask[doubleArgs, doubleResult]("chain-one")
		err := taskq.Register(registry, oneTask, func(ctx context.Context, args doubleArgs) (doubleResult, error) {
			return doubleResult{Value: args.Value * 10}, nil
		})
		require.NoError(t, err)

		client, _ := newOrchestrationEnv(t, registry)

		// act
		builder, err := taskq.NewChain(client)
		require.NoError(t, err)
		future, err := taskq.Add(builder, oneTask, doubleArgs{Value: 4}).Send(ctx)
		require.NoError(t, err)

		result, err := future.GetWithTimeout(ctx, 5*time.Second)

		// assert
		require.NoError(t, err)
		assert.Equal(t, doubleResult{Value: 40}, result)
	})

	// Проверяем валидацию цепочки при отправке.
	t.Run("empty chain", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		client, _ := newOrchestrationEnv(t, registry)
		builder, err := taskq.NewChain(client)
		require.NoError(t, err)

		// act
		future, err := builder.Send(ctx)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
		assert.Nil(t, future)
	})

	t.Run("payload only for first step", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		client, _ := newOrchestrationEnv(t, registry)

		firstTask := taskq.NewTask[doubleArgs, doubleResult]("chain-payload-first")
		secondTask := taskq.NewTask[doubleResult, doubleResult]("chain-payload-second")

		builder, err := taskq.NewChain(client)
		require.NoError(t, err)

		// act
		_, err = taskq.Add(
			taskq.Add(builder, firstTask, doubleArgs{Value: 1}),
			secondTask,
			doubleResult{Value: 2},
		).Send(ctx)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "payload")
	})

	t.Run("client nil", func(t *testing.T) {
		t.Parallel()

		// act
		builder, err := taskq.NewChain(nil)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "client")
		assert.Nil(t, builder)
	})
}

func TestChord(t *testing.T) {
	t.Parallel()

	// Проверяем вызов callback с результатами группы в исходном порядке.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()

		addTask := taskq.NewTask[addArgs, addResult]("chord-add")
		err := taskq.Register(registry, addTask, func(ctx context.Context, args addArgs) (addResult, error) {
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		var mu sync.Mutex
		var received []addResult
		callbackTask := taskq.NewTask[[]addResult, addResult]("chord-callback")
		err = taskq.Register(registry, callbackTask, func(ctx context.Context, args []addResult) (addResult, error) {
			mu.Lock()
			received = append(received, args...)
			mu.Unlock()

			total := 0
			for _, r := range args {
				total += r.Sum
			}
			return addResult{Sum: total}, nil
		})
		require.NoError(t, err)

		client, _ := newOrchestrationEnv(t, registry)

		// act
		future, err := taskq.SubmitChord(ctx, client, addTask, []addArgs{
			{A: 1, B: 2},
			{A: 3, B: 4},
			{A: 5, B: 6},
		}, callbackTask)
		require.NoError(t, err)

		result, err := future.GetWithTimeout(ctx, 5*time.Second)

		// assert: 3+7+11=21
		require.NoError(t, err)
		assert.Equal(t, addResult{Sum: 21}, result)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, []addResult{{Sum: 3}, {Sum: 7}, {Sum: 11}}, received)
	})

	// Проверяем, что сбой задачи в группе не запускает callback.
	t.Run("failure skips callback", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()

		faddTask := taskq.NewTask[addArgs, addResult]("chord-fadd")
		err := taskq.Register(registry, faddTask, func(ctx context.Context, args addArgs) (addResult, error) {
			if args.A < 0 {
				return addResult{}, errors.New("negative not allowed")
			}
			return addResult{Sum: args.A + args.B}, nil
		})
		require.NoError(t, err)

		var callbackRan atomic.Bool
		callbackTask := taskq.NewTask[[]addResult, addResult]("chord-fail-callback")
		err = taskq.Register(registry, callbackTask, func(ctx context.Context, args []addResult) (addResult, error) {
			callbackRan.Store(true)
			return addResult{Sum: 0}, nil
		})
		require.NoError(t, err)

		client, _ := newOrchestrationEnv(t, registry)

		// act
		future, err := taskq.SubmitChord(ctx, client, faddTask, []addArgs{
			{A: 1, B: 2},
			{A: -1, B: 2},
		}, callbackTask)
		require.NoError(t, err)

		_, err = future.GetWithTimeout(ctx, 5*time.Second)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "chord")
		assert.False(t, callbackRan.Load())
	})

	// Проверяем отказ при отправке, если backend не поддерживает аккорды.
	t.Run("backend without chord support", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		broker := membroker.NewBroker()
		backend := &plainBackend{inner: membackend.NewBackend()}

		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		addTask := taskq.NewTask[addArgs, addResult]("chord-add-plain")
		callbackTask := taskq.NewTask[[]addResult, addResult]("chord-callback-plain")

		// act
		future, err := taskq.SubmitChord(ctx, client, addTask, []addArgs{{A: 1, B: 2}}, callbackTask)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ChordBackend")
		assert.Nil(t, future)
	})

	// Проверяем валидацию параметров SubmitChord.
	t.Run("empty payloads", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		client, _ := newOrchestrationEnv(t, registry)

		addTask := taskq.NewTask[addArgs, addResult]("chord-add-empty")
		callbackTask := taskq.NewTask[[]addResult, addResult]("chord-callback-empty")

		// act
		future, err := taskq.SubmitChord(ctx, client, addTask, nil, callbackTask)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "payloads")
		assert.Nil(t, future)
	})

	t.Run("callback nil", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		registry := taskq.NewRegistry()
		client, _ := newOrchestrationEnv(t, registry)

		addTask := taskq.NewTask[addArgs, addResult]("chord-add-nocb")

		// act
		future, err := taskq.SubmitChord[addArgs, addResult, addResult](ctx, client, addTask, []addArgs{{A: 1, B: 2}}, nil)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "callback")
		assert.Nil(t, future)
	})
}
