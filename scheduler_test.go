package taskq_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	membackend "github.com/denisdubovitskiy/taskq/backends/memory"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
	"github.com/denisdubovitskiy/taskq/lockers/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler_AddEvery(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)
		registry := taskq.NewRegistry()

		var fired int64
		task := taskq.NewTask[struct{}, struct{}]("every")
		err = taskq.Register(registry, task, func(ctx context.Context, p struct{}) (struct{}, error) {
			atomic.AddInt64(&fired, 1)
			return struct{}{}, nil
		})
		require.NoError(t, err)

		worker, err := taskq.NewWorker(registry, broker, backend)
		require.NoError(t, err)
		go func() { _ = worker.Run(ctx, "default") }()

		scheduler, err := taskq.NewScheduler(client, nil) // no locker
		require.NoError(t, err)

		sched, err := taskq.AddEvery(ctx, scheduler, "every-task", 100*time.Millisecond, task, struct{}{})
		require.NoError(t, err)
		assert.Equal(t, "every-task", sched.Name())

		// Первый запуск — через interval после регистрации.
		require.False(t, sched.NextRun().IsZero())
		assert.True(t, sched.NextRun().After(time.Now().Add(50*time.Millisecond)),
			"first run should be ~100ms after registration")

		go func() { _ = scheduler.Run(ctx) }()
		defer func() { _ = scheduler.Stop() }()

		// В течение 800ms ожидается ~7 срабатываний (первое — через 100ms).
		deadline := time.Now().Add(800 * time.Millisecond)
		for time.Now().Before(deadline) && atomic.LoadInt64(&fired) < 3 {
			time.Sleep(20 * time.Millisecond)
		}
		assert.GreaterOrEqual(t, atomic.LoadInt64(&fired), int64(3),
			"task should have fired multiple times")
	})

	t.Run("invalid params", func(t *testing.T) {
		t.Parallel()
		client, err := taskq.NewClient(membroker.NewBroker(), membackend.NewBackend())
		require.NoError(t, err)
		scheduler, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)
		task := taskq.NewTask[struct{}, struct{}]("task")

		_, err = taskq.AddEvery(context.Background(), scheduler, "", 100*time.Millisecond, task, struct{}{})
		assert.Error(t, err, "name required")

		_, err = taskq.AddEvery(context.Background(), scheduler, "name", -1, task, struct{}{})
		assert.Error(t, err, "positive interval required")

		_, err = taskq.AddEvery[struct{}, struct{}](context.Background(), scheduler, "name", 100*time.Millisecond, nil, struct{}{})
		assert.Error(t, err, "task required")

		_, err = taskq.AddEvery(context.Background(), nil, "name", 100*time.Millisecond, task, struct{}{})
		assert.Error(t, err, "scheduler required")
	})

	t.Run("duplicate name", func(t *testing.T) {
		t.Parallel()
		client, err := taskq.NewClient(membroker.NewBroker(), membackend.NewBackend())
		require.NoError(t, err)
		scheduler, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)
		task := taskq.NewTask[struct{}, struct{}]("task")

		_, err = taskq.AddEvery(context.Background(), scheduler, "name", 100*time.Millisecond, task, struct{}{})
		require.NoError(t, err)

		_, err = taskq.AddEvery(context.Background(), scheduler, "name", 200*time.Millisecond, task, struct{}{})
		assert.Error(t, err, "duplicate name must be rejected")
	})
}

func TestScheduler_AddCron(t *testing.T) {
	t.Parallel()

	t.Run("validation", func(t *testing.T) {
		t.Parallel()
		client, err := taskq.NewClient(membroker.NewBroker(), membackend.NewBackend())
		require.NoError(t, err)
		scheduler, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)
		task := taskq.NewTask[struct{}, struct{}]("task")

		_, err = taskq.AddCron(context.Background(), scheduler, "bad-cron", "invalid expression", task, struct{}{})
		assert.Error(t, err, "invalid cron expr")

		_, err = taskq.AddCron(context.Background(), scheduler, "", "0 * * * *", task, struct{}{})
		assert.Error(t, err, "name required")
	})

	t.Run("next run calculation", func(t *testing.T) {
		t.Parallel()
		client, err := taskq.NewClient(membroker.NewBroker(), membackend.NewBackend())
		require.NoError(t, err)
		scheduler, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)
		task := taskq.NewTask[struct{}, struct{}]("task")

		// Cron: начало каждого часа.
		sched, err := taskq.AddCron(context.Background(), scheduler, "hourly", "0 * * * *", task, struct{}{})
		require.NoError(t, err)

		before := time.Now().UTC()
		next := sched.NextRun()
		require.False(t, next.IsZero(), "next run must be calculated")
		assert.True(t, next.After(before), "next run must be in the future")

		expected := before.Truncate(time.Hour).Add(time.Hour)
		// Допускаем сдвиг на час, если wall clock пересек границу часа
		// между AddCron и NextRun.
		assert.True(t, next.Equal(expected) || next.Equal(expected.Add(time.Hour)),
			"next run %s should be top of the next hour %s", next, expected)
	})
}

func TestScheduler_Remove(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	broker := membroker.NewBroker()
	backend := membackend.NewBackend()
	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)
	scheduler, err := taskq.NewScheduler(client, nil)
	require.NoError(t, err)
	task := taskq.NewTask[struct{}, struct{}]("task")

	var fired int64
	registry := taskq.NewRegistry()
	require.NoError(t, taskq.Register(registry, task, func(ctx context.Context, p struct{}) (struct{}, error) {
		atomic.AddInt64(&fired, 1)
		return struct{}{}, nil
	}))
	worker, err := taskq.NewWorker(registry, broker, backend)
	require.NoError(t, err)
	go func() { _ = worker.Run(ctx, "default") }()

	sched, err := taskq.AddEvery(ctx, scheduler, "to-remove", 100*time.Millisecond, task, struct{}{})
	require.NoError(t, err)

	go func() { _ = scheduler.Run(ctx) }()
	defer func() { _ = scheduler.Stop() }()

	// Удаление до первого тика: задача не должна сработать.
	require.NoError(t, scheduler.Remove("to-remove"))
	assert.True(t, sched.NextRun().IsZero(), "removed schedule has no next run")

	assert.Error(t, scheduler.Remove("to-remove"), "second remove must fail")
	assert.Error(t, scheduler.Remove("unknown"), "unknown schedule must fail")

	time.Sleep(300 * time.Millisecond)
	assert.Zero(t, atomic.LoadInt64(&fired), "removed schedule must not fire")
}

func TestScheduler_DualInstanceExclusion(t *testing.T) {
	t.Parallel()

	// Shared resources
	broker := membroker.NewBroker()
	backend := membackend.NewBackend()
	locker := memory.NewLocker()
	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)
	registry := taskq.NewRegistry()

	task := taskq.NewTask[struct{}, struct{}]("shared-task")
	var fireCount int64
	err = taskq.Register(registry, task, func(ctx context.Context, p struct{}) (struct{}, error) {
		atomic.AddInt64(&fireCount, 1)
		return struct{}{}, nil
	})
	require.NoError(t, err)

	// One worker to handle all jobs
	worker, err := taskq.NewWorker(registry, broker, backend)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = worker.Run(ctx, "default") }()

	// Two schedulers competing
	sched1, err := taskq.NewScheduler(client, locker)
	require.NoError(t, err)
	sched2, err := taskq.NewScheduler(client, locker)
	require.NoError(t, err)

	// Task triggers every 100ms
	_, err = taskq.AddEvery(ctx, sched1, "s1", 100*time.Millisecond, task, struct{}{})
	require.NoError(t, err)
	_, err = taskq.AddEvery(ctx, sched2, "s2", 100*time.Millisecond, task, struct{}{})
	require.NoError(t, err)

	go func() { _ = sched1.Run(ctx) }()
	go func() { _ = sched2.Run(ctx) }()

	// 600ms: ~6 окон тика. Если исключение работает, каждое окно
	// срабатывает ровно один раз (победитель lock'а), итого ~5-6.
	// Если lock'а нет/он не работает, будет ~12.
	time.Sleep(600 * time.Millisecond)
	_ = sched1.Stop()
	_ = sched2.Stop()

	count := atomic.LoadInt64(&fireCount)
	// Запас на drift таймеров, но без дублирования.
	assert.GreaterOrEqual(t, count, int64(3), "should have fired at least some times")
	assert.LessOrEqual(t, count, int64(8), "should NOT have doubled the count (exclusion failed)")
}

func TestScheduler_Stop(t *testing.T) {
	t.Parallel()
	client, err := taskq.NewClient(membroker.NewBroker(), membackend.NewBackend())
	require.NoError(t, err)
	scheduler, err := taskq.NewScheduler(client, nil)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- scheduler.Run(t.Context())
	}()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, scheduler.Stop())

	select {
	case err := <-done:
		assert.NoError(t, err, "Run returns nil on graceful stop")
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not stop gracefully")
	}
}

func TestScheduler_Shutdown(t *testing.T) {
	t.Parallel()
	client, err := taskq.NewClient(membroker.NewBroker(), membackend.NewBackend())
	require.NoError(t, err)
	scheduler, err := taskq.NewScheduler(client, nil)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- scheduler.Run(t.Context())
	}()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, scheduler.Shutdown(t.Context()))

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not shutdown gracefully")
	}
}
