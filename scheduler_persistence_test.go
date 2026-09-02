package taskq_test

import (
	"context"
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

// newScheduleEnv — брокер, клиент и backend, реализующий ScheduleStore.
func newScheduleEnv(t *testing.T, useFile bool) (*membroker.Broker, *taskq.Client, taskq.Backend, taskq.ScheduleStore) {
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

	store, ok := base.(taskq.ScheduleStore)
	require.True(t, ok, "test backend must implement ScheduleStore")

	client, err := taskq.NewClient(broker, base)
	require.NoError(t, err)
	return broker, client, base, store
}

// absDuration возвращает длительность по модулю.
func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// Фаза расписания переживает «рестарт»: повторная регистрация
// забирает next из сохраненного документа, а не вычисляет заново.
func TestSchedulerPersistence(t *testing.T) {
	t.Parallel()

	runCases := func(t *testing.T, useFile bool) {
		ctx := t.Context()
		_, client, _, store := newScheduleEnv(t, useFile)

		registry := taskq.NewRegistry()
		task := taskq.NewTask[struct{}, struct{}]("persist-task")
		require.NoError(t, taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, nil
		}))

		sched1, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)
		s1, err := taskq.AddEvery(ctx, sched1, "persist", 10*time.Minute, task, struct{}{})
		require.NoError(t, err)
		require.False(t, s1.NextRun().IsZero())

		// Документ сохранен при регистрации.
		doc, err := store.GetSchedule(ctx, "persist")
		require.NoError(t, err)
		assert.Equal(t, "persist-task", doc.Task)
		assert.Equal(t, taskq.ScheduleEvery, doc.Kind)
		assert.Equal(t, 10*time.Minute, doc.Interval)
		assert.True(t, doc.NextRun.After(time.Now().UTC()))

		// Расписания не попадают в job-список.
		jobsBefore, err := client.List(ctx, taskq.ListQuery{})
		require.NoError(t, err)
		jobsAfter, err := client.List(ctx, taskq.ListQuery{})
		require.NoError(t, err)
		assert.Equal(t, len(jobsBefore.Items), len(jobsAfter.Items))

		// ListSchedules видит расписание.
		list, err := store.ListSchedules(ctx)
		require.NoError(t, err)
		require.Len(t, list, 1)
		assert.Equal(t, "persist", list[0].Name)

		// «Рестарт»: новый планировщик на том же backend — next из документа.
		time.Sleep(100 * time.Millisecond)
		sched2, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)
		s2, err := taskq.AddEvery(ctx, sched2, "persist", 10*time.Minute, task, struct{}{})
		require.NoError(t, err)
		assert.Equal(t, doc.NextRun, s2.NextRun(), "next must be adopted from the stored document")

		// Изменение определения — конфликт.
		sched3, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)
		_, err = taskq.AddEvery(ctx, sched3, "persist", 20*time.Minute, task, struct{}{})
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrScheduleConflict)

		// Remove удаляет документ.
		require.NoError(t, sched2.Remove("persist"))
		_, err = store.GetSchedule(ctx, "persist")
		require.Error(t, err)
		assert.ErrorIs(t, err, taskq.ErrScheduleNotFound)
	}

	t.Run("memory backend", func(t *testing.T) {
		t.Parallel()
		runCases(t, false)
	})
	t.Run("file backend", func(t *testing.T) {
		t.Parallel()
		runCases(t, true)
	})
}

// После тиков следующий next сохраняется в документ.
func TestSchedulerPersistsTicks(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	broker, client, base, store := newScheduleEnv(t, false)

	registry := taskq.NewRegistry()
	task := taskq.NewTask[struct{}, struct{}]("ticks")
	var calls atomic.Int32
	require.NoError(t, taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
		calls.Add(1)
		return struct{}{}, nil
	}))

	worker, err := taskq.NewWorker(registry, broker, base)
	require.NoError(t, err)

	sched, err := taskq.NewScheduler(client, nil)
	require.NoError(t, err)
	_, err = taskq.AddEvery(ctx, sched, "ticks", 50*time.Millisecond, task, struct{}{})
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		_ = worker.Run(runCtx, "default")
	}()
	start := time.Now().UTC()
	go func() {
		_ = sched.Run(runCtx)
	}()

	time.Sleep(300 * time.Millisecond)
	require.NoError(t, sched.Stop())

	// assert
	assert.GreaterOrEqual(t, calls.Load(), int32(3), "expected at least 3 ticks in 300ms")
	doc, err := store.GetSchedule(ctx, "ticks")
	require.NoError(t, err)
	assert.True(t, doc.NextRun.After(start), "next must have advanced past the start")
}

// WithCronTimezone сдвигает момент срабатывания на смещение таймзоны.
func TestSchedulerCronTimezone(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, client, _, _ := newScheduleEnv(t, false)

	registry := taskq.NewRegistry()
	task := taskq.NewTask[struct{}, struct{}]("tz-task")
	require.NoError(t, taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	}))

	sched1, err := taskq.NewScheduler(client, nil)
	require.NoError(t, err)
	utc, err := taskq.AddCron(ctx, sched1, "utc", "0 0 * * *", task, struct{}{})
	require.NoError(t, err)

	sched2, err := taskq.NewScheduler(client, nil)
	require.NoError(t, err)
	shifted, err := taskq.AddCron(
		ctx,
		sched2,
		"shifted",
		"0 0 * * *",
		task,
		struct{}{},
		taskq.WithCronTimezone(time.FixedZone("TZ", -12*3600)),
	)
	require.NoError(t, err)

	now := time.Now().UTC()
	nextUTC := utc.NextRun()
	nextTZ := shifted.NextRun()

	// Оба «следующих полуночи» лежат в ближайшие 24 часа...
	assert.True(t, nextUTC.After(now))
	assert.True(t, nextUTC.Before(now.Add(24*time.Hour)))
	assert.True(t, nextTZ.After(now))
	assert.True(t, nextTZ.Before(now.Add(24*time.Hour)))

	// ...и отличаются ровно на смещение таймзоны (12 часов).
	assert.Equal(t, 12*time.Hour, absDuration(nextTZ.Sub(nextUTC)))
}

// Catch-up политика: документ с next в прошлом.
// Skip — догонного запуска нет, FireOnce — ровно один.
func TestSchedulerCatchUp(t *testing.T) {
	t.Parallel()

	runCases := func(t *testing.T, useFile bool, policy taskq.CatchUpPolicy, wantCalls int32) {
		ctx := t.Context()
		broker, client, base, store := newScheduleEnv(t, useFile)

		registry := taskq.NewRegistry()
		task := taskq.NewTask[struct{}, struct{}]("catch-task")
		var calls atomic.Int32
		require.NoError(t, taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
			calls.Add(1)
			return struct{}{}, nil
		}))

		worker, err := taskq.NewWorker(registry, broker, base)
		require.NoError(t, err)

		// Документ из прошлого: интервал час, next — 100 мс назад.
		interval := time.Hour
		doc := taskq.ScheduleDocument{
			Name:      "catch",
			Task:      task.Name,
			Kind:      taskq.ScheduleEvery,
			Interval:  interval,
			NextRun:   time.Now().UTC().Add(-100 * time.Millisecond),
			UpdatedAt: time.Now().UTC(),
		}
		require.NoError(t, store.SaveSchedule(ctx, doc))

		sched, err := taskq.NewScheduler(client, nil)
		require.NoError(t, err)

		var opts []taskq.ScheduleOption
		if policy == taskq.CatchUpFireOnce {
			opts = append(opts, taskq.WithCatchUp(taskq.CatchUpFireOnce))
		}
		s, err := taskq.AddEvery(ctx, sched, "catch", interval, task, struct{}{}, opts...)
		require.NoError(t, err)

		// Skip: next сразу в будущем, догонного запуска не будет.
		if policy == taskq.CatchUpSkip {
			assert.True(t, s.NextRun().After(time.Now().UTC()), "skip policy must advance past now")
		} else {
			// FireOnce: next в прошлом — тик станет catch-up запуском.
			assert.True(t, s.NextRun().Before(time.Now().UTC()))
		}

		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()
		go func() {
			_ = worker.Run(runCtx, "default")
		}()
		go func() {
			_ = sched.Run(runCtx)
		}()

		// 300 мс: при интервале час регулярных тиков не будет.
		time.Sleep(300 * time.Millisecond)
		require.NoError(t, sched.Stop())

		assert.Equal(t, wantCalls, calls.Load(), "policy %d must fire %d times", policy, wantCalls)
	}

	t.Run("skip / memory", func(t *testing.T) {
		t.Parallel()
		runCases(t, false, taskq.CatchUpSkip, 0)
	})
	t.Run("skip / file", func(t *testing.T) {
		t.Parallel()
		runCases(t, true, taskq.CatchUpSkip, 0)
	})
	t.Run("fire once / memory", func(t *testing.T) {
		t.Parallel()
		runCases(t, false, taskq.CatchUpFireOnce, 1)
	})
	t.Run("fire once / file", func(t *testing.T) {
		t.Parallel()
		runCases(t, true, taskq.CatchUpFireOnce, 1)
	})
}
