package taskq_test

import (
	"context"
	"encoding/json"
	"fmt"
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

// concurrencyTracker фиксирует максимум одновременных вызовов хэндлера.
type concurrencyTracker struct {
	cur int64
	max int64
}

func (c *concurrencyTracker) enter() {
	n := atomic.AddInt64(&c.cur, 1)
	for {
		old := atomic.LoadInt64(&c.max)
		if n <= old || atomic.CompareAndSwapInt64(&c.max, old, n) {
			break
		}
	}
}

func (c *concurrencyTracker) leave() { atomic.AddInt64(&c.cur, -1) }

// bodyForJob собирает тело доставки, как это делает брокер.
func bodyForJob(t *testing.T, id, name string) []byte {
	t.Helper()
	body, err := json.Marshal(taskq.Job{
		ID:      id,
		Name:    name,
		State:   taskq.StatePending,
		Payload: []byte("{}"),
	})
	require.NoError(t, err)
	return body
}

// handleDeliveries вызывает worker.Handle параллельно для каждой доставки.
func handleDeliveries(t *testing.T, worker *taskq.Worker, ctx context.Context, bodies ...[]byte) []taskq.AckType {
	t.Helper()

	acks := make(chan taskq.AckType, len(bodies))
	var wg sync.WaitGroup
	for i, body := range bodies {
		wg.Add(1)
		go func(i int, body []byte) {
			defer wg.Done()
			acks <- worker.Handle(ctx, &taskq.Delivery{
				ID:   fmt.Sprintf("job-%d", i),
				Body: body,
				Ack:  func(context.Context) error { return nil },
				Nack: func(context.Context, bool) error { return nil },
			})
		}(i, body)
	}
	wg.Wait()
	close(acks)

	out := make([]taskq.AckType, 0, len(bodies))
	for ack := range acks {
		out = append(out, ack)
	}
	return out
}

// sleepHandler регистрирует задачу, которая «занимает» слот на d.
func sleepHandler(t *testing.T, registry *taskq.Registry, name string, d time.Duration, tracker *concurrencyTracker) {
	t.Helper()
	task := taskq.NewTask[struct{}, struct{}](name)
	require.NoError(t, taskq.Register(registry, task, func(ctx context.Context, _ struct{}) (struct{}, error) {
		tracker.enter()
		defer tracker.leave()
		select {
		case <-time.After(d):
			return struct{}{}, nil
		case <-ctx.Done():
			return struct{}{}, ctx.Err()
		}
	}))
}

func TestWorker_TaskConcurrencyLimit(t *testing.T) {
	t.Parallel()

	// arrange
	broker := membroker.NewBroker()
	backend := membackend.NewBackend()
	registry := taskq.NewRegistry()
	tracker := &concurrencyTracker{}
	sleepHandler(t, registry, "heavy", 50*time.Millisecond, tracker)

	worker, err := taskq.NewWorker(registry, broker, backend,
		taskq.WithConcurrency(10),
		taskq.WithTaskConcurrency("heavy", 2),
	)
	require.NoError(t, err)

	bodies := make([][]byte, 6)
	for i := range bodies {
		bodies[i] = bodyForJob(t, fmt.Sprintf("j-%d", i), "heavy")
	}

	// act
	acks := handleDeliveries(t, worker, t.Context(), bodies...)

	// assert
	for _, ack := range acks {
		assert.Equal(t, taskq.AckAck, ack)
	}
	assert.LessOrEqual(t, atomic.LoadInt64(&tracker.max), int64(2), "превышен per-task лимит")
	assert.Greater(t, atomic.LoadInt64(&tracker.max), int64(1), "задачи должны выполняться параллельно в рамках лимита")
}

func TestWorker_GlobalPoolBoundsExecution(t *testing.T) {
	t.Parallel()

	// arrange
	broker := membroker.NewBroker()
	backend := membackend.NewBackend()
	registry := taskq.NewRegistry()
	tracker := &concurrencyTracker{}
	sleepHandler(t, registry, "any", 40*time.Millisecond, tracker)

	worker, err := taskq.NewWorker(registry, broker, backend, taskq.WithConcurrency(2))
	require.NoError(t, err)

	bodies := make([][]byte, 4)
	for i := range bodies {
		bodies[i] = bodyForJob(t, fmt.Sprintf("j-%d", i), "any")
	}

	// act
	acks := handleDeliveries(t, worker, t.Context(), bodies...)

	// assert
	for _, ack := range acks {
		assert.Equal(t, taskq.AckAck, ack)
	}
	assert.LessOrEqual(t, atomic.LoadInt64(&tracker.max), int64(2), "превышен общий пул воркера")
	assert.Greater(t, atomic.LoadInt64(&tracker.max), int64(1), "задачи должны выполняться параллельно")
}

func TestWorker_TaskLimitsIndependent(t *testing.T) {
	t.Parallel()

	// arrange
	broker := membroker.NewBroker()
	backend := membackend.NewBackend()
	registry := taskq.NewRegistry()
	trackerA := &concurrencyTracker{}
	trackerB := &concurrencyTracker{}
	shared := &concurrencyTracker{}

	for _, spec := range []struct {
		name    string
		tracker *concurrencyTracker
	}{
		{name: "alpha", tracker: trackerA},
		{name: "beta", tracker: trackerB},
	} {
		tracker := spec.tracker
		require.NoError(t, taskq.Register(registry, taskq.NewTask[struct{}, struct{}](spec.name), func(ctx context.Context, _ struct{}) (struct{}, error) {
			tracker.enter()
			shared.enter()
			defer tracker.leave()
			defer shared.leave()
			select {
			case <-time.After(60 * time.Millisecond):
				return struct{}{}, nil
			case <-ctx.Done():
				return struct{}{}, ctx.Err()
			}
		}))
	}

	worker, err := taskq.NewWorker(registry, broker, backend,
		taskq.WithConcurrency(10),
		taskq.WithTaskConcurrency("alpha", 3),
		taskq.WithTaskConcurrency("beta", 1),
	)
	require.NoError(t, err)

	bodies := make([][]byte, 0, 6)
	for i := 0; i < 4; i++ {
		bodies = append(bodies, bodyForJob(t, fmt.Sprintf("a-%d", i), "alpha"))
	}
	for i := 0; i < 2; i++ {
		bodies = append(bodies, bodyForJob(t, fmt.Sprintf("b-%d", i), "beta"))
	}

	// act
	acks := handleDeliveries(t, worker, t.Context(), bodies...)

	// assert
	for _, ack := range acks {
		assert.Equal(t, taskq.AckAck, ack)
	}
	assert.LessOrEqual(t, atomic.LoadInt64(&trackerA.max), int64(3), "превышен лимит задачи alpha")
	assert.LessOrEqual(t, atomic.LoadInt64(&trackerB.max), int64(1), "превышен лимит задачи beta")
	assert.GreaterOrEqual(t, atomic.LoadInt64(&shared.max), int64(2), "задачи разных типов должны выполняться параллельно")
}

// captureHandler сохраняет доставленную задачу в канал.
type captureHandler struct {
	ch chan<- *taskq.Job
}

func (h *captureHandler) Handle(ctx context.Context, delivery *taskq.Delivery) taskq.AckType {
	var job taskq.Job
	if err := json.Unmarshal(delivery.Body, &job); err != nil {
		return taskq.AckNackDrop
	}
	h.ch <- &job
	return taskq.AckAck
}

// consumeFirstJob дожидается первой задачи из очереди с таймаутом.
func consumeFirstJob(t *testing.T, broker *membroker.Broker, queue string, timeout time.Duration) (*taskq.Job, error) {
	t.Helper()

	received := make(chan *taskq.Job, 1)
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- broker.Consume(ctx, queue, &captureHandler{ch: received})
	}()

	select {
	case job := <-received:
		return job, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("no job in queue %q within %s", queue, timeout)
	}
}

func TestSubmit_WithQueue(t *testing.T) {
	t.Parallel()

	t.Run("routes job to named queue", func(t *testing.T) {
		t.Parallel()

		// arrange
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		task := taskq.NewTask[struct{}, struct{}]("mail-task")

		// act
		_, err = taskq.Submit(t.Context(), client, task, struct{}{}, taskq.WithQueue("mail"))
		require.NoError(t, err)

		// assert: задача в «mail» с правильным Queue
		job, err := consumeFirstJob(t, broker, "mail", 2*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "mail", job.Queue)
		assert.Equal(t, "mail-task", job.Name)

		// Дефолтная очередь осталась пустой.
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		err = broker.Consume(ctx, "default", &captureHandler{ch: make(chan *taskq.Job, 1)})
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("task default queue and WithQueue override", func(t *testing.T) {
		t.Parallel()

		// arrange
		broker := membroker.NewBroker()
		backend := membackend.NewBackend()
		client, err := taskq.NewClient(broker, backend)
		require.NoError(t, err)

		task := taskq.NewTask[struct{}, struct{}]("report")
		task.Queue = "reports"

		// act
		_, err = taskq.Submit(t.Context(), client, task, struct{}{})
		require.NoError(t, err)
		_, err = taskq.Submit(t.Context(), client, task, struct{}{}, taskq.WithQueue("urgent"))
		require.NoError(t, err)

		// assert
		defaultJob, err := consumeFirstJob(t, broker, "reports", 2*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "reports", defaultJob.Queue)

		urgentJob, err := consumeFirstJob(t, broker, "urgent", 2*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "urgent", urgentJob.Queue)
	})
}

// waitForJobState дожидается состояния задачи в backend (не более 5 секунд).
func waitForJobState(t *testing.T, client *taskq.Client, jobID string, want taskq.State) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	for {
		job, err := client.Inspect(ctx, jobID)
		if err == nil && job.State == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("job %s did not reach state %s (timeout)", jobID, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestWorker_RunQueues(t *testing.T) {
	t.Parallel()

	// arrange
	broker := membroker.NewBroker()
	backend := membackend.NewBackend()
	registry := taskq.NewRegistry()
	task := taskq.NewTask[struct{}, struct{}]("both")
	require.NoError(t, taskq.Register(registry, task, func(context.Context, struct{}) (struct{}, error) {
		return struct{}{}, nil
	}))

	worker, err := taskq.NewWorker(registry, broker, backend)
	require.NoError(t, err)
	client, err := taskq.NewClient(broker, backend)
	require.NoError(t, err)

	ctx := t.Context()
	runErrCh := make(chan error, 1)
	// «reports» передан дважды — дубликаты должны игнорироваться.
	go func() {
		runErrCh <- worker.RunQueues(ctx, "default", "reports", "reports")
	}()
	time.Sleep(50 * time.Millisecond)

	// act: задачи в обе очереди
	id1, err := taskq.Submit(ctx, client, task, struct{}{})
	require.NoError(t, err)
	id2, err := taskq.Submit(ctx, client, task, struct{}{}, taskq.WithQueue("reports"))
	require.NoError(t, err)

	// assert: обе обработаны
	waitForJobState(t, client, id1.ID(), taskq.StateSuccess)
	waitForJobState(t, client, id2.ID(), taskq.StateSuccess)

	// act: остановка
	require.NoError(t, worker.Stop())

	// assert: RunQueues завершился с ошибкой контекста
	select {
	case runErr := <-runErrCh:
		assert.ErrorIs(t, runErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for RunQueues to return")
	}
}
