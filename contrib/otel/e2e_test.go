package otel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/denisdubovitskiy/taskq"
	membackend "github.com/denisdubovitskiy/taskq/backends/memory"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
	oteltracer "github.com/denisdubovitskiy/taskq/contrib/otel"
)

// e2eArgs — аргументы тестовой задачи.
type e2eArgs struct {
	N int `json:"n"`
}

// newE2EStack собирает стек с трейсером: брокер, backend, воркер и клиент,
// подключенные через один Tracer со сбором спанов.
func newE2EStack(t *testing.T) (*taskq.Client, *tracetest.SpanRecorder) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := oteltracer.NewTracer(provider.Tracer("e2e"))

	broker := membroker.NewBroker()
	backend := membackend.NewBackend()

	double := taskq.NewTask[e2eArgs, int]("double")
	reg := taskq.NewRegistry()
	require.NoError(t, taskq.Register(reg, double, func(_ context.Context, a e2eArgs) (int, error) {
		return a.N * 2, nil
	}))
	failer := taskq.NewTask[e2eArgs, int]("failer")
	require.NoError(t, taskq.Register(reg, failer, func(_ context.Context, _ e2eArgs) (int, error) {
		return 0, errors.New("временный сбой")
	}))

	worker, err := taskq.NewWorker(reg, broker, backend,
		taskq.WithWorkerTracer(tracer))
	require.NoError(t, err)

	client, err := taskq.NewClient(broker, backend,
		taskq.WithTracer(tracer))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = worker.Run(ctx, "")
	}()
	t.Cleanup(func() {
		cancel()
		_ = worker.Shutdown(context.Background())
	})
	return client, recorder
}

// spanByName ищет завершенный спан по имени.
func spanByName(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, span := range recorder.Ended() {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("спан %q не найден", name)
	return nil
}

// TestE2E_SuccessSpans проверяет сквозной сценарий: клиент и воркер,
// подключенные через Tracer, эмитят спаны Submit и Worker.process
// с атрибутами job_id и task.
func TestE2E_SuccessSpans(t *testing.T) {
	t.Parallel()

	// arrange
	client, recorder := newE2EStack(t)
	double := taskq.NewTask[e2eArgs, int]("double")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// act
	future, err := taskq.Submit[e2eArgs, int](ctx, client, double, e2eArgs{N: 21})
	require.NoError(t, err)

	res, err := future.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, 42, res)

	// assert
	submit := spanByName(t, recorder, "taskq.Submit")
	assert.Equal(t, "Unset", submit.Status().Code.String(),
		"успешный Submit не должен ставить статус ошибки")

	process := spanByName(t, recorder, "taskq.Worker.process")
	assert.Equal(t, "Unset", process.Status().Code.String())
	assert.Equal(t, "double", attrValue(t, process, "task"))
	assert.Contains(t, attrValue(t, process, "job_id"), "job_")
}

// TestE2E_FailSpan проверяет путь сбоя: попытка без ретраев завершается
// спаном taskq.Worker.fail со статусом Error и событием exception.
func TestE2E_FailSpan(t *testing.T) {
	t.Parallel()

	// arrange
	client, recorder := newE2EStack(t)
	failer := taskq.NewTask[e2eArgs, int]("failer")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// act
	future, err := taskq.Submit[e2eArgs, int](ctx, client, failer, e2eArgs{N: 1},
		taskq.WithRetry(taskq.RetryPolicy{MaxAttempts: 1}))
	require.NoError(t, err)

	_, err = future.Get(ctx)
	require.Error(t, err)

	// assert
	fail := spanByName(t, recorder, "taskq.Worker.fail")
	assert.Equal(t, "Error", fail.Status().Code.String())
	require.Len(t, fail.Events(), 1, "SetError должен записать событие exception")
	assert.Equal(t, "exception", fail.Events()[0].Name)
}

// attrValue возвращает значение атрибута спана по ключу.
func attrValue(t *testing.T, span sdktrace.ReadOnlySpan, key string) string {
	t.Helper()

	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String()
		}
	}
	t.Fatalf("атрибут %q не найден", key)
	return ""
}
