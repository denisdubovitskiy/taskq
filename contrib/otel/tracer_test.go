package otel

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/denisdubovitskiy/taskq"
)

// errTest — тестовая ошибка для проверки SetError.
var errTest = errors.New("временный сбой")

// newRecordedTracer создает трейсер на SDK-провайдере со сбором спанов.
func newRecordedTracer(t *testing.T) (*Tracer, *tracetest.SpanRecorder) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })
	return NewTracer(provider.Tracer("test")), recorder
}

// endedByName возвращает завершенные спаны с указанным именем.
func endedByName(t *testing.T, recorder *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	t.Helper()

	var found []sdktrace.ReadOnlySpan
	for _, ended := range recorder.Ended() {
		if ended.Name() == name {
			found = append(found, ended)
		}
	}
	return found
}

// singleEnded возвращает единственный завершенный спан с указанным именем.
func singleEnded(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	found := endedByName(t, recorder, name)
	require.Len(t, found, 1, "ожидался ровно один спан %q", name)
	return found[0]
}

// attrByKey возвращает значение атрибута спана по ключу.
func attrByKey(t *testing.T, span sdktrace.ReadOnlySpan, key string) string {
	t.Helper()

	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String()
		}
	}
	t.Fatalf("атрибут %q не найден", key)
	return ""
}

func TestTracer_Start(t *testing.T) {
	t.Parallel()

	t.Run("span ends with operation name", func(t *testing.T) {
		t.Parallel()

		// arrange
		tracer, recorder := newRecordedTracer(t)

		// act
		_, span := tracer.Start(t.Context(), "taskq.Submit")
		span.End()

		// assert
		require.Len(t, endedByName(t, recorder, "taskq.Submit"), 1)
	})

	t.Run("returned context carries the span", func(t *testing.T) {
		t.Parallel()

		// arrange
		tracer, _ := newRecordedTracer(t)

		// act
		ctx, span := tracer.Start(t.Context(), "taskq.Submit")
		defer span.End()

		// assert
		assert.True(t, trace.SpanFromContext(ctx).SpanContext().IsValid(),
			"span должен быть прикреплен к возвращенному контексту")
	})

	t.Run("nested starts form a trace tree", func(t *testing.T) {
		t.Parallel()

		// arrange
		tracer, recorder := newRecordedTracer(t)

		// act
		ctx, parent := tracer.Start(t.Context(), "taskq.SubmitGroup")
		_, child := tracer.Start(ctx, "taskq.Submit")
		child.End()
		parent.End()

		// assert
		parentSpan := singleEnded(t, recorder, "taskq.SubmitGroup")
		childSpan := singleEnded(t, recorder, "taskq.Submit")
		assert.Equal(t, parentSpan.SpanContext().SpanID(), childSpan.Parent().SpanID(),
			"вложенный span должен ссылаться на родителя")
	})
}

func TestSpan_SetError(t *testing.T) {
	t.Parallel()

	t.Run("records exception event and error status", func(t *testing.T) {
		t.Parallel()

		// arrange
		tracer, recorder := newRecordedTracer(t)
		_, span := tracer.Start(t.Context(), "taskq.Worker.fail")

		// act
		span.SetError(errTest)
		span.End()

		// assert
		stub := singleEnded(t, recorder, "taskq.Worker.fail")
		assert.Equal(t, "Error", stub.Status().Code.String())
		assert.Equal(t, errTest.Error(), stub.Status().Description)
		events := stub.Events()
		require.Len(t, events, 1, "должно быть записано событие exception")
		assert.Equal(t, "exception", events[0].Name)
	})

	t.Run("nil error keeps status unset", func(t *testing.T) {
		t.Parallel()

		// arrange
		tracer, recorder := newRecordedTracer(t)
		_, span := tracer.Start(t.Context(), "taskq.Submit")

		// act
		span.SetError(nil)
		span.End()

		// assert
		stub := singleEnded(t, recorder, "taskq.Submit")
		assert.Equal(t, "Unset", stub.Status().Code.String())
		assert.Empty(t, stub.Events())
	})
}

func TestSpan_SetAttributes(t *testing.T) {
	t.Parallel()

	t.Run("converts slog attrs by value kind", func(t *testing.T) {
		t.Parallel()

		// arrange
		tracer, recorder := newRecordedTracer(t)
		_, span := tracer.Start(t.Context(), "taskq.Worker.process")

		// act
		span.SetAttributes(
			slog.String("job_id", "job_42"),
			slog.String("task", "email"),
			slog.Int("attempts", 3),
			slog.Bool("retry", true),
			slog.Float64("ratio", 0.5),
			slog.Duration("duration", 1500*time.Millisecond),
			slog.Time("eta", time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)),
			slog.Any("cause", errTest),
			slog.String("", "пропускается"),
		)
		span.End()

		// assert
		stub := singleEnded(t, recorder, "taskq.Worker.process")
		assert.Equal(t, "job_42", attrByKey(t, stub, "job_id"))
		assert.Equal(t, "email", attrByKey(t, stub, "task"))
		assert.Equal(t, "3", attrByKey(t, stub, "attempts"))
		assert.Equal(t, "true", attrByKey(t, stub, "retry"))
		assert.Equal(t, "0.5", attrByKey(t, stub, "ratio"))
		assert.Equal(t, "1.5s", attrByKey(t, stub, "duration"))
		assert.Equal(t, "2026-09-02T12:00:00Z", attrByKey(t, stub, "eta"))
		assert.Equal(t, errTest.Error(), attrByKey(t, stub, "cause"))
		for _, kv := range stub.Attributes() {
			assert.NotEmpty(t, string(kv.Key), "атрибут с пустым ключом должен быть пропущен")
		}
	})

	t.Run("groups flatten with prefix", func(t *testing.T) {
		t.Parallel()

		// arrange
		tracer, recorder := newRecordedTracer(t)
		_, span := tracer.Start(t.Context(), "taskq.Submit")

		// act
		span.SetAttributes(
			slog.Group("chord",
				slog.String("chord_id", "chord_1"),
				slog.Group("meta", slog.Int("size", 2)),
			),
		)
		span.End()

		// assert
		stub := singleEnded(t, recorder, "taskq.Submit")
		assert.Equal(t, "chord_1", attrByKey(t, stub, "chord.chord_id"))
		assert.Equal(t, "2", attrByKey(t, stub, "chord.meta.size"))
	})
}

func TestNewTracer_NilFallback(t *testing.T) {
	t.Parallel()

	t.Run("nil tracer is a safe no-op", func(t *testing.T) {
		t.Parallel()

		// arrange + act
		tracer := NewTracer(nil)
		ctx, span := tracer.Start(t.Context(), "taskq.Submit")
		span.SetError(errTest)
		span.SetAttributes(slog.String("job_id", "job_42"))
		span.End()

		// assert: не паникует, span в контексте non-recording
		assert.False(t, trace.SpanFromContext(ctx).IsRecording())
	})
}

// Компиляционная проверка совместимости с интерфейсом taskq.Tracer.
var _ taskq.Tracer = NewTracer(nil)
