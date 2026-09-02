package slogadapter_test

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
	"github.com/denisdubovitskiy/taskq/adapters/slogadapter"
)

// newTestLogger возвращает логгер уровня Debug, пишущий в буфер,
// и сам буфер для проверок.
func newTestLogger(t *testing.T) (*strings.Builder, *slog.Logger) {
	t.Helper()
	buf := &strings.Builder{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return buf, logger
}

func TestLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		level string
		log   func(l taskq.Logger, msg string, attrs ...any)
	}{
		{name: "debug", level: "level=DEBUG", log: func(l taskq.Logger, msg string, attrs ...any) { l.Debug(msg, attrs...) }},
		{name: "info", level: "level=INFO", log: func(l taskq.Logger, msg string, attrs ...any) { l.Info(msg, attrs...) }},
		{name: "warn", level: "level=WARN", log: func(l taskq.Logger, msg string, attrs ...any) { l.Warn(msg, attrs...) }},
		{name: "error", level: "level=ERROR", log: func(l taskq.Logger, msg string, attrs ...any) { l.Error(msg, attrs...) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			buf, logger := newTestLogger(t)

			tt.log(slogadapter.NewLogger(logger), "hello world", "job_id", "job_1", "task", "add")

			out := buf.String()
			assert.Contains(t, out, tt.level)
			assert.Contains(t, out, `msg="hello world"`)
			assert.Contains(t, out, "job_id=job_1")
			assert.Contains(t, out, "task=add")
		})
	}
}

func TestLoggerNilFallsBackToDefault(t *testing.T) {
	t.Parallel()

	// Не должно паниковать: nil заменяется на slog.Default().
	logger := slogadapter.NewLogger(nil)
	logger.Debug("noop", "key", "value")
}

func TestTracer(t *testing.T) {
	t.Parallel()

	t.Run("successful span", func(t *testing.T) {
		t.Parallel()
		buf, logger := newTestLogger(t)
		tracer := slogadapter.NewTracer(logger)

		_, span := tracer.Start(t.Context(), "taskq.Submit")
		span.SetAttributes(slog.String("job_id", "job_1"), slog.String("task", "add"))
		span.End()
		span.End() // идемпотентность

		out := buf.String()
		assert.Contains(t, out, `msg="span started"`)
		assert.Contains(t, out, "operation=taskq.Submit")
		assert.Contains(t, out, `msg="span finished"`)
		assert.Contains(t, out, "job_id=job_1")
		assert.Contains(t, out, "task=add")
		assert.Contains(t, out, "duration=")
		assert.Equal(t, 1, strings.Count(out, `msg="span finished"`), "End должен быть идемпотентным")
		assert.NotContains(t, out, "level=WARN")
	})

	t.Run("failed span", func(t *testing.T) {
		t.Parallel()
		buf, logger := newTestLogger(t)
		tracer := slogadapter.NewTracer(logger)

		_, span := tracer.Start(t.Context(), "taskq.Worker.fail")
		span.SetError(errors.New("boom"))
		span.End()

		out := buf.String()
		assert.Contains(t, out, `level=WARN`)
		assert.Contains(t, out, `msg="span failed"`)
		assert.Contains(t, out, "error=boom")
		assert.Contains(t, out, "operation=taskq.Worker.fail")
		assert.Contains(t, out, "duration=")
		assert.NotContains(t, out, `msg="span finished"`)
	})

	t.Run("nil logger does not panic", func(t *testing.T) {
		t.Parallel()
		tracer := slogadapter.NewTracer(nil)
		_, span := tracer.Start(t.Context(), "taskq.Submit")
		span.End()
	})
}

func TestTracerStartPassesContextThrough(t *testing.T) {
	t.Parallel()
	buf, logger := newTestLogger(t)
	_ = buf
	tracer := slogadapter.NewTracer(logger)

	ctx := t.Context()
	returned, _ := tracer.Start(ctx, "taskq.Submit")
	assert.Equal(t, ctx, returned)
}
func TestMeter(t *testing.T) {
	t.Parallel()
	buf, logger := newTestLogger(t)
	meter := slogadapter.NewMeter(logger)
	ctx := t.Context()

	counter := meter.Counter("taskq.submitted", taskq.WithMetricDescription("Submitted jobs"))
	counter.Inc(ctx, taskq.MetricAttr{Key: "task", Value: "add"})
	counter.Add(ctx, 5, taskq.MetricAttr{Key: "task", Value: "add"})

	meter.Histogram("taskq.duration").Record(ctx, 42*time.Millisecond, taskq.MetricAttr{Key: "task", Value: "add"})
	meter.Gauge("taskq.inflight").Set(ctx, 3.5)

	out := buf.String()

	// Счетчик: Inc и Add — две строки.
	assert.Equal(t, 2, strings.Count(out, `msg="metric counter"`))
	assert.Contains(t, out, "name=taskq.submitted")
	assert.Contains(t, out, "value=1")
	assert.Contains(t, out, "value=5")
	assert.Contains(t, out, "task=add")

	// Гистограмма.
	assert.Contains(t, out, `msg="metric record"`)
	assert.Contains(t, out, "name=taskq.duration")
	assert.Contains(t, out, "value=42ms")

	// Датчик.
	assert.Contains(t, out, `msg="metric set"`)
	assert.Contains(t, out, "name=taskq.inflight")
	assert.Contains(t, out, "value=3.5")

	// Все записи — уровень Debug.
	assert.Equal(t, 4, strings.Count(out, "level=DEBUG"))
}

func TestMeterNilFallsBackToDefault(t *testing.T) {
	t.Parallel()

	// Не должно паниковать: nil заменяется на slog.Default().
	meter := slogadapter.NewMeter(nil)
	meter.Counter("x").Inc(t.Context())
	meter.Histogram("y").Record(t.Context(), time.Millisecond)
	meter.Gauge("z").Set(t.Context(), 1)
}

func TestImplementsInterfaces(t *testing.T) {
	t.Parallel()

	var logger taskq.Logger = slogadapter.NewLogger(nil)
	var tracer taskq.Tracer = slogadapter.NewTracer(nil)
	var meter taskq.Meter = slogadapter.NewMeter(nil)
	require.NotNil(t, logger)
	require.NotNil(t, tracer)
	require.NotNil(t, meter)
}
