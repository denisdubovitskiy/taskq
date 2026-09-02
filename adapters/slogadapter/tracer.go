package slogadapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/denisdubovitskiy/taskq"
)

// Tracer реализует taskq.Tracer поверх *slog.Logger.
// Каждый спан — пара лог-строк: старт уровня Debug, завершение уровня Debug,
// завершение с ошибкой — уровня Warn. Завершающая строка содержит длительность
// спана и атрибуты, переданные через SetAttributes.
type Tracer struct {
	inner *slog.Logger
}

var _ taskq.Tracer = (*Tracer)(nil)

// NewTracer создает slog-трейсер.
// Nil-аргумент заменяется на slog.Default().
func NewTracer(inner *slog.Logger) *Tracer {
	return &Tracer{inner: orDefault(inner)}
}

// Start создает спан с именем операции и логирует его старт.
// Контекст возвращается без изменений: нести по нему нечего.
func (t *Tracer) Start(ctx context.Context, operation string) (context.Context, taskq.Span) {
	t.inner.Debug("span started", "operation", operation)
	return ctx, &span{
		logger:    t.inner,
		operation: operation,
		start:     time.Now(),
	}
}

// span — реализация taskq.Span как лог-строк.
// Методы вызываются из горутины, создавшей спан (паттерн ядра:
// SetAttributes/SetError — до отложенного End).
type span struct {
	logger    *slog.Logger
	operation string
	start     time.Time
	attrs     []slog.Attr
	err       error
	ended     bool
}

var _ taskq.Span = (*span)(nil)

// SetAttributes запоминает атрибуты для завершающей записи.
func (s *span) SetAttributes(attrs ...slog.Attr) {
	s.attrs = append(s.attrs, attrs...)
}

// SetError помечает спан как неудачный.
func (s *span) SetError(err error) {
	s.err = err
}

// End логирует завершение спана с длительностью.
// Идемпотентен: повторные вызовы игнорируются.
func (s *span) End() {
	if s.ended {
		return
	}
	s.ended = true

	attrs := append(s.attrs,
		slog.String("operation", s.operation),
		slog.Duration("duration", time.Since(s.start)),
	)

	if s.err != nil {
		s.logger.LogAttrs(context.Background(), slog.LevelWarn, "span failed",
			append(attrs, slog.Any("error", s.err))...)
		return
	}
	s.logger.LogAttrs(context.Background(), slog.LevelDebug, "span finished", attrs...)
}
