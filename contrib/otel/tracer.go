package otel

import (
	"context"
	"log/slog"
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/denisdubovitskiy/taskq"
)

// Tracer реализует taskq.Tracer поверх trace.Tracer из OpenTelemetry API.
type Tracer struct {
	inner trace.Tracer
}

var _ taskq.Tracer = (*Tracer)(nil)

// NewTracer создает трейсер поверх t (например, otel.Tracer("taskq")).
// Nil t заменяется на no-op трейсер: вызовы Start работают, но ничего
// не записывают — удобно для тестов и локальных запусков.
func NewTracer(t trace.Tracer) *Tracer {
	if t == nil {
		t = noop.NewTracerProvider().Tracer("github.com/denisdubovitskiy/taskq/contrib/otel")
	}
	return &Tracer{inner: t}
}

// Start создает span и возвращает контекст с прикрепленным span:
// вложенные Start образуют дерево трейса.
func (t *Tracer) Start(ctx context.Context, operation string) (context.Context, taskq.Span) {
	ctx, inner := t.inner.Start(ctx, operation)
	return ctx, &span{inner: inner}
}

// span — спан taskq.Span поверх trace.Span из OpenTelemetry API.
type span struct {
	inner trace.Span
}

var _ taskq.Span = (*span)(nil)

// End завершает span.
func (s *span) End() {
	s.inner.End()
}

// SetError записывает событие exception и переводит span в статус Error.
// Nil-ошибка игнорируется: статус span не меняется.
func (s *span) SetError(err error) {
	if err == nil {
		return
	}
	s.inner.RecordError(err)
	s.inner.SetStatus(codes.Error, err.Error())
}

// SetAttributes конвертирует slog.Attr в атрибуты OpenTelemetry.
func (s *span) SetAttributes(attrs ...slog.Attr) {
	converted := toKeyValues(attrs)
	if len(converted) == 0 {
		return
	}
	s.inner.SetAttributes(converted...)
}

// toKeyValues конвертирует slog.Attr в attribute.KeyValue.
// Атрибуты с пустым ключом пропускаются, группы разворачиваются
// с префиксом "group.key".
func toKeyValues(attrs []slog.Attr) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		out = appendKeyValues(out, "", attr)
	}
	return out
}

// appendKeyValues добавляет конвертированный атрибут с учетом префикса
// родительской группы.
func appendKeyValues(out []attribute.KeyValue, prefix string, attr slog.Attr) []attribute.KeyValue {
	if attr.Key == "" && attr.Value.Kind() != slog.KindGroup {
		return out
	}
	key := attr.Key
	if prefix != "" {
		key = prefix + "." + key
	}

	if attr.Value.Kind() == slog.KindGroup {
		group := attr.Value.Group()
		for _, nested := range group {
			out = appendKeyValues(out, key, nested)
		}
		return out
	}
	return append(out, attribute.KeyValue{Key: attribute.Key(key), Value: toValue(attr.Value)})
}

// toValue конвертирует slog.Value в attribute.Value: примитивы — по типу,
// остальное — строкой (slog.Value.String).
func toValue(v slog.Value) attribute.Value {
	switch v.Kind() {
	case slog.KindString:
		return attribute.StringValue(v.String())
	case slog.KindInt64:
		return attribute.Int64Value(v.Int64())
	case slog.KindUint64:
		if u := v.Uint64(); u <= math.MaxInt64 {
			return attribute.Int64Value(int64(u))
		}
		return attribute.StringValue(v.String())
	case slog.KindFloat64:
		return attribute.Float64Value(v.Float64())
	case slog.KindBool:
		return attribute.BoolValue(v.Bool())
	case slog.KindDuration:
		// Длительность строкой ("24.416µs") — единый формат с логами
		// ядра (slog.Duration рендерится так же).
		return attribute.StringValue(v.Duration().String())
	case slog.KindTime:
		return attribute.StringValue(v.Time().Format(time.RFC3339Nano))
	default:
		return attribute.StringValue(v.String())
	}
}
