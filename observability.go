package taskq

import (
	"context"
	"log/slog"
	"time"
)

// Logger — минимальный интерфейс для структурированного логирования.
// Адаптеры для zap/slog реализуют его в одну строку.
type Logger interface {
	Debug(msg string, attrs ...any)
	Info(msg string, attrs ...any)
	Warn(msg string, attrs ...any)
	Error(msg string, attrs ...any)
}

// Tracer — абстракция распределенного трейсинга.
type Tracer interface {
	Start(ctx context.Context, operation string) (context.Context, Span)
}

// Span — единица трейсинга.
type Span interface {
	End()
	SetError(err error)
	SetAttributes(attrs ...slog.Attr)
}

// Meter — фабрика метрик.
type Meter interface {
	Counter(name string, opts ...MetricOption) Counter
	Histogram(name string, opts ...MetricOption) Histogram
	Gauge(name string, opts ...MetricOption) Gauge
}

// Counter — счетчик метрик.
type Counter interface {
	Add(ctx context.Context, n int64, attrs ...MetricAttr)
	Inc(ctx context.Context, attrs ...MetricAttr)
}

// Histogram — гистограмма длительностей/значений.
type Histogram interface {
	Record(ctx context.Context, d time.Duration, attrs ...MetricAttr)
}

// Gauge — метрика мгновенного значения.
type Gauge interface {
	Set(ctx context.Context, value float64, attrs ...MetricAttr)
}

// MetricAttr — атрибут метрики.
type MetricAttr struct {
	Key   string
	Value string
}

// MetricOption — опция создания метрики.
type MetricOption func(*metricConfig)

type metricConfig struct {
	description string
	unit        string
}

// WithMetricDescription задает описание метрики.
func WithMetricDescription(desc string) MetricOption {
	return func(c *metricConfig) {
		c.description = desc
	}
}

// WithMetricUnit задает единицу измерения метрики.
func WithMetricUnit(unit string) MetricOption {
	return func(c *metricConfig) {
		c.unit = unit
	}
}

// MetricSpec — результат применения опций метрики.
// Нужен внешним реализациям Meter: тип metricConfig неэкспортируем,
// поэтому применить опции напрямую вне пакета taskq нельзя.
type MetricSpec struct {
	// Description — человекочитаемое описание метрики.
	Description string
	// Unit — единица измерения метрики (например, "s").
	Unit string
}

// ApplyMetricOptions применяет опции метрики и возвращает их итоговую
// спецификацию. Адаптеры Meter вызывают её, чтобы достать описание и юнит.
func ApplyMetricOptions(opts ...MetricOption) MetricSpec {
	var cfg metricConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return MetricSpec{Description: cfg.description, Unit: cfg.unit}
}
