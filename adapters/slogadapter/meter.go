package slogadapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/denisdubovitskiy/taskq"
)

// Meter реализует taskq.Meter поверх *slog.Logger.
// Каждое обновление метрики — событийная строка уровня Debug
// («metric counter», «metric record», «metric set»).
//
// Опции метрик (WithMetricDescription, WithMetricUnit) адаптер не использует:
// они несут метаданные для реестровых реализаций, вроде prometheus.
type Meter struct {
	inner *slog.Logger
}

var _ taskq.Meter = (*Meter)(nil)

// NewMeter создает slog-метр.
// Nil-аргумент заменяется на slog.Default().
func NewMeter(inner *slog.Logger) *Meter {
	return &Meter{inner: orDefault(inner)}
}

// Counter возвращает счетчик: каждый Add/Inc пишет «metric counter».
func (m *Meter) Counter(name string, opts ...taskq.MetricOption) taskq.Counter {
	return &counter{logger: m.inner, name: name}
}

// Histogram возвращает гистограмму: каждый Record пишет «metric record».
func (m *Meter) Histogram(name string, opts ...taskq.MetricOption) taskq.Histogram {
	return &histogram{logger: m.inner, name: name}
}

// Gauge возвращает датчик: каждый Set пишет «metric set».
func (m *Meter) Gauge(name string, opts ...taskq.MetricOption) taskq.Gauge {
	return &gauge{logger: m.inner, name: name}
}

type counter struct {
	logger *slog.Logger
	name   string
}

var _ taskq.Counter = (*counter)(nil)

// Add пишет значение инкремента счетчика.
func (c *counter) Add(ctx context.Context, n int64, attrs ...taskq.MetricAttr) {
	c.logger.LogAttrs(ctx, slog.LevelDebug, "metric counter",
		append(metricAttrs(c.name, slog.Int64("value", n)), toSlogAttrs(attrs)...)...)
}

// Inc инкрементирует счетчик на единицу.
func (c *counter) Inc(ctx context.Context, attrs ...taskq.MetricAttr) {
	c.Add(ctx, 1, attrs...)
}

type histogram struct {
	logger *slog.Logger
	name   string
}

var _ taskq.Histogram = (*histogram)(nil)

// Record пишет измерение.
func (h *histogram) Record(ctx context.Context, d time.Duration, attrs ...taskq.MetricAttr) {
	h.logger.LogAttrs(ctx, slog.LevelDebug, "metric record",
		append(metricAttrs(h.name, slog.Duration("value", d)), toSlogAttrs(attrs)...)...)
}

type gauge struct {
	logger *slog.Logger
	name   string
}

var _ taskq.Gauge = (*gauge)(nil)

// Set пишет текущее значение.
func (g *gauge) Set(ctx context.Context, value float64, attrs ...taskq.MetricAttr) {
	g.logger.LogAttrs(ctx, slog.LevelDebug, "metric set",
		append(metricAttrs(g.name, slog.Float64("value", value)), toSlogAttrs(attrs)...)...)
}

// metricAttrs собирает базовые атрибуты записи (имя метрики + значение).
func metricAttrs(name string, value slog.Attr) []slog.Attr {
	return []slog.Attr{slog.String("name", name), value}
}

// toSlogAttrs конвертирует taskq.MetricAttr (строковые ключ/значение) в slog.Attr.
func toSlogAttrs(attrs []taskq.MetricAttr) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, slog.String(a.Key, a.Value))
	}
	return out
}
