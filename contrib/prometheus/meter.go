package prometheus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/denisdubovitskiy/taskq"
)

// defaultLabel — лейбл, который ядро taskq прикладывает ко всем метрикам.
const defaultLabel = "task"

// unnamedMetric — замена пустого имени метрики после санитизации.
const unnamedMetric = "unnamed"

// Meter реализует taskq.Meter поверх prometheus.Registerer.
type Meter struct {
	reg       prometheus.Registerer
	namespace string
	labels    []string

	// vectors — кэш зарегистрированных векторов по исходному имени метрики
	// (map[string]prometheus.Collector). Без кэша каждое обращение ядра
	// к Counter/Histogram/Gauge пыталось бы зарегистрировать метрику заново.
	vectors sync.Map
}

var _ taskq.Meter = (*Meter)(nil)

// Option — опция Meter.
type Option func(*meterConfig)

type meterConfig struct {
	namespace string
	labels    []string
	// labelsSet отличает явно заданный пустой набор лейблов
	// (WithLabels() без аргументов) от умолчания.
	labelsSet bool
}

// WithNamespace задает префикс имен метрик: namespace "app" превращает
// "taskq.submitted" в "app_taskq_submitted". Пустой префикс не используется.
func WithNamespace(namespace string) Option {
	return func(c *meterConfig) {
		c.namespace = namespace
	}
}

// WithLabels задает набор лейблов всех метрик Meter. Атрибуты метрики
// с ключами из этого набора становятся значениями лейблов; остальные
// атрибуты игнорируются. По умолчанию ["task"] — единственный атрибут,
// который эмитит ядро taskq. Вызов без аргументов отключает лейблы
// (все метрики без кардинальности).
func WithLabels(labels ...string) Option {
	return func(c *meterConfig) {
		c.labels = labels
		c.labelsSet = true
	}
}

// NewMeter создает Meter, регистрирующий метрики в reg.
// Nil reg заменяется на prometheus.DefaultRegisterer.
func NewMeter(reg prometheus.Registerer, opts ...Option) *Meter {
	var cfg meterConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	labels := cfg.labels
	if !cfg.labelsSet {
		labels = []string{defaultLabel}
	}
	return &Meter{
		reg:       reg,
		namespace: cfg.namespace,
		labels:    labels,
	}
}

// Counter возвращает счетчик Prometheus по имени.
// Повторные вызовы с тем же именем возвращают один и тот же вектор.
func (m *Meter) Counter(name string, opts ...taskq.MetricOption) taskq.Counter {
	vec := m.metricVec(name, opts, func(help string) prometheus.Collector {
		return prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: m.fqName(name),
			Help: help,
		}, m.labels)
	})
	counterVec, ok := vec.(*prometheus.CounterVec)
	if !ok {
		panic(fmt.Errorf("taskq/contrib/prometheus: metric %q is not a counter", name))
	}
	return &counter{vec: counterVec, labels: m.labels}
}

// Histogram возвращает гистограмму Prometheus по имени.
// Record переводит time.Duration в секунды (Observe(d.Seconds())).
func (m *Meter) Histogram(name string, opts ...taskq.MetricOption) taskq.Histogram {
	vec := m.metricVec(name, opts, func(help string) prometheus.Collector {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: m.fqName(name),
			Help: help,
		}, m.labels)
	})
	histogramVec, ok := vec.(*prometheus.HistogramVec)
	if !ok {
		panic(fmt.Errorf("taskq/contrib/prometheus: metric %q is not a histogram", name))
	}
	return &histogram{vec: histogramVec, labels: m.labels}
}

// Gauge возвращает датчик Prometheus по имени.
func (m *Meter) Gauge(name string, opts ...taskq.MetricOption) taskq.Gauge {
	vec := m.metricVec(name, opts, func(help string) prometheus.Collector {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: m.fqName(name),
			Help: help,
		}, m.labels)
	})
	gaugeVec, ok := vec.(*prometheus.GaugeVec)
	if !ok {
		panic(fmt.Errorf("taskq/contrib/prometheus: metric %q is not a gauge", name))
	}
	return &gauge{vec: gaugeVec, labels: m.labels}
}

// metricVec возвращает вектор метрики по имени: из кэша, либо создает
// и регистрирует новый. Если имя уже занято вектором того же типа
// (например, другой Meter на том же Registerer) — возвращает существующий.
// Имя, занятое метрикой другого типа, и ошибки регистрации — паника:
// это ошибка конфигурации, а не исполнения.
func (m *Meter) metricVec(name string, opts []taskq.MetricOption, newVec func(help string) prometheus.Collector) prometheus.Collector {
	if cached, ok := m.vectors.Load(name); ok {
		return cached.(prometheus.Collector)
	}
	vec := newVec(metricHelp(name, taskq.ApplyMetricOptions(opts...)))
	if err := m.reg.Register(vec); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			// Имя уже занято вектором с идентичным дескриптором (например,
			// другим Meter на том же Registerer) — переиспользуем его.
			m.vectors.Store(name, are.ExistingCollector)
			return are.ExistingCollector
		}
		panic(fmt.Errorf("taskq/contrib/prometheus: register metric %q: %w", m.fqName(name), err))
	}
	m.vectors.Store(name, vec)
	return vec
}

// fqName собирает итоговое имя метрики: namespace + санитизированное имя.
// Пустой namespace не участвует в имени; пустое имя превращается в "unnamed".
func (m *Meter) fqName(name string) string {
	sanitized := sanitizeName(name)
	if sanitized == "" {
		sanitized = unnamedMetric
	}
	return prometheus.BuildFQName(sanitizeName(m.namespace), "", sanitized)
}

// counter — счетчик taskq.Counter поверх prometheus.CounterVec.
type counter struct {
	vec    *prometheus.CounterVec
	labels []string
}

var _ taskq.Counter = (*counter)(nil)

// Add прибавляет n к счетчику с лейблами из attrs.
func (c *counter) Add(_ context.Context, n int64, attrs ...taskq.MetricAttr) {
	c.vec.WithLabelValues(labelValues(c.labels, attrs)...).Add(float64(n))
}

// Inc инкрементирует счетчик с лейблами из attrs.
func (c *counter) Inc(_ context.Context, attrs ...taskq.MetricAttr) {
	c.vec.WithLabelValues(labelValues(c.labels, attrs)...).Inc()
}

// histogram — гистограмма taskq.Histogram поверх prometheus.HistogramVec.
type histogram struct {
	vec    *prometheus.HistogramVec
	labels []string
}

var _ taskq.Histogram = (*histogram)(nil)

// Record регистрирует наблюдение; time.Duration переводится в секунды.
func (h *histogram) Record(_ context.Context, d time.Duration, attrs ...taskq.MetricAttr) {
	h.vec.WithLabelValues(labelValues(h.labels, attrs)...).Observe(d.Seconds())
}

// gauge — датчик taskq.Gauge поверх prometheus.GaugeVec.
type gauge struct {
	vec    *prometheus.GaugeVec
	labels []string
}

var _ taskq.Gauge = (*gauge)(nil)

// Set устанавливает мгновенное значение с лейблами из attrs.
func (g *gauge) Set(_ context.Context, value float64, attrs ...taskq.MetricAttr) {
	g.vec.WithLabelValues(labelValues(g.labels, attrs)...).Set(value)
}

// labelValues собирает значения лейблов из атрибутов: для каждого ключа
// labels берется первый подходящий атрибут, отсутствующий атрибут
// дает пустую строку. Порядок значений совпадает с порядком labels.
func labelValues(labels []string, attrs []taskq.MetricAttr) []string {
	values := make([]string, len(labels))
	for i, key := range labels {
		values[i] = attrValue(attrs, key)
	}
	return values
}

// attrValue ищет значение атрибута по ключу; не найдено — пустая строка.
func attrValue(attrs []taskq.MetricAttr, key string) string {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value
		}
	}
	return ""
}

// metricHelp строит Help метрики из описания и юнита.
// Юнит дописывается текстом: в Prometheus юниты принято вшивать в имя,
// а имена метрик taskq менять нельзя — их ждут дашборды пользователя.
func metricHelp(name string, spec taskq.MetricSpec) string {
	switch {
	case spec.Description != "" && spec.Unit != "":
		return spec.Description + " (unit: " + spec.Unit + ")"
	case spec.Description != "":
		return spec.Description
	default:
		return "taskq metric " + name
	}
}

// sanitizeName приводит имя к правилам Prometheus ([a-zA-Z_:][a-zA-Z0-9_:]*):
// недопустимые символы заменяются на "_", ведущая цифра получает префикс "_".
func sanitizeName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r == ':',
			r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s != "" && s[0] >= '0' && s[0] <= '9' {
		return "_" + s
	}
	return s
}
