package prometheus

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
)

// family возвращает MetricFamily по имени метрики из реестра.
func family(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == name {
			return mf
		}
	}
	t.Fatalf("метрика %q не найдена в реестре", name)
	return nil
}

// labelValue возвращает значение лейбла метрики по ключу.
func labelValue(t *testing.T, metric *dto.Metric, key string) string {
	t.Helper()

	for _, pair := range metric.GetLabel() {
		if pair.GetName() == key {
			return pair.GetValue()
		}
	}
	t.Fatalf("лейбл %q не найден", key)
	return ""
}

// metric0 возвращает единственную метрику family.
func metric0(t *testing.T, mf *dto.MetricFamily) *dto.Metric {
	t.Helper()

	require.Len(t, mf.GetMetric(), 1, "ожидалась ровно одна метрика в family")
	return mf.GetMetric()[0]
}

func TestMeter_Counter(t *testing.T) {
	t.Parallel()

	t.Run("inc and add sanitize name and use task label", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg)
		counter := meter.Counter("taskq.submitted")

		// act
		counter.Inc(t.Context(), taskq.MetricAttr{Key: "task", Value: "email"})
		counter.Add(t.Context(), 4, taskq.MetricAttr{Key: "task", Value: "email"})

		// assert
		mf := family(t, reg, "taskq_submitted")
		metric := metric0(t, mf)
		assert.InDelta(t, 5, metric.GetCounter().GetValue(), 1e-9)
		assert.Equal(t, "email", labelValue(t, metric, "task"))
		assert.Equal(t, "taskq metric taskq.submitted", mf.GetHelp())
	})

	t.Run("help from description and unit options", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg)

		// act
		counter := meter.Counter("jobs", taskq.WithMetricDescription("обработанные задачи"),
			taskq.WithMetricUnit("1"))
		counter.Inc(t.Context())

		// assert
		mf := family(t, reg, "jobs")
		assert.Equal(t, "обработанные задачи (unit: 1)", mf.GetHelp())
	})

	t.Run("repeated calls reuse the same vector", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg)

		// act
		meter.Counter("taskq.submitted").Inc(t.Context(), taskq.MetricAttr{Key: "task", Value: "a"})
		meter.Counter("taskq.submitted").Inc(t.Context(), taskq.MetricAttr{Key: "task", Value: "b"})

		// assert
		mf := family(t, reg, "taskq_submitted")
		require.Len(t, mf.GetMetric(), 2, "две метрики по лейблу task")
		assert.InDelta(t, 1, mf.GetMetric()[0].GetCounter().GetValue(), 1e-9)
		assert.InDelta(t, 1, mf.GetMetric()[1].GetCounter().GetValue(), 1e-9)
	})

	t.Run("external vector of same type is reused", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		// Desc должен полностью совпасть (имя + help + лейблы),
		// иначе Register отклонит регистрацию как конфликт дескрипторов.
		external := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskq_submitted",
			Help: "taskq metric taskq.submitted",
		}, []string{"task"})
		require.NoError(t, reg.Register(external))
		meter := NewMeter(reg)

		// act
		meter.Counter("taskq.submitted").Inc(t.Context(), taskq.MetricAttr{Key: "task", Value: "a"})

		// assert
		mf := family(t, reg, "taskq_submitted")
		metric := metric0(t, mf)
		assert.InDelta(t, 1, metric.GetCounter().GetValue(), 1e-9)
		assert.Equal(t, testutil.ToFloat64(external.WithLabelValues("a")), float64(1))
	})

	t.Run("name taken by another metric type panics", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		// Тот же fqName, те же лейблы и help, но другой тип —
		// регистрация пройдет, а каст к CounterVec нет.
		gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taskq_submitted",
			Help: "taskq metric taskq.submitted",
		}, []string{"task"})
		require.NoError(t, reg.Register(gauge))
		meter := NewMeter(reg)

		// act + assert
		assert.PanicsWithError(t,
			`taskq/contrib/prometheus: metric "taskq.submitted" is not a counter`,
			func() {
				meter.Counter("taskq.submitted")
			})
	})
}

func TestMeter_Histogram(t *testing.T) {
	t.Parallel()

	t.Run("record converts duration to seconds", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg)
		histogram := meter.Histogram("taskq.duration")

		// act
		histogram.Record(t.Context(), 1500*time.Millisecond, taskq.MetricAttr{Key: "task", Value: "resize"})

		// assert
		mf := family(t, reg, "taskq_duration")
		metric := metric0(t, mf)
		h := metric.GetHistogram()
		assert.Equal(t, uint64(1), h.GetSampleCount())
		assert.InDelta(t, 1.5, h.GetSampleSum(), 1e-9)
		assert.Equal(t, "resize", labelValue(t, metric, "task"))
	})
}

func TestMeter_Gauge(t *testing.T) {
	t.Parallel()

	t.Run("set keeps instant value", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg)
		gauge := meter.Gauge("taskq.inflight")

		// act
		gauge.Set(t.Context(), 3.5, taskq.MetricAttr{Key: "task", Value: "email"})

		// assert
		mf := family(t, reg, "taskq_inflight")
		metric := metric0(t, mf)
		assert.InDelta(t, 3.5, metric.GetGauge().GetValue(), 1e-9)
	})
}

func TestMeter_Namespace(t *testing.T) {
	t.Parallel()

	t.Run("namespace prefixes sanitized name", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg, WithNamespace("app"))

		// act
		meter.Counter("taskq.submitted").Inc(t.Context(), taskq.MetricAttr{Key: "task", Value: "a"})

		// assert
		mf := family(t, reg, "app_taskq_submitted")
		metric := metric0(t, mf)
		assert.InDelta(t, 1, metric.GetCounter().GetValue(), 1e-9)
	})
}

func TestMeter_Labels(t *testing.T) {
	t.Parallel()

	t.Run("configured labels map attrs and missing attrs become empty", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg, WithLabels("task", "queue"))

		// act
		meter.Counter("taskq.submitted").Inc(t.Context(),
			taskq.MetricAttr{Key: "task", Value: "email"},
			taskq.MetricAttr{Key: "queue", Value: "critical"},
			taskq.MetricAttr{Key: "ignored", Value: "value"},
		)
		meter.Counter("taskq.submitted").Inc(t.Context(),
			taskq.MetricAttr{Key: "task", Value: "email"},
		)

		// assert
		mf := family(t, reg, "taskq_submitted")
		require.Len(t, mf.GetMetric(), 2, "queue задает кардинальность")

		byQueue := map[string]float64{}
		for _, metric := range mf.GetMetric() {
			byQueue[labelValue(t, metric, "queue")] = metric.GetCounter().GetValue()
		}
		assert.InDelta(t, 1, byQueue["critical"], 1e-9)
		assert.InDelta(t, 1, byQueue[""], 1e-9, "отсутствующий атрибут — пустой лейбл")
	})

	t.Run("empty labels disable cardinality", func(t *testing.T) {
		t.Parallel()

		// arrange
		reg := prometheus.NewRegistry()
		meter := NewMeter(reg, WithLabels())

		// act
		meter.Counter("taskq.submitted").Inc(t.Context(), taskq.MetricAttr{Key: "task", Value: "ignored"})

		// assert
		mf := family(t, reg, "taskq_submitted")
		metric := metric0(t, mf)
		assert.Empty(t, metric.GetLabel(), "атрибуты не попадают в лейблы")
	})
}

func TestNewMeter_DefaultRegisterer(t *testing.T) {
	t.Parallel()

	t.Run("nil registerer falls back to default", func(t *testing.T) {
		t.Parallel()

		// arrange + act
		meter := NewMeter(nil)

		// assert
		assert.Equal(t, prometheus.DefaultRegisterer, meter.reg)
	})
}

func TestSanitizeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "dots become underscores", input: "taskq.submitted", want: "taskq_submitted"},
		{name: "hyphen becomes underscore", input: "my-metric", want: "my_metric"},
		{name: "slash becomes underscore", input: "a/b", want: "a_b"},
		{name: "colon is allowed", input: "ns:name", want: "ns:name"},
		{name: "digits inside are allowed", input: "task2fast", want: "task2fast"},
		{name: "leading digit gets underscore prefix", input: "42jobs", want: "_42jobs"},
		{name: "empty stays empty for optional parts", input: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// act
			got := sanitizeName(tc.input)

			// assert
			assert.Equal(t, tc.want, got)
		})
	}
}
