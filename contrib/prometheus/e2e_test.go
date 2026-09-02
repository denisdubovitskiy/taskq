package prometheus_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
	membackend "github.com/denisdubovitskiy/taskq/backends/memory"
	membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
	prometheusmeter "github.com/denisdubovitskiy/taskq/contrib/prometheus"
)

// e2eArgs — аргументы тестовой задачи.
type e2eArgs struct {
	N int `json:"n"`
}

// TestE2E_WorkerMetrics проверяет сквозной сценарий: клиент и воркер,
// подключенные через Meter, эмитят метрики ядра в реестр Prometheus —
// submitted/started/succeeded инкрементируются, duration наблюдается в секундах.
func TestE2E_WorkerMetrics(t *testing.T) {
	t.Parallel()

	// arrange
	reg := prometheus.NewRegistry()
	meter := prometheusmeter.NewMeter(reg)

	broker := membroker.NewBroker()
	backend := membackend.NewBackend()

	double := taskq.NewTask[e2eArgs, int]("double")
	regTasks := taskq.NewRegistry()
	require.NoError(t, taskq.Register(regTasks, double, func(_ context.Context, a e2eArgs) (int, error) {
		return a.N * 2, nil
	}))

	worker, err := taskq.NewWorker(regTasks, broker, backend,
		taskq.WithWorkerMeter(meter))
	require.NoError(t, err)

	client, err := taskq.NewClient(broker, backend,
		taskq.WithMeter(meter))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	go func() {
		_ = worker.Run(ctx, "")
	}()
	t.Cleanup(func() { _ = worker.Shutdown(context.Background()) })

	// act
	future, err := taskq.Submit[e2eArgs, int](ctx, client, double, e2eArgs{N: 21})
	require.NoError(t, err)

	res, err := future.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, 42, res)

	// assert
	families, err := reg.Gather()
	require.NoError(t, err)

	values := map[string]float64{}
	histograms := map[string]uint64{}
	for _, mf := range families {
		for _, metric := range mf.GetMetric() {
			switch {
			case metric.GetCounter() != nil:
				values[mf.GetName()] += metric.GetCounter().GetValue()
			case metric.GetHistogram() != nil:
				histograms[mf.GetName()] += metric.GetHistogram().GetSampleCount()
			}
		}
	}

	const wantTasks = 1.0
	assert.Equal(t, wantTasks, values["taskq_submitted"], "submit должен инкрементировать taskq_submitted")
	assert.Equal(t, wantTasks, values["taskq_started"], "запуск хендлера должен инкрементировать taskq_started")
	assert.Equal(t, wantTasks, values["taskq_succeeded"], "успех должен инкрементировать taskq_succeeded")
	assert.Equal(t, uint64(1), histograms["taskq_duration"], "длительность должна наблюдаться один раз")
}
