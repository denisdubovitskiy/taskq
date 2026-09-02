package prometheus_test

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	prometheusmeter "github.com/denisdubovitskiy/taskq/contrib/prometheus"
)

// ExampleNewMeter показывает подключение Meter и экспорт реестра
// на стандартный эндпоинт /metrics.
func ExampleNewMeter() {
	reg := prometheus.NewRegistry()
	meter := prometheusmeter.NewMeter(reg)

	// Один Meter подключается и к клиенту, и к воркеру:
	//
	//	client, _ := taskq.NewClient(broker, backend, taskq.WithMeter(meter))
	//	worker, _ := taskq.NewWorker(regTasks, broker, backend,
	//		taskq.WithWorkerMeter(meter))
	//
	// Ядро эмитит счетчики taskq_submitted / taskq_started /
	// taskq_succeeded / taskq_failed / taskq_dead / taskq_canceled
	// и гистограмму taskq_duration (в секундах) с лейблом task.
	_ = meter

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	// _ = http.ListenAndServe(":9090", nil)
}
