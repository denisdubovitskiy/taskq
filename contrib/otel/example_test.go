package otel_test

import (
	"go.opentelemetry.io/otel"

	oteltracer "github.com/denisdubovitskiy/taskq/contrib/otel"
)

// ExampleNewTracer показывает подключение Tracer к клиенту и воркеру.
func ExampleNewTracer() {
	tracer := oteltracer.NewTracer(otel.Tracer("taskq"))

	// Один Tracer подключается и к клиенту, и к воркеру:
	//
	//	client, _ := taskq.NewClient(broker, backend,
	//		taskq.WithTracer(tracer))
	//	worker, _ := taskq.NewWorker(registry, broker, backend,
	//		taskq.WithWorkerTracer(tracer))
	//
	// Ядро эмитит спаны taskq.Submit / taskq.SubmitGroup / taskq.SubmitChord /
	// taskq.Chain.Send (клиент) и taskq.Worker.process /
	// taskq.Worker.fail (воркер); ошибки задают статус Error
	// и событие exception.
	_ = tracer
}
