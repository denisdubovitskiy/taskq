package taskq

import (
	"context"
	"log/slog"
	"time"
)

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, name string) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End()                       {}
func (noopSpan) SetError(error)             {}
func (noopSpan) SetAttributes(...slog.Attr) {}

type noopMeter struct{}

func (noopMeter) Counter(string, ...MetricOption) Counter     { return noopCounter{} }
func (noopMeter) Histogram(string, ...MetricOption) Histogram { return noopHistogram{} }
func (noopMeter) Gauge(string, ...MetricOption) Gauge         { return noopGauge{} }

type noopCounter struct{}

func (noopCounter) Add(context.Context, int64, ...MetricAttr) {}
func (noopCounter) Inc(context.Context, ...MetricAttr)        {}

type noopHistogram struct{}

func (noopHistogram) Record(context.Context, time.Duration, ...MetricAttr) {}

type noopGauge struct{}

func (noopGauge) Set(context.Context, float64, ...MetricAttr) {}
