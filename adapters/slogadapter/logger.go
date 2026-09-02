// Package slogadapter реализует наблюдаемость taskq (Logger, Tracer, Meter)
// на стандартной библиотеке log/slog.
//
// Референс-адаптер для локальной разработки и отладки: без внешних
// зависимостей. Спаны трейсера превращаются в лог-строки с длительностью,
// метрики — в событийные строки уровня Debug.
//
// Пример использования:
//
//	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
//
//	client, err := taskq.NewClient(broker, backend,
//		taskq.WithLogger(slogadapter.NewLogger(logger)),
//		taskq.WithTracer(slogadapter.NewTracer(logger)),
//		taskq.WithMeter(slogadapter.NewMeter(logger)),
//	)
//	if err != nil {
//		// обработка
//	}
//
//	worker, err := taskq.NewWorker(registry, broker, backend,
//		taskq.WithWorkerLogger(slogadapter.NewLogger(logger)),
//		taskq.WithWorkerTracer(slogadapter.NewTracer(logger)),
//		taskq.WithWorkerMeter(slogadapter.NewMeter(logger)),
//	)
//	if err != nil {
//		// обработка
//	}
package slogadapter

import (
	"log/slog"

	"github.com/denisdubovitskiy/taskq"
)

// Logger реализует taskq.Logger поверх *slog.Logger.
// Ключ-значения из вызовов ядра передаются в slog без изменений.
type Logger struct {
	inner *slog.Logger
}

var _ taskq.Logger = (*Logger)(nil)

// NewLogger оборачивает *slog.Logger.
// Nil-аргумент заменяется на slog.Default().
func NewLogger(inner *slog.Logger) *Logger {
	return &Logger{inner: orDefault(inner)}
}

// Debug пишет запись уровня slog.LevelDebug.
func (l *Logger) Debug(msg string, attrs ...any) { l.inner.Debug(msg, attrs...) }

// Info пишет запись уровня slog.LevelInfo.
func (l *Logger) Info(msg string, attrs ...any) { l.inner.Info(msg, attrs...) }

// Warn пишет запись уровня slog.LevelWarn.
func (l *Logger) Warn(msg string, attrs ...any) { l.inner.Warn(msg, attrs...) }

// Error пишет запись уровня slog.LevelError.
func (l *Logger) Error(msg string, attrs ...any) { l.inner.Error(msg, attrs...) }

// orDefault возвращает переданный логгер либо slog.Default() для nil.
func orDefault(inner *slog.Logger) *slog.Logger {
	if inner == nil {
		return slog.Default()
	}
	return inner
}
