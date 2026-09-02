// Package otel — реализация taskq.Tracer для OpenTelemetry
// (go.opentelemetry.io/otel).
//
// Tracer оборачивает trace.Tracer из OTel API; SDK (экспортер, сэмплер)
// подключает вызывающий код — библиотека не тянет SDK-зависимости.
//
// Соответствие модели taskq модели OpenTelemetry:
//
//   - Start создает span с именем операции ("taskq.Submit",
//     "taskq.Worker.process", …) и возвращает контекст, к которому span
//     уже прикреплен — вложенные вызовы образуют дерево трейса;
//   - Span.End завершает span;
//   - Span.SetError(err) записывает событие exception (RecordError)
//     и переводит span в статус Error (codes.Error);
//   - Span.SetAttributes конвертирует slog.Attr в attribute.KeyValue:
//     string/int64/uint64/float64/bool/time.Duration/time.Time/error —
//     по типу, прочие значения — строкой (slog.Value.String);
//     группы разворачиваются с префиксом "group.key";
//     атрибуты с пустым ключом пропускаются.
//
// Внимание: propagation trace-контекста через брокер ядром taskq пока
// не выполняется — span клиента (taskq.Submit) и span воркера
// (taskq.Worker.process) оказываются независимыми корнями трейса.
// Распределенный трейсинг появится вместе с propagation в ядре.
package otel
