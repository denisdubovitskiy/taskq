# taskq contrib: otel

Реализация plugin-интерфейса `taskq.Tracer` для OpenTelemetry
([go.opentelemetry.io/otel](https://go.opentelemetry.io/otel)).

Отдельный Go-модуль: подключается только если нужен OTel,
ядро taskq не зависит от OTel API. SDK (экспортеры, сэмплеры) —
на стороне вызывающего кода; модуль зависит только от API-пакетов
(`trace`, `attribute`, `codes`).

## Подключение

```bash
go get github.com/denisdubovitskiy/taskq/contrib/otel
```

Пока ядро не опубликовано в proxy, укажите локальный путь в `replace`:

```
replace github.com/denisdubovitskiy/taskq => /путь/к/taskq
```

## Использование

```go
package main

import (
	"go.opentelemetry.io/otel"
	oteltracer "github.com/denisdubovitskiy/taskq/contrib/otel"

	"github.com/denisdubovitskiy/taskq"
)

func main() {
	// otel.Tracer берется из глобального провайдера; настройте
	// провайдер с экспортером (OTLP, stdout, …) до старта taskq.
	tracer := oteltracer.NewTracer(otel.Tracer("taskq"))

	client, err := taskq.NewClient(broker, backend,
		taskq.WithTracer(tracer))
	if err != nil {
		panic(err)
	}

	worker, err := taskq.NewWorker(registry, broker, backend,
		taskq.WithWorkerTracer(tracer))
	if err != nil {
		panic(err)
	}
}
```

Nil вместо `trace.Tracer` — безопасный no-op (удобно для тестов).

## Спаны ядра

| Операция | Где | Примечания |
|----------|-----|------------|
| `taskq.Submit` | client | encode → state → publish |
| `taskq.SubmitGroup` | client | публикация всей группы |
| `taskq.SubmitChord` | client | chord init + группа + callback |
| `taskq.Chain.Send` | client | публикация первого шага |
| `taskq.Worker.process` | worker | вся доставка: состояния, хук, обработчик |
| `taskq.Worker.fail` | worker | failure-путь одной попытки |

Неуспешные пути вызывают `Span.SetError(err)` — адаптер записывает
событие `exception` (`RecordError`) и переводит спан в статус `Error`.

## Модель соответствия

- **Start** создает span с именем операции и возвращает контекст,
  к которому span уже прикреплен — вложенные вызовы образуют дерево трейса.
- **Атрибуты**: `slog.Attr` конвертируются в `attribute.KeyValue`:
  `string`/`int64`/`uint64`/`float64`/`bool`/`time.Duration`/`time.Time` —
  по типу (длительность — строкой, как в логах ядра), прочие значения —
  строкой через `slog.Value.String`. Группы разворачиваются с префиксом
  `group.key`; атрибуты с пустым ключом пропускаются.
- **Nil-трейсер** заменяется на no-op из `go.opentelemetry.io/otel/trace/noop`.

## Ограничение: propagation

Ядро taskq пока не переносит trace-контекст через брокер: span клиента
(`taskq.Submit`) и span воркера (`taskq.Worker.process`) — независимые
корни трейса. Распределенный трейсинг появится вместе с W3C
traceparent-пропагацией в job-пейлоаде.

## Разработка

Тесты — обычные юнит-тесты на SDK SpanRecorder, внешние сервисы не нужны:

```bash
go test ./...
```
