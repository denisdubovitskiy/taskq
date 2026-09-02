# taskq contrib: prometheus

Реализация plugin-интерфейса `taskq.Meter` для Prometheus
([prometheus/client_golang](https://github.com/prometheus/client_golang)).

Отдельный Go-модуль: подключается только если нужен Prometheus,
ядро taskq не зависит от клиента метрик.

## Подключение

```bash
go get github.com/denisdubovitskiy/taskq/contrib/prometheus
```

Пока ядро не опубликовано в proxy, укажите локальный путь в `replace`:

```
replace github.com/denisdubovitskiy/taskq => /путь/к/taskq
```

## Использование

```go
package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/denisdubovitskiy/taskq"
	prometheusmeter "github.com/denisdubovitskiy/taskq/contrib/prometheus"
)

func main() {
	reg := prometheus.NewRegistry()
	meter := prometheusmeter.NewMeter(reg)

	client, err := taskq.NewClient(broker, backend,
		taskq.WithMeter(meter))
	if err != nil {
		panic(err)
	}

	worker, err := taskq.NewWorker(registry, broker, backend,
		taskq.WithWorkerMeter(meter))
	if err != nil {
		panic(err)
	}

	// Экспорт реестра: GET /metrics.
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	_ = http.ListenAndServe(":9090", nil)
}
```

## Метрики ядра

Ядро эмитит (лейбл `task` — имя задачи):

| Метрика taskq | Имя Prometheus | Тип |
|---------------|----------------|-----|
| `taskq.submitted` | `taskq_submitted` | counter |
| `taskq.started` | `taskq_started` | counter |
| `taskq.succeeded` | `taskq_succeeded` | counter |
| `taskq.failed` | `taskq_failed` | counter |
| `taskq.dead` | `taskq_dead` | counter |
| `taskq.canceled` | `taskq_canceled` | counter |
| `taskq.rescued` | `taskq_rescued` | counter |
| `taskq.duration` | `taskq_duration` | histogram (секунды) |

## Модель соответствия

- **Имена**: точки и прочие символы, недопустимые в Prometheus, заменяются
  на `_` (`taskq.submitted` → `taskq_submitted`). Опция `WithNamespace("app")`
  добавляет префикс: `app_taskq_submitted`.
- **Лейблы**: `taskq.MetricAttr` становятся лейблами. Набор лейблов фиксирован
  на уровне Meter и по умолчанию `["task"]` — единственный атрибут, который
  эмитит ядро. `WithLabels("task", "queue")` расширяет набор: атрибуты с
  другими ключами игнорируются, отсутствующий атрибут дает пустое значение
  лейбла. `WithLabels()` без аргументов отключает кардинальность.
- **Опции метрик**: `taskq.WithMetricDescription` становится Help,
  `taskq.WithMetricUnit` дописывается в Help (`(unit: s)`): юниты в Prometheus
  принято вшивать в имя, а имена метрик ядра фиксированы.
- **Гистограммы**: `Histogram.Record` переводит `time.Duration` в секунды.
- **Создание**: метрики создаются лениво при первом обращении
  `Counter`/`Histogram`/`Gauge` и регистрируются в переданный
  `prometheus.Registerer` (`nil` → `prometheus.DefaultRegisterer`).
  Повторные обращения по тому же имени переиспользуют вектор, поэтому ядро
  может звать `Counter("taskq.submitted")` на каждую задачу без лишних
  регистраций.
- **Ошибки регистрации** (имя занято метрикой другого типа, дескриптор
  конфликтует) — паника с понятным сообщением, как в `promauto`: это ошибка
  конфигурации, а не исполнения задачи.

## Разработка

Тесты — обычные юнит-тесты, внешние сервисы не нужны:

```bash
go test ./...
```
