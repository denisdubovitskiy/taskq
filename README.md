# taskq

Современная Go-библиотека для фоновых задач с type-safe API.

## Особенности

- `Task[T, R]` — compile-time типизация аргументов и результата
- JSON-полезные нагрузки через обычные Go-структуры с тегами
- Плагин-интерфейсы: `Broker`, `Backend`, `Locker`, `Logger`, `Tracer`, `Meter`
- Ядро только на стандартной библиотеке Go
- Встроенные in-memory и файловые реализации для тестов
- Bounded concurrency, graceful shutdown, retry с экспоненциальным backoff
- Периодические задачи: `Scheduler` с `AddEvery` (интервал) и `AddCron` (5-полевые cron-выражения), защита от двойного запуска через `Locker`
- Оркестрация: группы (`SubmitGroup`), цепочки (`NewChain` + `Add`), аккорды (`SubmitChord`)

## Установка

```bash
go get github.com/denisdubovitskiy/taskq
```

### Contrib-модули (production-брокеры)

Production-реализации `Broker`/`Backend`/`Locker` живут в отдельных
Go-модулях — подключается только то, что нужно:

| Модуль | Транспорт |
|--------|-----------|
| [`contrib/redis`](./contrib/redis) | Redis Streams + consumer group |
| [`contrib/postgresql`](./contrib/postgresql) | PostgreSQL (river-style) |

```bash
go get github.com/denisdubovitskiy/taskq/contrib/redis
```

## Документация

Полная двухязычная документация собирается локально через MkDocs в
Poetry-окружении:

```bash
# один раз — установить зависимости (локальный venv создаётся в ./.venv)
make docs-deps

# собрать сайт
make docs

# или запустить dev-сервер
make docs-serve
```

Требуется установленный [Poetry](https://python-poetry.org/docs/#installation).
Сайт будет доступен по `http://localhost:8000` (EN по умолчанию, переключатель
языка в шапке). Исходники документации — в `docs/en/` и `docs/ru/`;
запускаемые примеры — в `docs/examples/`.

## Быстрый старт

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "time"

    "github.com/denisdubovitskiy/taskq"
    filebackend "github.com/denisdubovitskiy/taskq/backends/file"
    membroker "github.com/denisdubovitskiy/taskq/brokers/memory"
)

type AddArgs struct {
    A int `json:"a"`
    B int `json:"b"`
}

type AddResult struct {
    Sum int `json:"sum"`
}

func main() {
    broker := membroker.NewBroker()
    backend, err := filebackend.New("./task-results")
    if err != nil {
        panic(err)
    }

    client, err := taskq.NewClient(broker, backend)
    if err != nil {
        panic(err)
    }

    addTask := taskq.NewTask[AddArgs, AddResult]("add")

    registry := taskq.NewRegistry()
    if err := taskq.Register(registry, addTask, func(ctx context.Context, args AddArgs) (AddResult, error) {
        return AddResult{Sum: args.A + args.B}, nil
    }); err != nil {
        panic(err)
    }

    worker, err := taskq.NewWorker(registry, broker, backend)
    if err != nil {
        panic(err)
    }

    ctx := context.Background()

    workerCtx, stopWorker := context.WithCancel(ctx)
    defer stopWorker()

    go func() {
        if err := worker.Run(workerCtx, "default"); err != nil && !errors.Is(err, context.Canceled) {
            panic(err)
        }
    }()

    future, err := taskq.Submit(ctx, client, addTask, AddArgs{A: 2, B: 3})
    if err != nil {
        panic(err)
    }

    result, err := future.GetWithTimeout(ctx, 30*time.Second)
    if err != nil {
        panic(err)
    }

    fmt.Println(result.Sum) // 5

    // Graceful shutdown: останавливаем consume и ждем завершения активных задач.
    shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()
    if err := worker.Shutdown(shutdownCtx); err != nil {
        panic(err)
    }
}
```

## Оркестрация

### Группы (`SubmitGroup`)

Параллельное выполнение задач одного типа. Результаты приходят в исходном порядке;
ошибки отдельных задач не прерывают ожидание и фиксируются в `Errors`.

```go
// addTask *taskq.Task[AddArgs, AddResult]
group, err := taskq.SubmitGroup(ctx, client, addTask, []AddArgs{
    {A: 1, B: 2},
    {A: 3, B: 4},
    {A: 5, B: 6},
})
if err != nil {
    panic(err)
}

groupResult, err := group.GetWithTimeout(ctx, 30*time.Second)
if err != nil {
    panic(err)
}

if groupResult.AllSucceeded() {
    for i, r := range groupResult.Results {
        fmt.Println(i, r.Sum)
    }
} else {
    for i, e := range groupResult.Errors {
        if e != nil {
            fmt.Println(i, e)
        }
    }
}
```

### Цепочки (`NewChain` + `Add`)

Последовательное выполнение задач разных типов: результат каждого шага
десериализуется в payload следующего. Payload указывается только у первого шага.

```go
// scaleTask *taskq.Task[ScaleArgs, ScaleResult]
// shiftTask *taskq.Task[ScaleResult, ScaleResult]
builder, err := taskq.NewChain(client)
if err != nil {
    panic(err)
}

chainFuture, err := taskq.Add(
    taskq.Add(builder, scaleTask, ScaleArgs{Value: 21, Factor: 2}),
    shiftTask,
).Send(ctx)
if err != nil {
    panic(err)
}

result, err := chainFuture.GetWithTimeout(ctx, 30*time.Second)
if err != nil {
    panic(err)
}

fmt.Println(result.Value) // 52 = 21*2+10
```

Если шаг падает окончательно, цепочка прерывается: оставшиеся шаги
помечаются `StateFailure` с причиной, следующий шаг не публикуется.

### Аккорды (`SubmitChord`)

Группа задач одного типа + callback, выполняющийся после успешного завершения
всех задач группы. Payload callback-задачи — список результатов группы `[]R`
в исходном порядке. Если любая задача группы падает, callback не вызывается,
a его future возвращает ошибку.

```go
// addTask *taskq.Task[AddArgs, AddResult]
// sumAllTask *taskq.Task[[]AddResult, AddResult]
chordFuture, err := taskq.SubmitChord(ctx, client, addTask, []AddArgs{
    {A: 1, B: 2},
    {A: 3, B: 4},
}, sumAllTask)
if err != nil {
    panic(err)
}

result, err := chordFuture.GetWithTimeout(ctx, 30*time.Second)
if err != nil {
    panic(err)
}

fmt.Println(result.Sum) // 10 = 3+7
```

Аккорды требуют, чтобы backend реализовал `ChordBackend` — оба встроенных
backend (`backends/memory`, `backends/file`) поддерживают аккорды.

## Жизненный цикл задачи

Состояния: `pending → received → started → success | failure | dead | canceled`.

- **Таймаут выполнения** — `WithTimeout(d)` (от старта) или `WithDeadline(t)`
  (не позже момента). При превышении контекст задачи отменяется, ошибка
  `task timeout` попадает в ретраи по обычной политике.
- **Dead-letter** — если ретрай-политика задана и ретраи исчерпаны, задача
  переходит в `dead` (вместо `failure`). Хук `WithOnDeadHook` вызывается один
  раз. Без ретрай-политики окончательный сбой — `failure`.
- **Отмена** — `Client.Cancel(ctx, jobID)` отменяет задачу, еще не начатую;
  для выполняющейся в этом процессе — `Worker.Cancel(jobID)` отменяет ее
  контекст. Итоговое состояние — `canceled`, `Future.Get` возвращает
  `ErrJobCanceled`.
- **Rescue** — `Client.Rescue(ctx, jobID)` возвращает задачу из `failure`/`dead`
  в очередь: сбрасывает попытку и перепубликует документ. Требует, чтобы backend
  реализовывал `JobInspector` и `JobStore` (встроенные оба поддерживают).
- **Инспекция** — `Client.Inspect(ctx, jobID)` возвращает полный документ
  задачи с актуальным состоянием и последней ошибкой; `Client.List(ctx, q)` —
  фильтры по state/task + курсорная пагинация; `Client.Delete` — полное удаление.
- **Заголовки** — `WithHeader(key, value)`: произвольные ключи-значения задачи,
  видны в `Inspect` и передаются через брокер.
- **Future** — `Get`/`GetWithTimeout` ждут завершения задачи; `Touch` —
  неблокирующая проверка состояния (пока незавершена — zero-результат и state).
- **Идемпотентность** — `WithJobID(id)`: если задача с таким ID уже
  зарегистрирована, повторный `Submit` не публикует ее повторно и возвращает
  Future на существующую задачу.

```go
// Таймаут: задача не выполняется дольше 30 секунд.
future, err := taskq.Submit(ctx, client, addTask, AddArgs{A: 1, B: 2},
    taskq.WithTimeout(30*time.Second),
    taskq.WithRetry(taskq.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Second}),
)
if err != nil {
    panic(err)
}

// Dead-letter + rescue.
if err := client.Rescue(ctx, future.ID()); err != nil {
    fmt.Println(err) // только из states failure/dead
}

// Инспекция.
job, err := client.Inspect(ctx, future.ID())
if err == nil {
    fmt.Println(job.State, job.Error)
}

page, err := client.List(ctx, taskq.ListQuery{State: taskq.StateDead, Limit: 50})
if err == nil {
    for _, item := range page.Items {
        fmt.Println(item.ID, item.Task)
    }
}
```

## Масштабирование

### Per-task concurrency (`WithTaskConcurrency`)

По умолчанию у воркера один общий пул (`WithConcurrency`) — все задачи делят
его. Чтобы «тяжёлые» и «почтовые» задачи ходили с разными лимитами, задайте
лимит на задачу:

```go
worker, err := taskq.NewWorker(registry, broker, backend,
	taskq.WithConcurrency(8),           // общий потолок воркера
	taskq.WithTaskConcurrency("export", 2),
	taskq.WithTaskConcurrency("mail", 4),
)
```

Лимит задачи действует внутри общего пула: суммарная конкурентность по-прежнему
ограничена `WithConcurrency`, а задачи без явного лимита ограничены только им.

### Множественные очереди (`WithQueue`)

Очередь задаётся на задачу строителем `taskq.NewTask[T, R]("name").WithQueue("reports")`
(иначе — очередь по умолчанию). `WithQueue` в опциях сабмита переопределяет
очередь задачи на конкретный сабмит — для Submit, SubmitOneWay и оркестрации
одинаково:

```go
// Задача в специализированную очередь.
taskq.Submit(ctx, client, reportTask, args, taskq.WithQueue("reports"))
```

Воркер выбирает, какие очереди потреблять: `worker.Run(ctx, "reports")` — одну,
`worker.RunQueues(ctx, "default", "reports")` — несколько одновременно (все
очереди разделяют лимиты одного воркера). Типичная схема: дефолтная очередь +
специализированные под тяжёлые/приоритетные задачи.

## Периодические задачи (`Scheduler`)

Планировщик периодически отправляет задачи через тот же `Client`, что и обычный
`Submit` — ретраи, ETA, backend и оркестрация работают без изменений.

- `AddEvery` — фиксированный интервал; первый запуск — через `interval` после регистрации.
- `AddCron` — стандартное 5-полевое cron-выражение (`"0 * * * *"` — начало каждого часа);
  таймзона вычисления — UTC, меняется `WithCronTimezone(loc)`.
- `Schedule.NextRun()` — время следующего срабатывания, `Scheduler.Remove(name)` — динамическое удаление.
- **Персистентность** — если backend реализует `ScheduleStore` (оба встроенных — да),
  планировщик сохраняет документ с временем следующего срабатывания: фаза расписания
  переживает рестарт, несколько экземпляров синхронизированы. Повторная регистрация
  с другим определением (задача, интервал, выражение, таймзона) — `ErrScheduleConflict`.
- **Упущенные тики** — `WithCatchUp(taskq.CatchUpSkip)` (по умолчанию: не выполнять,
  сдвинуть тик в будущее) или `WithCatchUp(taskq.CatchUpFireOnce)` (единовременный
  догонный запуск). Тик считается упущенным, когда его время в прошлом в момент
  активации расписания (регистрация, рестарт).
- Job-опции (ретраи и т.п.) для задач каждого тика — `WithSubmitOpts(...)`.

```go
// Locker опционален: с ним несколько экземпляров планировщика не запускают
// одну задачу дважды (см. ниже).
scheduler, err := taskq.NewScheduler(client, locker)
if err != nil {
    panic(err)
}

// cleanupTask *taskq.Task[CleanupArgs, struct{}]
if _, err := taskq.AddEvery(ctx, scheduler, "cleanup", 100*time.Millisecond, cleanupTask, CleanupArgs{}); err != nil {
    panic(err)
}

// reportTask *taskq.Task[ReportArgs, ReportResult]
// Считается по таймзоне (в проде — time.LoadLocation), после долгого простоя
// выполняется один догонный запуск.
loc, err := time.LoadLocation("Europe/Moscow")
if err != nil {
    panic(err)
}
if _, err := taskq.AddCron(ctx, scheduler, "hourly-report", "0 * * * *", reportTask, ReportArgs{},
    taskq.WithCronTimezone(loc),
    taskq.WithCatchUp(taskq.CatchUpFireOnce),
); err != nil {
    panic(err)
}

go scheduler.Run(ctx) // блокируется до Stop/Shutdown или отмены ctx
defer scheduler.Stop()
```

### Защита от двойного запуска (`Locker`)

Если в системе запущено несколько экземпляров планировщика с одинаковыми
настройками, каждый тик защищается распределенной блокировкой: ключ — имя task
и окно тика (для `AddEvery` — интервал, для cron — секунда срабатывания).
Победитель lock'a отправляет задачу, остальные пропускают тик. Блокировка
удерживается до истечения TTL (`WithSchedulerLockTTL`, по умолчанию 10с),
чтобы опоздавший на миллисекунды экземпляр не дублировал срабатывание.

Без `Locker` (`NewScheduler(client, nil)`) защита выключена — это ок для
одного экземпляра.

## Наблюдаемость

Ядро пишет логи, спаны и метрики через три плагин-интерфейса: `Logger`,
`Tracer`, `Meter`. Без конфигурации используются noop-реализации. Подключить
можно отдельно для клиента (`WithLogger`, `WithTracer`, `WithMeter`) и воркера
(`WithWorkerLogger`, `WithWorkerTracer`, `WithWorkerMeter`).

В репозитории — референс-адаптер на стандартной `log/slog` (`adapters/slogadapter`):

```go
logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

client, err := taskq.NewClient(broker, backend,
	taskq.WithLogger(slogadapter.NewLogger(logger)),
	taskq.WithTracer(slogadapter.NewTracer(logger)),
	taskq.WithMeter(slogadapter.NewMeter(logger)),
)

worker, err := taskq.NewWorker(registry, broker, backend,
	taskq.WithWorkerLogger(slogadapter.NewLogger(logger)),
	taskq.WithWorkerTracer(slogadapter.NewTracer(logger)),
	taskq.WithWorkerMeter(slogadapter.NewMeter(logger)),
)
```

- **Logger** — ключ-значения ядра передаются в slog как есть.
- **Tracer** — каждый спан (`taskq.Submit`, `taskq.Worker.process` и др.) — пара
  лог-строк: старт (Debug), завершение с длительностью (Debug) либо `span failed` (Warn).
- **Meter** — каждое обновление метрик (`taskq.submitted`, `taskq.duration` и др.) —
  строка уровня Debug; включайте уровень Debug, чтобы видеть их.

Свои реализации (zap, prometheus, OpenTelemetry) пишутся по тем же интерфейсам.

## Примеры

В `example/` — исполняемые демо **всего** функционала библиотеки.

```bash
go run ./example          # ядро: submit, delay, retry, timeout, deadline,
                          # отмена, dead-letter, rescue, инспекция, идемпотентность,
                          # оркестрация (группа/цепочка/аккорд), очереди
                          # (Task.WithQueue, WithQueue, RunQueues, per-task concurrency)
go run ./example/scheduler  # планировщик: AddEvery/AddCron, таймзона, catch-up,
                          # персистентность через рестарт, два экземпляра с Locker,
                          # Remove, конфликты определений
```

Требование к развитию: каждая новая фича должна использоваться в `example/`
(см. `AGENTS.md` → «Приложение-пример»). Сложные сценарии (рестарт, несколько
экземпляров) живут в подпакетах `example/<сценарий>/`.

## Архитектура

```
                ┌───────────┐
                │ Scheduler │ AddEvery / AddCron
                └─────┬─────┘
                      │ Submit[T,R]
┌─────────┐   Submit[T,R]   ┌─────────┐   Publish   ┌─────────┐
│ Client  │ ──────────────> │  Job    │ ──────────> │ Broker  │
└─────────┘                 └─────────┘             └─────────┘
     ^                                                    │
     │                 ┌─────────┐   Consume              │
     │                 │ Worker  │ <──────────────────────┘
     │                 └────┬────┘
     │                      │
     │                 ┌────┴────┐
     └─────────────────│ Backend │
        Future[R]      │ (state + result)
                       └─────────┘
```

Полный дизайн: [DESIGN.md](./DESIGN.md).

## Разработка

```bash
make test        # запуск тестов
make test-race   # запуск тестов с race detector
make lint        # golangci-lint (ставится в ./bin автоматически)
make ci-local    # локальный прогон GitHub Actions через act (docker)
make fmt         # форматирование
```

Линтер — [golangci-lint](https://golangci-lint.run) (конфигурация — `.golangci.yml`); версия
фиксирована в `Makefile` и совпадает с docker-образом в CI (`.github/workflows/ci.yml`).
Инструменты разработки не зависят от системных установок: `make lint` и `make ci-local`
при необходимости скачивают бинарники в `./bin` (каталог в `.gitignore`).

## License

MIT
