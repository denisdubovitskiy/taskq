# taskq contrib: postgresql

Реализации plugin-интерфейсов taskq на PostgreSQL — для команд
без Redis (river-style):

| Интерфейс | Реализация |
|-----------|-----------|
| `taskq.Broker` | таблица очередей, `FOR UPDATE SKIP LOCKED`, `LISTEN/NOTIFY` + polling |
| `taskq.Backend` | таблицы состояний и результатов |
| `taskq.Locker` | таблица блокировок с TTL |

Отдельный Go-модуль: подключается только если нужен PostgreSQL,
ядро и другие contrib-модули не импортируются.

## Подключение

```bash
go get github.com/denisdubovitskiy/taskq/contrib/postgresql
```

Пока ядро не опубликовано в proxy, укажите локальный путь в `replace`:

```
replace github.com/denisdubovitskiy/taskq => /путь/к/taskq
```

## Использование

```go
package main

import (
    "context"
    "log"

    postgresb "github.com/denisdubovitskiy/taskq/contrib/postgresql"

    "github.com/denisdubovitskiy/taskq"
)

func main() {
    dsn := "postgres://user:pass@localhost:5432/taskq?sslmode=disable"
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    broker, err := postgresb.NewBroker(ctx, dsn)
    if err != nil {
        log.Fatal(err)
    }
    backend, err := postgresb.NewBackend(ctx, dsn)
    if err != nil {
        log.Fatal(err)
    }
    locker, err := postgresb.NewLocker(ctx, dsn)
    if err != nil {
        log.Fatal(err)
    }

    qclient, err := taskq.NewClient(broker, backend)
    if err != nil {
        log.Fatal(err)
    }

    registry := taskq.NewRegistry()
    // taskq.Register(registry, task, handler)

    worker, err := taskq.NewWorker(registry, broker, backend)
    if err != nil {
        log.Fatal(err)
    }

    go func() {
        if err := worker.Run(ctx, ""); err != nil {
            log.Fatal(err)
        }
    }()

    // scheduler := taskq.NewScheduler(qclient, locker, ...)
    _ = qclient
}
```

Каждый компонент создает и закрывает собственный connection pool:
`New*` подключается и применяет миграции (создает таблицы, если их
нет), `Close` закрывает пул.

## Семантика

### Broker

- Очередь задачи — таблица `tq_jobs` (id, queue, body JSONB, eta,
  status `waiting`/`processing`, owner, claimed_at, created_at).
  Пустое имя очереди — `default`.
- **Конкурирующие потребители**: заявка на задачи —
  `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE`, несколько инстансов
  делят очередь без дублей.
- **Доставка**: публикация шлет `NOTIFY taskq_jobs` (миллисекундная
  задержка), polling каждые `WithPollInterval` (250 мс) — фолбэк.
- **Подтверждение**: `Ack` удаляет строку; `Nack(requeue)` возвращает
  в `waiting`; `Nack(drop)` удаляет.
- **Задержка**: задача с будущим `job.ETA` заявляется только после
  наступления задержки.
- **Lease** (`WithLease > 0`): задачи, застрявшие в `processing`
  (инстанс упал без Ack), забирают другие инстансы. По умолчанию
  выключен (0), чтобы не дублировать долго выполняющиеся задачи.
  At-least-once: при включенном lease задача, выполняющаяся дольше
  lease, может быть доставлена повторно.

### Backend

- Состояние — `tq_job_states` (id, state, error, created_at, updated_at),
  результат — `tq_job_results` (id, data).
- TTL отсутствует: строки живут до `Purge` или внешней очистки
  (полезно для истории задач).
- Backend не реализует опциональные интерфейсы ядра `JobStore`,
  `JobInspector`, `ChordBackend`, `ScheduleStore` — оркестрация
  (группы/цепочки/аккорды) и персистентность расписаний планировщика
  недоступны; обычный Submit/retry/cancel работают полностью.

### Locker

- Таблица `tq_locks` (key, token, expires_at). Захват атомарен:
  `INSERT ... ON CONFLICT DO UPDATE` только для просроченной строки.
- `Release` снимает только свою блокировку (по токену): «старый»
  владелец после истечения TTL не снимет блокировку нового владельца.
- Просроченные строки подчищаются при каждом `Lock` (best effort).

## Опции

Все три конструктора принимают общие `Option`:

| Опция | По умолчанию | Описание |
|-------|-------------|----------|
| `WithSchema(s)` | `"public"` | схема PostgreSQL |
| `WithPollInterval(d)` | `250ms` | период опроса без оповещения |
| `WithBatchSize(n)` | `16` | задач за одну заявку |
| `WithConsumeConcurrency(n)` | `16` | доставок «в полете» на один Consume |
| `WithLease(d)` | `0` (выкл.) | возврат застрявших `processing` задач |
| `WithMaxConns(n)` | `16` | размер connection pool |

## Сборка и тесты

`Makefile` автоматизирует поднятие PostgreSQL (docker compose) и прогон тестов:

```bash
cd contrib/postgresql

make test-docker   # up + тесты + down — самодостаточно, ничего не оставляет
make test-local    # up + тесты, PostgreSQL остается запущенной для повторных прогонов
make test          # только тесты (нужна уже доступная PostgreSQL)
make test-race     # тесты с race detector
```

Остальные цели: `make up` / `make down` / `make wait` (docker compose, pg_isready),
`make vet`, `make lint` (нужен staticcheck), `make fmt`, `make tidy`,
`make build`, `make clean`. Справка по целям — просто `make`.

Без Makefile — вручную (тот же цикл):

```bash
cd contrib/postgresql
docker compose up -d
go test -count=1 ./...
docker compose down
```

DSN переопределяется переменной окружения `TASKQ_TEST_PG_DSN`
(по умолчанию `postgres://taskq:taskq@localhost:5432/taskq?sslmode=disable`).
Без доступной PostgreSQL тесты пропускаются (`t.Skip`).
