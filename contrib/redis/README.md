# taskq contrib: redis

Реализации plugin-интерфейсов taskq на Redis:

| Интерфейс | Реализация |
|-----------|-----------|
| `taskq.Broker` | Redis Streams + consumer group, доставка at-least-once |
| `taskq.Backend` | хэш состояния + ключ результата с TTL |
| `taskq.Locker` | `SET key token NX PX ttl`, снятие — Lua compare-and-delete |

Отдельный Go-модуль: подключается только если нужен Redis,
ядро и другие contrib-модули не импортируются.

## Подключение

```bash
go get github.com/denisdubovitskiy/taskq/contrib/redis
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
    "time"

    "github.com/redis/go-redis/v9"
    redisb "github.com/denisdubovitskiy/taskq/contrib/redis"

    "github.com/denisdubovitskiy/taskq"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    defer client.Close() // клиент общий — закрывает вызывающий код

    broker, err := redisb.NewBroker(client)
    if err != nil {
        log.Fatal(err)
    }
    backend, err := redisb.NewBackend(client)
    if err != nil {
        log.Fatal(err)
    }
    locker, err := redisb.NewLocker(client)
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

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go func() {
        if err := worker.Run(ctx, ""); err != nil {
            log.Fatal(err)
        }
    }()

    // scheduler := taskq.NewScheduler(qclient, locker, ...)
    _ = qclient
}
```

## Семантика

### Broker

- Очередь задачи — стрим `taskq:stream:<queue>` (пустое имя — `default`),
  consumer group `taskq`, имя потребителя `tq-<pid>-<hex>` (у каждого
  инстанса свое).
- **At-least-once**: сообщение считается обработанным после `XACK`.
  Если воркер упал во время обработки, сообщение вернет в работу
  XAUTOCLAIM-цикл (этот или другой инстанс группы) через `lease`.
- **Задержка**: задача с `job.ETA` в будущем уходит в sorted set
  `taskq:delayed` и переносится в стрим по наступлении задержки.
- **Requeue** (`Nack(requeue=true)`) — повторный `XADD` + подтверждение
  исходного сообщения; **drop** — `XACK` + `XDEL`.
- Длина стрима ограничивается приблизительно `WithMaxLen`
  (по умолчанию 10000 сообщений). При обрезке стрима вместе с группой
  брокер пересоздает группу (сообщения, добавленные между обрезкой и
  пересозданием, теряются — увеличьте `WithMaxLen` или `WithLease`).

### Backend

- Состояние — хэш `taskq:job:<id>` (`state`, `error`, `created_at`,
  `updated_at`), результат — ключ `taskq:job:<id>:result`.
- TTL по умолчанию 24 часа (`WithResultTTL`), 0 — без истечения.
  Future, не успевший прочитать результат до истечения TTL, получит
  ошибку `taskq.ErrJobNotFound`.
- Backend не реализует опциональные интерфейсы ядра `JobStore`,
  `JobInspector`, `ChordBackend`, `ScheduleStore` — оркестрация
  (группы/цепочки/аккорды) и персистентность расписаний планировщика
  недоступны; обычный Submit/retry/cancel работают полностью.

### Locker

- Блокировка `taskq:lock:<key>` живёт `ttl` (`SET ... NX PX`).
- `Release` снимает только свою блокировку (по токену): «старый»
  владелец после истечения TTL не снимет блокировку нового владельца.

## Опции

Все три конструктора принимают общие `Option`:

| Опция | По умолчанию | Описание |
|-------|-------------|----------|
| `WithPrefix(p)` | `"taskq:"` | префикс всех ключей |
| `WithGroup(g)` | `"taskq"` | имя consumer group |
| `WithConsumer(n)` | `tq-<pid>-<hex>` | имя потребителя |
| `WithLease(d)` | `10m` | время возврата сообщений «мертвых» воркеров |
| `WithClaimInterval(d)` | `30s` | период XAUTOCLAIM-цикла |
| `WithMaxLen(n)` | `10000` | приблизительный максимум длины стрима (0 — без) |
| `WithConsumeConcurrency(n)` | `16` | доставок «в полете» на один Consume |
| `WithDelayInterval(d)` | `100ms` | период переноса задержанных задач |
| `WithResultTTL(d)` | `24h` | TTL ключей backend (0 — без) |

## Сборка и тесты

`Makefile` автоматизирует поднятие Redis (docker compose) и прогон тестов:

```bash
cd contrib/redis

make test-docker   # up + тесты + down — самодостаточно, ничего не оставляет
make test-local    # up + тесты, Redis остается запущенным для повторных прогонов
make test          # только тесты (нужен уже доступный Redis)
make test-race     # тесты с race detector
```

Остальные цели: `make up` / `make down` / `make wait` (docker compose),
`make vet`, `make lint` (нужен staticcheck), `make fmt`, `make tidy`,
`make build`, `make clean`. Справка по целям — просто `make`.

Без Makefile — вручную (тот же цикл):

```bash
cd contrib/redis
docker compose up -d
go test -count=1 ./...
docker compose down
```

Адрес переопределяется переменной окружения `TASKQ_TEST_REDIS_ADDR`
(по умолчанию `localhost:6380`). Без доступного Redis тесты
пропускаются (`t.Skip`).
