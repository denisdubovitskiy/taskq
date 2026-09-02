# taskq example

Полноценное приложение, демонстрирующее все фичи библиотеки `taskq`.

## Запуск

Из корня `taskq/`:

```bash
go run ./example
```

## Что демонстрируется

1. **Простая задача** — `taskq.Submit` + `Future.GetWithTimeout`.
2. **Отложенная задача** — `taskq.WithDelay`.
3. **Retry policy** — `taskq.WithRetry` на заведомо падающей задаче.
4. **Fire-and-forget** — `taskq.SubmitOneWay`.
5. **Hooks** — pre-execute, post-execute, error handler.
6. **Файловый backend** — `backends/file` для сохранения состояний и результатов.
7. **In-memory broker** — `brokers/memory` для локального тестирования.

## Очистка

Пример создает директорию `./taskq-example-results` для хранения результатов. Она удаляется при старте и может быть удалена вручную:

```bash
rm -rf ./taskq-example-results
```
