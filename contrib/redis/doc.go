// Package redis — реализации taskq.Broker, taskq.Backend и taskq.Locker на Redis.
//
// Broker строит на Redis Streams с consumer group (доставка at-least-once):
//
//   - очередь задачи — stream "<prefix>stream:<queue>", consumer group "<group>";
//   - подтверждение — XACK после обработки (delivery.Ack), requeue — повторный
//     XADD + XACK, drop — XACK + XDEL;
//   - задержанные задачи (job.ETA в будущем) уходят в sorted set "<prefix>delayed"
//     и переносятся в stream по наступлении задержки;
//   - сообщения, не подтвержденные в течение lease (воркер упал во время
//     обработки), возвращаются в работу XAUTOCLAIM-циклом.
//
// Backend хранит состояние задачи в хэше "<prefix>job:<id>" (state, error,
// created_at, updated_at) и результат в ключе "<prefix>job:<id>:result".
// Все ключи имеют TTL (по умолчанию 24 часа, WithResultTTL).
//
// Locker реализует распределенную блокировку: SET key token NX PX ttl,
// снятие — Lua-скрипт compare-and-delete по токену (Release не снимет
// блокировку, захваченную другим владельцем после истечения TTL).
//
// Компоненты принимают *redis.Client, который передается вызывающим кодом
// и может разделяться между компонентами. Close компонента останавливает его
// внутренние горутинные циклы, но НЕ закрывает клиент — закройте *redis.Client
// сами, когда компоненты больше не нужны.
package redis
