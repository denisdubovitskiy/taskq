// Package postgresql — реализации taskq.Broker, taskq.Backend и taskq.Locker
// на PostgreSQL (для команд без Redis).
//
// Broker строит на таблице очередей с конкурирующими потребителями:
//
//   - очередь задачи — таблица "tq_jobs" (id, queue, body, eta, status);
//   - заявка на задачи — "FOR UPDATE SKIP LOCKED" (атомарный claim,
//     несколько инстансов делят очередь без дублей);
//   - оповещение о новых задачах — "LISTEN/NOTIFY" (миллисекундная
//     задержка доставки) плюс polling как фолбэк;
//   - подтверждение — удаление строки (Ack), requeue — возврат в "waiting",
//     drop — удаление;
//   - lease (WithLease > 0) возвращает в работу задачи, застрявшие
//     в "processing" после сбоя инстанса; по умолчанию выключен,
//     чтобы не дублировать долго выполняющиеся задачи.
//
// Backend хранит состояние задачи в "tq_job_states" и результат
// в "tq_job_results" (TTL нет: строки живут до Purge или очистки).
//
// Locker — таблица "tq_locks" (key, token, expires_at): захват атомарен
// (INSERT ... ON CONFLICT), просроченную блокировку можно занять,
// Release снимает только свою (по токену).
//
// Каждый компонент создает и закрывает собственный connection pool:
// New* принимает DSN, Close закрывает пул. Схемы создаются автоматически
// (CREATE TABLE IF NOT EXISTS) при конструировании.
package postgresql
