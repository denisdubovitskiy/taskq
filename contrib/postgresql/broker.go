package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// defaultQueue — имя очереди для задач без явного имени.
	defaultQueue = "default"

	// notifyChannel — канал LISTEN/NOTIFY о новых задачах.
	notifyChannel = "taskq_jobs"

	// listenRetryMax — максимум паузы перед повтором LISTEN.
	listenRetryMax = 10 * time.Second
)

// errBrokerClosed — брокер закрыт.
var errBrokerClosed = errors.New("postgresql broker is closed")

// Broker — реализация taskq.Broker на PostgreSQL.
//
// Очередь — таблица tq_jobs. Несколько инстансов делят очередь через
// FOR UPDATE SKIP LOCKED; новые задачи разбудить потребителей через
// LISTEN/NOTIFY, polling — фолбэк. Доставка at-least-once: если инстанс
// упал без Ack и включен lease, задачу возьмет другой инстанс.
type Broker struct {
	pool         *pgxpool.Pool
	cfg          config
	consumer     string
	sql          brokerSQL
	notify       chan struct{}
	inFlight     chan struct{}
	done         chan struct{}
	listenCtx    context.Context
	listenCancel context.CancelFunc
	closeOnce    sync.Once
	closed       atomic.Bool
}

// brokerSQL — подготовленные SQL-запросы брокера (схема подставлена).
type brokerSQL struct {
	publish string
	claim   string
	ack     string
	requeue string
	drop    string
	notify  string
}

// NewBroker создает брокер: подключается к PostgreSQL, проверяет связь
// и применяет миграции. Close закрывает connection pool.
func NewBroker(ctx context.Context, dsn string, opts ...Option) (*Broker, error) {
	if dsn == "" {
		return nil, errors.New("dsn is empty")
	}

	cfg := defaultConfig()
	applyOptions(&cfg, opts)

	pool, err := newPool(ctx, dsn, cfg.maxConns)
	if err != nil {
		return nil, err
	}

	if err := migrate(ctx, pool, cfg.schema); err != nil {
		pool.Close()
		return nil, err
	}

	listenCtx, listenCancel := context.WithCancel(context.Background())

	q := quoteIdent(cfg.schema)
	b := &Broker{
		pool:         pool,
		cfg:          cfg,
		consumer:     defaultConsumerName(),
		listenCtx:    listenCtx,
		listenCancel: listenCancel,
		sql: brokerSQL{
			publish: `
INSERT INTO ` + q + `.tq_jobs (id, queue, body, eta, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'waiting', now(), now())
ON CONFLICT (id) DO UPDATE
SET queue = EXCLUDED.queue,
	body = EXCLUDED.body,
	eta = EXCLUDED.eta,
	status = 'waiting',
	owner = NULL,
	claimed_at = NULL,
	updated_at = now()
RETURNING id`,
			claim: `
UPDATE ` + q + `.tq_jobs AS j
SET status = 'processing',
	owner = $1,
	claimed_at = now(),
	updated_at = now()
FROM (
	SELECT id
	FROM ` + q + `.tq_jobs
	WHERE queue = $2
	  AND ((status = 'waiting' AND (eta IS NULL OR eta <= now()))
	       OR ($3 > 0 AND status = 'processing' AND claimed_at + ($3::float8 * interval '1 millisecond') <= now()))
	ORDER BY created_at, id
	LIMIT $4
	FOR UPDATE SKIP LOCKED
) AS picked
WHERE j.id = picked.id
RETURNING j.id, j.body`,
			ack:     `DELETE FROM ` + q + `.tq_jobs WHERE id = $1 AND owner = $2`,
			requeue: `UPDATE ` + q + `.tq_jobs SET status = 'waiting', owner = NULL, claimed_at = NULL, updated_at = now() WHERE id = $1 AND owner = $2`,
			drop:    `DELETE FROM ` + q + `.tq_jobs WHERE id = $1 AND owner = $2`,
			notify:  `SELECT pg_notify($1, '')`,
		},
		notify:   make(chan struct{}, 1),
		inFlight: make(chan struct{}, cfg.consumeConcurrency),
		done:     make(chan struct{}),
	}
	go b.listener()

	return b, nil
}

// Publish публикует задачу. Повторная публикация с тем же ID (retry ядра)
// возвращает задачу в очередь с новым телом и ETA.
func (b *Broker) Publish(ctx context.Context, job *taskq.Job) error {
	if b.closed.Load() {
		return errBrokerClosed
	}
	if job == nil {
		return errors.New("job is nil")
	}

	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode job %s: %w", job.ID, err)
	}

	queue := job.Queue
	if queue == "" {
		queue = defaultQueue
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("publish job %s: %w", job.ID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, b.sql.publish, job.ID, queue, string(body), job.ETA); err != nil {
		return fmt.Errorf("publish job %s: %w", job.ID, err)
	}
	if _, err := tx.Exec(ctx, b.sql.notify, notifyChannel); err != nil {
		return fmt.Errorf("notify job %s: %w", job.ID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("publish job %s: %w", job.ID, err)
	}
	return nil
}

// Consume читает задачи из очереди и передает их handler.
// Блокируется до отмены ctx. Возвращает ctx.Err() при штатной остановке;
// транзиентные ошибки БД переживают в следующем цикле.
func (b *Broker) Consume(ctx context.Context, queue string, handler taskq.DeliveryHandler) error {
	if b.closed.Load() {
		return errBrokerClosed
	}
	if handler == nil {
		return errors.New("handler is nil")
	}
	if queue == "" {
		queue = defaultQueue
	}

	ticker := time.NewTicker(b.cfg.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-b.notify:
		}

		// Дренаж оставшихся оповещений: один цикл опроса
		// перекрывает все накопленные пробуждения.
		drained := true
		for drained {
			select {
			case <-b.notify:
			default:
				drained = false
			}
		}

		if err := b.claimAndDispatch(ctx, queue, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Ошибка БД — повторим в следующем цикле.
		}
	}
}

// Close закрывает брокер: останавливает цикл LISTEN и connection pool.
// Повторный вызов безопасен.
func (b *Broker) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		close(b.done)
		b.listenCancel()
	})
	b.closed.Store(true)
	b.pool.Close()
	return nil
}

// claimAndDispatch заявляет партию задач из очереди и отдает их воркеру.
func (b *Broker) claimAndDispatch(ctx context.Context, queue string, handler taskq.DeliveryHandler) error {
	rows, err := b.pool.Query(ctx, b.sql.claim, b.consumer, queue, b.cfg.lease.Milliseconds(), b.cfg.batchSize)
	if err != nil {
		return fmt.Errorf("claim from %s: %w", queue, err)
	}
	defer rows.Close()

	var jobs []claimedJob
	for rows.Next() {
		var j claimedJob
		if err := rows.Scan(&j.id, &j.body); err != nil {
			return fmt.Errorf("scan claim: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("claim from %s: %w", queue, err)
	}

	for _, j := range jobs {
		b.dispatch(ctx, handler, j)
	}
	return nil
}

// claimedJob — заявленная задача.
type claimedJob struct {
	id   string
	body []byte
}

// dispatch отправляет заявленную задачу воркеру в отдельной горутине,
// ограничив число доставок «в полете».
func (b *Broker) dispatch(ctx context.Context, handler taskq.DeliveryHandler, j claimedJob) {
	select {
	case b.inFlight <- struct{}{}:
	case <-ctx.Done():
		// Воркер останавливается — вернем задачу в очередь,
		// чтобы ее забрал следующий инстанс.
		// Свежий контекст: ctx уже отменен.
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = b.requeue(rctx, j.id)
		rcancel()
		return
	}

	go func() {
		defer func() { <-b.inFlight }()

		var job taskq.Job
		if len(j.body) > 0 {
			_ = json.Unmarshal(j.body, &job)
		}

		delivery := &taskq.Delivery{
			ID:      job.ID,
			Body:    j.body,
			Headers: job.Headers,
			Ack:     func(ctx context.Context) error { return b.ack(ctx, job.ID) },
			Nack: func(ctx context.Context, requeue bool) error {
				if requeue {
					return b.requeue(ctx, job.ID)
				}
				return b.drop(ctx, job.ID)
			},
		}

		handler.Handle(ctx, delivery)
	}()
}

// ack подтверждает обработку задачи: строка удаляется из очереди.
func (b *Broker) ack(ctx context.Context, jobID string) error {
	if _, err := b.pool.Exec(ctx, b.sql.ack, jobID, b.consumer); err != nil {
		return fmt.Errorf("ack job %s: %w", jobID, err)
	}
	return nil
}

// requeue возвращает задачу в ожидание (статус waiting) и будит потребителей.
func (b *Broker) requeue(ctx context.Context, jobID string) error {
	if _, err := b.pool.Exec(ctx, b.sql.requeue, jobID, b.consumer); err != nil {
		return fmt.Errorf("requeue job %s: %w", jobID, err)
	}
	if _, err := b.pool.Exec(ctx, b.sql.notify, notifyChannel); err != nil {
		return fmt.Errorf("notify job %s: %w", jobID, err)
	}
	return nil
}

// drop отбрасывает задачу без повторной доставки.
func (b *Broker) drop(ctx context.Context, jobID string) error {
	if _, err := b.pool.Exec(ctx, b.sql.drop, jobID, b.consumer); err != nil {
		return fmt.Errorf("drop job %s: %w", jobID, err)
	}
	return nil
}

// listener держит соединение на LISTEN и будит цикл Consume о новых
// задачах. При обрыве переподключается с экспоненциальной паузой.
func (b *Broker) listener() {
	backoff := time.Second
	for {
		if b.closed.Load() {
			return
		}

		if err := b.listenOnce(); err == nil {
			continue
		}
		if b.closed.Load() {
			return
		}

		select {
		case <-b.done:
			return
		case <-time.After(backoff):
		}
		if backoff < listenRetryMax {
			backoff *= 2
		}
	}
}

// listenOnce слушает оповещения до ошибки или остановки брокера.
func (b *Broker) listenOnce() error {
	conn, err := b.pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()

	pgxConn := conn.Conn()
	if _, err := pgxConn.Exec(b.listenCtx, "LISTEN "+notifyChannel); err != nil {
		return err
	}

	for {
		if _, err := pgxConn.WaitForNotification(b.listenCtx); err != nil {
			return err
		}
		select {
		case b.notify <- struct{}{}:
		default:
		}
	}
}
