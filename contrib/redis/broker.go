package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/redis/go-redis/v9"
)

const (
	// jobField — поле стрима, в котором хранится сериализованная задача.
	jobField = "job"

	// defaultQueue — имя очереди для задач без явного имени.
	defaultQueue = "default"

	// readRetryDelay — пауза перед повтором после ошибки чтения.
	readRetryDelay = 500 * time.Millisecond

	// claimBatch — максимум сообщений за один XAUTOCLAIM.
	claimBatch = 64

	// moverBatch — максимум задержанных задач за один цикл переноса.
	moverBatch = 100
)

// errBrokerClosed — брокер закрыт.
var errBrokerClosed = errors.New("redis broker is closed")

// moverScript атомарно извлекает из sorted set все задачи, чья задержка
// наступила (score <= now), и удаляет их из набора.
const moverScript = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for i = 1, #due do
    redis.call('ZREM', KEYS[1], due[i])
end
return due`

// Broker — реализация taskq.Broker на Redis Streams.
//
// Каждая очередь — отдельный stream с consumer group. Доставка at-least-once:
// сообщение считается обработанным после XACK, если воркер упал — сообщение
// вернут в работу XAUTOCLAIM-цикл (lease) или другой инстанс группы.
type Broker struct {
	client    *redis.Client
	cfg       config
	inFlight  chan struct{}
	streams   map[string]struct{}
	streamsMu sync.Mutex
	done      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
}

// NewBroker создает брокер. Клиент передается вызывающим кодом и
// закрывается им же (Close брокера не закрывает клиент).
func NewBroker(client *redis.Client, opts ...Option) (*Broker, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	cfg := defaultConfig()
	applyOptions(&cfg, opts)

	b := &Broker{
		client:   client,
		cfg:      cfg,
		inFlight: make(chan struct{}, cfg.consumeConcurrency),
		streams:  make(map[string]struct{}),
		done:     make(chan struct{}),
	}
	go b.mover()

	return b, nil
}

// Publish публикует задачу. Если job.ETA в будущем, задача уходит
// в sorted set и попадает в очередь по наступлении задержки.
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

	if job.ETA != nil && job.ETA.After(time.Now()) {
		if err := b.client.ZAdd(ctx, b.delayedKey(), redis.Z{Score: float64(job.ETA.UnixMilli()), Member: body}).Err(); err != nil {
			return fmt.Errorf("add delayed job %s: %w", job.ID, err)
		}
		return nil
	}

	return b.publishToStream(ctx, job.Queue, body)
}

// Consume читает задачи из очереди и передает их handler.
// Блокируется до отмены ctx. Возвращает ctx.Err() при штатной остановке.
func (b *Broker) Consume(ctx context.Context, queue string, handler taskq.DeliveryHandler) error {
	if b.closed.Load() {
		return errBrokerClosed
	}
	if handler == nil {
		return errors.New("handler is nil")
	}

	stream := b.streamFor(queue)
	if err := b.ensureGroup(ctx, stream); err != nil {
		return fmt.Errorf("ensure group %s on %s: %w", b.cfg.group, stream, err)
	}

	// Цикл возврата сообщений от «мертвых» воркеров.
	go b.autoClaim(ctx, stream, handler)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		streams, err := b.read(ctx, stream)
		switch {
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			return ctx.Err()
		case err == nil:
			for _, s := range streams {
				for _, msg := range s.Messages {
					b.dispatch(ctx, s.Stream, handler, msg)
				}
			}
		case isNoGroup(err):
			// Стрим обрезан вместе с группой (MAXLEN) — пересоздаем.
			if genErr := b.ensureGroup(ctx, stream); genErr != nil {
				return fmt.Errorf("ensure group %s on %s: %w", b.cfg.group, stream, genErr)
			}
		default:
			// Транзиентная ошибка сети — повторяем с паузой.
			if waitErr := wait(ctx, readRetryDelay); waitErr != nil {
				return waitErr
			}
		}
	}
}

// Close останавливает внутренние циклы брокера (перенос задержанных задач,
// автоklaim). Переданный *redis.Client закрывается вызывающим кодом.
// Повторный вызов безопасен.
func (b *Broker) Close(ctx context.Context) error {
	b.closeOnce.Do(func() { close(b.done) })
	b.closed.Store(true)
	return nil
}

// publishToStream кладет тело задачи в стрим очереди, при необходимости
// создавая consumer group.
func (b *Broker) publishToStream(ctx context.Context, queue string, body []byte) error {
	stream := b.streamFor(queue)

	b.streamsMu.Lock()
	_, known := b.streams[stream]
	b.streamsMu.Unlock()

	if known {
		return b.xAdd(ctx, stream, body)
	}

	args := &redis.XAddArgs{Stream: stream, Values: map[string]any{jobField: body}}
	if b.cfg.maxLen > 0 {
		args.MaxLen = int64(b.cfg.maxLen)
		args.Approx = true
	}

	pipe := b.client.Pipeline()
	pipe.XGroupCreateMkStream(ctx, stream, b.cfg.group, "$")
	xadd := pipe.XAdd(ctx, args)
	if _, err := pipe.Exec(ctx); err != nil && !isBusyGroup(err) {
		return fmt.Errorf("publish to %s: %w", stream, err)
	}
	// При BUSYGROUP ошибка Exec — от команды создания группы;
	// результат XADD проверяется отдельно.
	if _, err := xadd.Result(); err != nil {
		return fmt.Errorf("publish to %s: %w", stream, err)
	}

	b.streamsMu.Lock()
	b.streams[stream] = struct{}{}
	b.streamsMu.Unlock()

	return nil
}

// xAdd кладет тело задачи в стрим (с приблизительной обрезкой, если задана).
func (b *Broker) xAdd(ctx context.Context, stream string, body []byte) error {
	args := &redis.XAddArgs{Stream: stream, Values: map[string]any{jobField: body}}
	if b.cfg.maxLen > 0 {
		args.MaxLen = int64(b.cfg.maxLen)
		args.Approx = true
	}

	if _, err := b.client.XAdd(ctx, args).Result(); err != nil {
		return fmt.Errorf("xadd %s: %w", stream, err)
	}
	return nil
}

// ensureGroup создает consumer group со стартом "$" (новые сообщения).
// Существующая группа (BUSYGROUP) — не ошибка.
func (b *Broker) ensureGroup(ctx context.Context, stream string) error {
	err := b.client.XGroupCreateMkStream(ctx, stream, b.cfg.group, "$").Err()
	if err != nil && !isBusyGroup(err) {
		return err
	}
	return nil
}

// read блокирующе читает одно новое сообщение из стрима.
func (b *Broker) read(ctx context.Context, stream string) ([]redis.XStream, error) {
	args := &redis.XReadGroupArgs{
		Group:    b.cfg.group,
		Consumer: b.cfg.consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1,
	}
	return b.client.XReadGroup(ctx, args).Result()
}

// autoClaim периодически возвращает в доставку сообщения, доставленные
// дольше lease назад и не подтвержденные XACK (воркер упал во время работы).
func (b *Broker) autoClaim(ctx context.Context, stream string, handler taskq.DeliveryHandler) {
	ticker := time.NewTicker(b.cfg.claimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.done:
			return
		case <-ticker.C:
		}

		msgs, _, err := b.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    b.cfg.group,
			Consumer: b.cfg.consumer,
			MinIdle:  b.cfg.lease,
			Start:    "0-0",
			Count:    claimBatch,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		for _, msg := range msgs {
			b.dispatch(ctx, stream, handler, msg)
		}
	}
}

// mover переносит задержанные задачи (job.ETA наступила) в стримы очередей.
// Живет до Close брокера.
func (b *Broker) mover() {
	ctx := context.Background()
	ticker := time.NewTicker(b.cfg.delayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.done:
			return
		case <-ticker.C:
		}

		due, err := b.popDue(ctx)
		if err != nil {
			continue
		}

		for _, body := range due {
			if !b.doneClosed() {
				b.deliverDelayed(ctx, body)
			}
		}
	}
}

// popDue извлекает из sorted set все задачи, чья задержка наступила.
func (b *Broker) popDue(ctx context.Context) ([][]byte, error) {
	res, err := b.client.Eval(ctx, moverScript, []string{b.delayedKey()}, time.Now().UnixMilli(), moverBatch).Result()
	if err != nil {
		return nil, err
	}

	list, ok := res.([]any)
	if !ok {
		return nil, nil
	}

	due := make([][]byte, 0, len(list))
	for _, item := range list {
		body, ok := item.(string)
		if !ok {
			continue
		}
		due = append(due, []byte(body))
	}
	return due, nil
}

// deliverDelayed кладет задачу в стрим ее очереди.
// Ошибка не критична: задача потеряна только при сбое Redis.
func (b *Broker) deliverDelayed(ctx context.Context, body []byte) {
	var job taskq.Job
	if err := json.Unmarshal(body, &job); err != nil {
		return
	}

	if err := b.publishToStream(ctx, job.Queue, body); err != nil {
		// Задача уже извлечена из sorted set — вернем ее, если брокер жив.
		if !b.closed.Load() && ctx.Err() == nil {
			_, _ = b.client.ZAdd(ctx, b.delayedKey(), redis.Z{Score: float64(job.ETA.UnixMilli()), Member: body}).Result()
		}
	}
}

// dispatch отправляет доставленное сообщение воркеру в отдельной горутине,
// ограничив число доставок «в полете».
func (b *Broker) dispatch(ctx context.Context, stream string, handler taskq.DeliveryHandler, msg redis.XMessage) {
	select {
	case b.inFlight <- struct{}{}:
	case <-ctx.Done():
		// Воркер останавливается — вернем сообщение в очередь,
		// чтобы его забрал следующий инстанс, не дожидаясь XAUTOCLAIM.
		// Свежий контекст: ctx уже отменен.
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = b.requeue(rctx, stream, msg, messageBody(msg))
		rcancel()
		return
	}

	go func() {
		defer func() { <-b.inFlight }()

		body := messageBody(msg)
		var job taskq.Job
		if len(body) > 0 {
			_ = json.Unmarshal(body, &job)
		}

		delivery := &taskq.Delivery{
			ID:      job.ID,
			Body:    body,
			Headers: job.Headers,
			Ack:     func(ctx context.Context) error { return b.ack(ctx, stream, msg) },
			Nack: func(ctx context.Context, requeue bool) error {
				if requeue {
					return b.requeue(ctx, stream, msg, body)
				}
				return b.drop(ctx, stream, msg)
			},
		}

		handler.Handle(ctx, delivery)
	}()
}

// messageBody извлекает тело задачи из поля сообщения.
// go-redis возвращает значения стрима как string; []byte — на случай
// смены версии клиента.
func messageBody(msg redis.XMessage) []byte {
	switch v := msg.Values[jobField].(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	default:
		return nil
	}
}

// ack подтверждает обработку сообщения (XACK).
func (b *Broker) ack(ctx context.Context, stream string, msg redis.XMessage) error {
	err := b.client.XAck(ctx, stream, b.cfg.group, msg.ID).Err()
	if err != nil {
		return fmt.Errorf("ack %s: %w", msg.ID, err)
	}
	return nil
}

// requeue возвращает сообщение в очередь: повторный XADD + подтверждение
// исходного сообщения. Если сбой между XADD и XACK, сообщение может
// быть доставлено дважды (at-least-once).
func (b *Broker) requeue(ctx context.Context, stream string, msg redis.XMessage, body []byte) error {
	var job taskq.Job
	if len(body) > 0 {
		_ = json.Unmarshal(body, &job)
	}

	if err := b.publishToStream(ctx, job.Queue, body); err != nil {
		return fmt.Errorf("requeue %s: %w", msg.ID, err)
	}

	if err := b.drop(ctx, stream, msg); err != nil {
		return fmt.Errorf("drop original %s: %w", msg.ID, err)
	}
	return nil
}

// drop отбрасывает сообщение без повторной доставки (XACK + XDEL).
func (b *Broker) drop(ctx context.Context, stream string, msg redis.XMessage) error {
	if err := b.client.XAck(ctx, stream, b.cfg.group, msg.ID).Err(); err != nil {
		return fmt.Errorf("ack %s: %w", msg.ID, err)
	}
	if err := b.client.XDel(ctx, stream, msg.ID).Err(); err != nil {
		return fmt.Errorf("del %s: %w", msg.ID, err)
	}
	return nil
}

// streamFor возвращает имя стрима очереди.
func (b *Broker) streamFor(queue string) string {
	if queue == "" {
		queue = defaultQueue
	}
	return b.cfg.prefix + "stream:" + queue
}

// delayedKey возвращает ключ sorted set задержанных задач.
func (b *Broker) delayedKey() string {
	return b.cfg.prefix + "delayed"
}

// doneClosed сообщает, завершил ли брокер работу.
func (b *Broker) doneClosed() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}

// isNoGroup — ошибка NOGROUP (группа утрачена вместе со стримом).
func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

// isBusyGroup — ошибка BUSYGROUP (группа уже существует, не ошибка для нас).
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// wait ожидает d или отмены ctx.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
