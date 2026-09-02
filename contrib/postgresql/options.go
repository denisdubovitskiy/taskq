package postgresql

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// config — внутренняя конфигурация Broker/Backend/Locker.
type config struct {
	schema             string
	pollInterval       time.Duration
	batchSize          int
	consumeConcurrency int
	lease              time.Duration
	maxConns           int32
}

// defaultConfig возвращает конфигурацию со значениями по умолчанию.
func defaultConfig() config {
	return config{
		schema:             "public",
		pollInterval:       250 * time.Millisecond,
		batchSize:          16,
		consumeConcurrency: 16,
		lease:              0,
		maxConns:           16,
	}
}

// Option — функциональная опция NewBroker/NewBackend/NewLocker.
type Option func(*config)

// WithSchema задает схему PostgreSQL (по умолчанию "public").
func WithSchema(schema string) Option {
	return func(c *config) {
		if schema != "" {
			c.schema = schema
		}
	}
}

// WithPollInterval задает период опроса очереди при отсутствии
// оповещения LISTEN/NOTIFY (по умолчанию 250 миллисекунд).
func WithPollInterval(interval time.Duration) Option {
	return func(c *config) {
		if interval > 0 {
			c.pollInterval = interval
		}
	}
}

// WithBatchSize задает максимум задач за одну заявку (по умолчанию 16).
func WithBatchSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithConsumeConcurrency задает максимум параллельно обрабатываемых
// доставок одним вызовом Consume (по умолчанию 16). Реальный предел
// конкурентности исполнения устанавливает воркер (taskq.WithConcurrency);
// здесь ограничено число доставок «в полете» между брокером и воркером.
func WithConsumeConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.consumeConcurrency = n
		}
	}
}

// WithLease задает время возврата в работу задач, застрявших в статусе
// processing (по умолчанию 0 — выключено). Инстанс, не подтвердивший
// задачи в течение lease (сбой, рестарт), позволяет другим инстансам
// заново забрать эти задачи. Если задача может выполняться дольше lease,
// возможна повторная доставка выполняющейся задачи.
func WithLease(lease time.Duration) Option {
	return func(c *config) {
		c.lease = lease
	}
}

// WithMaxConns задает размер connection pool (по умолчанию 16).
func WithMaxConns(n int32) Option {
	return func(c *config) {
		if n > 0 {
			c.maxConns = n
		}
	}
}

// applyOptions применяет опции к конфигурации.
func applyOptions(cfg *config, opts []Option) {
	for _, opt := range opts {
		opt(cfg)
	}
}

// defaultConsumerName строит имя инстанса: "tq-<pid>-<8 hex>".
// Используется как владелец заявок (owner) — в логике не значим,
// нужен для диагностики и точного Ack.
func defaultConsumerName() string {
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("tq-%d-%s", os.Getpid(), hex.EncodeToString(buf))
}
