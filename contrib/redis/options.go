package redis

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"time"
)

// config — внутренняя конфигурация Broker/Backend/Locker.
type config struct {
	prefix             string
	group              string
	consumer           string
	lease              time.Duration
	claimInterval      time.Duration
	maxLen             int
	consumeConcurrency int
	delayInterval      time.Duration
	resultTTL          time.Duration
}

// defaultConfig возвращает конфигурацию со значениями по умолчанию.
func defaultConfig() config {
	return config{
		prefix:             "taskq:",
		group:              "taskq",
		lease:              10 * time.Minute,
		claimInterval:      30 * time.Second,
		maxLen:             10000,
		consumeConcurrency: 16,
		delayInterval:      100 * time.Millisecond,
		resultTTL:          24 * time.Hour,
	}
}

// Option — функциональная опция NewBroker/NewBackend/NewLocker.
type Option func(*config)

// WithPrefix задает префикс всех ключей (по умолчанию "taskq:").
func WithPrefix(prefix string) Option {
	return func(c *config) {
		if prefix != "" {
			c.prefix = prefix
		}
	}
}

// WithGroup задает имя consumer group для брокера (по умолчанию "taskq").
func WithGroup(group string) Option {
	return func(c *config) {
		if group != "" {
			c.group = group
		}
	}
}

// WithConsumer задает имя потребителя внутри consumer group.
// По умолчанию — "tq-<pid>-<8 hex>". У разных инстансов должно быть
// разное имя, иначе XAUTOCLAIM перераспределит сообщения между ними.
func WithConsumer(name string) Option {
	return func(c *config) {
		if name != "" {
			c.consumer = name
		}
	}
}

// WithLease задает максимальное время жизни недоподтвержденного сообщения
// (по умолчанию 10 минут). Сообщение, доставленное дольше lease назад и не
// подтвержденное XACK, считается за «мертвый» воркер и возвращается
// в доставку XAUTOCLAIM. Увеличьте lease, если задача может выполняться
// дольше — иначе возможна повторная доставка выполняющейся задачи.
func WithLease(lease time.Duration) Option {
	return func(c *config) {
		if lease > 0 {
			c.lease = lease
		}
	}
}

// WithClaimInterval задает период XAUTOCLAIM-цикла (по умолчанию 30 секунд).
func WithClaimInterval(interval time.Duration) Option {
	return func(c *config) {
		if interval > 0 {
			c.claimInterval = interval
		}
	}
}

// WithMaxLen задает приблизительный максимум длины стрима
// (по умолчанию 10000, 0 — без ограничения). Обрезка приблизительная (MAXLEN ~).
func WithMaxLen(maxLen int) Option {
	return func(c *config) {
		c.maxLen = maxLen
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

// WithDelayInterval задает период переноса задержанных задач (job.ETA)
// из sorted set в стрим (по умолчанию 100 миллисекунд).
func WithDelayInterval(interval time.Duration) Option {
	return func(c *config) {
		if interval > 0 {
			c.delayInterval = interval
		}
	}
}

// WithResultTTL задает TTL ключей backend (по умолчанию 24 часа,
// 0 — ключи не очищаются). Если Future не успел прочитать результат до
// истечения TTL, задача станет неотличима от отсутствующей.
func WithResultTTL(ttl time.Duration) Option {
	return func(c *config) {
		c.resultTTL = ttl
	}
}

// applyOptions применяет опции к конфигурации и доустанавливает значения
// по умолчанию, которые опции не затронули.
func applyOptions(cfg *config, opts []Option) {
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.consumer == "" {
		cfg.consumer = defaultConsumerName()
	}
}

// defaultConsumerName строит имя потребителя: "tq-<pid>-<8 hex>".
func defaultConsumerName() string {
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return "tq-" + strconv.Itoa(os.Getpid()) + "-" + hex.EncodeToString(buf)
}
