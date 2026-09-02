package taskq

import (
	"context"
	"time"
)

// Broker — транспорт для задач. Ядро не знает, это Redis, Kafka или in-memory очередь.
type Broker interface {
	Publish(ctx context.Context, job *Job) error
	Consume(ctx context.Context, queue string, handler DeliveryHandler) error
	Close(ctx context.Context) error
}

// DeliveryHandler вызывается брокером при получении сообщения.
type DeliveryHandler interface {
	Handle(ctx context.Context, delivery *Delivery) AckType
}

// Backend — хранилище состояний и результатов.
type Backend interface {
	SetState(ctx context.Context, jobID string, state State) error
	SetResult(ctx context.Context, jobID string, data []byte) error
	SetError(ctx context.Context, jobID string, err string) error
	GetState(ctx context.Context, jobID string) (*JobState, error)
	GetResult(ctx context.Context, jobID string) (*JobResult, error)
	Purge(ctx context.Context, jobID string) error
	Close(ctx context.Context) error
}

// Locker — распределенная блокировка для периодических задач.
type Locker interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}

// Lock — acquired lock.
type Lock interface {
	Release(ctx context.Context) error
}
