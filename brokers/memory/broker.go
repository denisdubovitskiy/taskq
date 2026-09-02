package memory

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/denisdubovitskiy/taskq"
)

// Broker — in-memory брокер для тестов.
type Broker struct {
	mu     sync.RWMutex
	queues map[string]chan *taskq.Job
	closed bool
}

// NewBroker создает in-memory брокер.
func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]chan *taskq.Job),
	}
}

// Publish кладет задачу в очередь.
func (b *Broker) Publish(ctx context.Context, job *taskq.Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return context.Canceled
	}

	queue := b.queueForLocked(job.Queue)
	select {
	case queue <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consume читает задачи из очереди и передает их handler.
func (b *Broker) Consume(ctx context.Context, queue string, handler taskq.DeliveryHandler) error {
	ch := b.queueFor(queue)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job := <-ch:
			body, err := json.Marshal(job)
			if err != nil {
				continue
			}

			delivery := &taskq.Delivery{
				ID:      job.ID,
				Body:    body,
				Headers: job.Headers,
				Ack:     func(context.Context) error { return nil },
				Nack:    func(context.Context, bool) error { return nil },
			}

			handler.Handle(ctx, delivery)
		}
	}
}

// Close закрывает брокер.
func (b *Broker) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *Broker) queueFor(name string) chan *taskq.Job {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.queueForLocked(name)
}

func (b *Broker) queueForLocked(name string) chan *taskq.Job {
	if name == "" {
		name = "default"
	}

	if q, ok := b.queues[name]; ok {
		return q
	}

	q := make(chan *taskq.Job, 1024)
	b.queues[name] = q
	return q
}
