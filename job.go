package taskq

import (
	"context"
	"errors"
	"time"
)

// ErrJobNotFound — задача отсутствует в backend.
var ErrJobNotFound = errors.New("job not found")

// ErrJobCanceled — задача отменена.
var ErrJobCanceled = errors.New("job canceled")

// ErrStateConflict — действие противоречит текущему состоянию задачи.
var ErrStateConflict = errors.New("state conflict")

// State — состояние выполнения задачи.
type State string

// Возможные состояния задачи.
const (
	StatePending  State = "pending"
	StateReceived State = "received"
	StateStarted  State = "started"
	StateRetry    State = "retry"
	StateSuccess  State = "success"
	StateFailure  State = "failure"
	StateDead     State = "dead"     // ретраи исчерпаны — dead-letter
	StateCanceled State = "canceled" // задача отменена клиентом или воркером
)

// Job — единица работы, передаваемая через брокер и хранимая в backend.
// Payload и Result хранятся как []byte, чтобы не зависеть от формата сериализации.
type Job struct {
	ID        string
	Name      string
	Queue     string
	Payload   []byte
	State     State
	Attempt   uint32
	CreatedAt time.Time
	UpdatedAt time.Time
	ETA       *time.Time
	Retry     RetryPolicy
	Headers   map[string]string
	// Ограничения выполнения: Timeout — от начала исполнения,
	// Deadline — не позже указанного момента. Если заданы оба, действует Deadline.
	Timeout  time.Duration
	Deadline *time.Time
	// Error — последняя ошибка задачи (заполняется backend при Inspect).
	Error string
}

// RetryPolicy описывает политику повторов.
type RetryPolicy struct {
	MaxAttempts  uint32
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64 // экспоненциальный backoff, по умолчанию 2.0
}

// AckType сообщает брокеру, что делать с сообщением после обработки.
type AckType int

// Варианты подтверждения обработки сообщения.
const (
	AckAck AckType = iota
	AckNackRequeue
	AckNackDrop
)

// Delivery — единица доставки от брокера.
type Delivery struct {
	ID      string
	Body    []byte
	Headers map[string]string
	Ack     func(ctx context.Context) error
	Nack    func(ctx context.Context, requeue bool) error
}

// JobState — текущее состояние задачи в backend.
type JobState struct {
	State     State
	Error     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// JobResult — сериализованный результат задачи.
type JobResult struct {
	Data []byte
}
