package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/denisdubovitskiy/taskq/internal"
)

// Client отправляет задачи в очередь.
type Client struct {
	broker  Broker
	backend Backend
	codec   Codec
	logger  Logger
	tracer  Tracer
	meter   Meter
	poll    time.Duration
}

// ClientOption — опция для Client.
type ClientOption func(*Client)

// NewClient создает клиента. Broker и Backend обязательны.
func NewClient(broker Broker, backend Backend, opts ...ClientOption) (*Client, error) {
	if broker == nil {
		return nil, errors.New("broker is required")
	}
	if backend == nil {
		return nil, errors.New("backend is required")
	}

	c := &Client{
		broker:  broker,
		backend: backend,
		codec:   NewJSONCodec(),
		logger:  noopLogger{},
		tracer:  noopTracer{},
		meter:   noopMeter{},
		poll:    50 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// WithCodec задает пользовательский кодек.
func WithCodec(codec Codec) ClientOption {
	return func(c *Client) {
		c.codec = codec
	}
}

// WithLogger задает логгер.
func WithLogger(logger Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithTracer задает трейсер.
func WithTracer(tracer Tracer) ClientOption {
	return func(c *Client) {
		c.tracer = tracer
	}
}

// WithMeter задает метрики.
func WithMeter(meter Meter) ClientOption {
	return func(c *Client) {
		c.meter = meter
	}
}

// SubmitOption — опция отправки задачи.
type SubmitOption func(*Job)

// WithRetry задает политику повторов.
func WithRetry(policy RetryPolicy) SubmitOption {
	return func(j *Job) {
		j.Retry = policy
	}
}

// WithETA откладывает выполнение задачи до указанного времени.
func WithETA(eta time.Time) SubmitOption {
	return func(j *Job) {
		j.ETA = &eta
	}
}

// WithDelay откладывает выполнение задачи на указанную длительность.
func WithDelay(d time.Duration) SubmitOption {
	return func(j *Job) {
		eta := time.Now().UTC().Add(d)
		j.ETA = &eta
	}
}

// WithHeader добавляет заголовок к задаче.
func WithHeader(key, value string) SubmitOption {
	return func(j *Job) {
		if j.Headers == nil {
			j.Headers = make(map[string]string)
		}
		j.Headers[key] = value
	}
}

// WithTimeout ограничивает время выполнения задачи от момента старта.
// При превышении контекст задачи отменяется; ошибка попадает в ретраи
// по обычной политике.
func WithTimeout(d time.Duration) SubmitOption {
	return func(j *Job) {
		if d > 0 {
			j.Timeout = d
		}
	}
}

// WithDeadline ограничивает время выполнения: задача должна завершиться
// не позже t. Если t уже в прошлом, каждый запуск завершится ошибкой таймаута.
// Если заданы и WithTimeout, и WithDeadline, действует WithDeadline.
func WithDeadline(t time.Time) SubmitOption {
	return func(j *Job) {
		if !t.IsZero() {
			utc := t.UTC()
			j.Deadline = &utc
		}
	}
}

// WithJobID назначает задаче явный идентификатор вместо сгенерированного.
// Если задача с таким ID уже зарегистрирована в backend, повторный Submit
// не публикует задачу в брокер и возвращает Future на существующую задачу
// (идемпотентность при повторных сабмитах клиента).
func WithJobID(id string) SubmitOption {
	return func(j *Job) {
		if id != "" {
			j.ID = id
		}
	}
}

// WithQueue задает очередь, в которую роутится задача.
// Переопределяет очередь по умолчанию задачи (Task.Queue).
// Пустое имя — очередь по умолчанию.
func WithQueue(queue string) SubmitOption {
	return func(j *Job) {
		j.Queue = queue
	}
}

// Submit отправляет задачу и возвращает Future для ожидания результата.
func Submit[T, R any](ctx context.Context, c *Client, task *Task[T, R], payload T, opts ...SubmitOption) (*Future[R], error) {
	if c == nil {
		return nil, errors.New("client is nil")
	}
	if task == nil {
		return nil, errors.New("task is nil")
	}

	ctx, span := c.tracer.Start(ctx, "taskq.Submit")
	defer span.End()

	jobID, err := c.publishJob(ctx, task.Name, task.Queue, payload, nil, opts...)
	if err != nil {
		span.SetError(err)
		return nil, err
	}

	return newFuture[R](c, jobID), nil
}

// SubmitOneWay отправляет задачу без ожидания результата.
func SubmitOneWay[T any](ctx context.Context, c *Client, task *Task[T, struct{}], payload T, opts ...SubmitOption) error {
	_, err := Submit[T, struct{}](ctx, c, task, payload, opts...)
	return err
}

// publishJob сериализует payload, сохраняет pending-состояние и публикует задачу.
// Возвращает идентификатор созданной задачи.
func (c *Client) publishJob(ctx context.Context, name, queue string, payload any, headers map[string]string, opts ...SubmitOption) (string, error) {
	data, err := c.codec.Encode(payload)
	if err != nil {
		return "", fmt.Errorf("encode payload: %w", err)
	}

	jobID, err := internal.GenerateID()
	if err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}

	now := time.Now().UTC()
	job := &Job{
		ID:        jobID,
		Name:      name,
		Queue:     queue,
		Payload:   data,
		State:     StatePending,
		CreatedAt: now,
		UpdatedAt: now,
		Retry: RetryPolicy{
			Multiplier: internal.DefaultRetryMultiplier,
		},
		Headers: headers,
	}
	for _, opt := range opts {
		opt(job)
	}

	// Идемпотентность: явный ID (WithJobID) уже зарегистрирован —
	// не публикуем повторно, возвращаем существующую задачу.
	if job.ID != jobID {
		if _, err := c.backend.GetState(ctx, job.ID); err == nil {
			c.logger.Info("task submit skipped: job already exists", "task", name, "job_id", job.ID)
			return job.ID, nil
		}
	}

	if store, ok := c.backend.(JobStore); ok {
		if err := store.SaveJob(ctx, *job); err != nil {
			return "", fmt.Errorf("save job document: %w", err)
		}
	}

	if err := c.backend.SetState(ctx, job.ID, StatePending); err != nil {
		return "", fmt.Errorf("set pending state: %w", err)
	}

	c.meter.Counter("taskq.submitted").Inc(ctx, MetricAttr{Key: "task", Value: name})

	if err := c.broker.Publish(ctx, job); err != nil {
		return "", fmt.Errorf("publish job: %w", err)
	}

	c.logger.Info("task submitted", "task", name, "job_id", job.ID)
	return job.ID, nil
}

// newFuture создает Future с декодером результата, привязанным к кодек клиента.
func newFuture[T any](c *Client, jobID string) *Future[T] {
	return &Future[T]{
		jobID:   jobID,
		backend: c.backend,
		poll:    c.poll,
		decode: func(data []byte) (T, error) {
			var r T
			if err := c.codec.Decode(data, &r); err != nil {
				return r, fmt.Errorf("decode result: %w", err)
			}
			return r, nil
		},
	}
}
