package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrTimeoutReached возвращается, когда истекло время ожидания результата.
var ErrTimeoutReached = errors.New("timeout reached")

// Task[T, R] — типизированная задача.
// T — тип аргумента (payload), сериализуется в JSON.
// R — тип результата, десериализуется из JSON backend.
type Task[T, R any] struct {
	Name  string
	Queue string
}

// NewTask создает типизированную задачу.
func NewTask[T, R any](name string) *Task[T, R] {
	return &Task[T, R]{Name: name}
}

// WithQueue возвращает ту же задачу, но с привязкой к очереди.
func (t *Task[T, R]) WithQueue(queue string) *Task[T, R] {
	return &Task[T, R]{Name: t.Name, Queue: queue}
}

// Future[R] — handle для ожидания результата асинхронной задачи.
type Future[R any] struct {
	jobID   string
	backend Backend
	decode  func([]byte) (R, error)
	poll    time.Duration
}

// ID возвращает идентификатор задачи.
func (f *Future[R]) ID() string {
	return f.jobID
}

// Get блокируется до завершения задачи или отмены контекста.
func (f *Future[R]) Get(ctx context.Context) (R, error) {
	var zero R
	for {
		result, state, err := f.Touch(ctx)
		if err != nil {
			return zero, err
		}
		if state != nil {
			switch state.State {
			case StateSuccess:
				return result, nil
			case StateFailure:
				return zero, errors.New(state.Error)
			case StateDead:
				return zero, fmt.Errorf("job is dead: %s", state.Error)
			case StateCanceled:
				return zero, ErrJobCanceled
			}
		}

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(f.pollInterval()):
		}
	}
}

// GetWithTimeout — удобная обертка с таймаутом.
func (f *Future[R]) GetWithTimeout(ctx context.Context, timeout time.Duration) (R, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return f.Get(ctx)
}

// Touch проверяет состояние задачи, не блокируясь.
// Возвращает zero value и nil state, если задача еще не завершена.
func (f *Future[R]) Touch(ctx context.Context) (R, *JobState, error) {
	var zero R

	state, err := f.backend.GetState(ctx, f.jobID)
	if err != nil {
		return zero, nil, err
	}

	if state.State != StateSuccess {
		return zero, state, nil
	}

	res, err := f.backend.GetResult(ctx, f.jobID)
	if err != nil {
		return zero, nil, err
	}

	decoded, err := f.decode(res.Data)
	if err != nil {
		return zero, nil, err
	}

	return decoded, state, nil
}

func (f *Future[R]) pollInterval() time.Duration {
	if f.poll > 0 {
		return f.poll
	}
	return 50 * time.Millisecond
}
