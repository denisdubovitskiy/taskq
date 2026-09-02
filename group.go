package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/denisdubovitskiy/taskq/internal"
)

// GroupOption — опция отправки группы задач. Применяется к каждой задаче группы.
type GroupOption = SubmitOption

// SubmitGroup отправляет группу задач одного типа параллельно.
// Возвращает handle для ожидания результатов всех задач.
func SubmitGroup[T, R any](ctx context.Context, c *Client, task *Task[T, R], payloads []T, opts ...GroupOption) (*GroupResult[R], error) {
	if c == nil {
		return nil, errors.New("client is nil")
	}
	if task == nil {
		return nil, errors.New("task is nil")
	}
	if len(payloads) == 0 {
		return nil, errors.New("payloads must not be empty")
	}

	ctx, span := c.tracer.Start(ctx, "taskq.SubmitGroup")
	defer span.End()

	groupID, err := internal.GenerateID()
	if err != nil {
		return nil, fmt.Errorf("generate group id: %w", err)
	}

	g := &GroupResult[R]{
		groupID: groupID,
		jobIDs:  make([]string, 0, len(payloads)),
		backend: c.backend,
		poll:    c.poll,
	}

	for _, payload := range payloads {
		jobID, err := c.publishJob(ctx, task.Name, task.Queue, payload, nil, opts...)
		if err != nil {
			span.SetError(err)
			return nil, err
		}
		g.jobIDs = append(g.jobIDs, jobID)
	}

	g.decode = func(data []byte) (R, error) {
		var r R
		if err := c.codec.Decode(data, &r); err != nil {
			return r, fmt.Errorf("decode result: %w", err)
		}
		return r, nil
	}

	c.logger.Info("group submitted", "group_id", groupID, "task", task.Name, "size", fmt.Sprintf("%d", len(payloads)))
	return g, nil
}

// GroupResult[R] — handle для ожидания результатов группы и сами результаты.
type GroupResult[R any] struct {
	// Results — результаты задач в исходном порядке. Для упавшей задачи — zero value.
	// Доступны после возврата Get или GetWithTimeout.
	Results []R
	// Errors — ошибки задач в исходном порядке. Для успешной задачи — nil.
	// Доступны после возврата Get или GetWithTimeout.
	Errors []error

	groupID string
	jobIDs  []string
	backend Backend
	poll    time.Duration
	decode  func([]byte) (R, error)
}

// ID возвращает идентификатор группы.
func (g *GroupResult[R]) ID() string {
	return g.groupID
}

// JobIDs возвращает идентификаторы задач группы в исходном порядке.
func (g *GroupResult[R]) JobIDs() []string {
	ids := make([]string, len(g.jobIDs))
	copy(ids, g.jobIDs)
	return ids
}

// Get блокируется до завершения всех задач группы (успешно или с ошибкой).
// Ошибки отдельных задач не прерывают ожидание: они фиксируются в поле Errors.
// Ошибка возвращается при отмене контекста или при сбое чтения backend.
func (g *GroupResult[R]) Get(ctx context.Context) (*GroupResult[R], error) {
	for {
		done, err := g.collect(ctx)
		if err != nil {
			return nil, err
		}
		if done {
			return g, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(g.pollInterval()):
		}
	}
}

// GetWithTimeout — удобная обертка над Get с таймаутом.
func (g *GroupResult[R]) GetWithTimeout(ctx context.Context, timeout time.Duration) (*GroupResult[R], error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return g.Get(ctx)
}

// AllSucceeded возвращает, завершили ли все задачи группы работу успешно.
// Осмысленно после возврата Get или GetWithTimeout.
func (g *GroupResult[R]) AllSucceeded() bool {
	for _, err := range g.Errors {
		if err != nil {
			return false
		}
	}
	return true
}

// collect читает состояния всех задач группы.
// Возвращает true, когда все задачи в терминальном состоянии.
func (g *GroupResult[R]) collect(ctx context.Context) (bool, error) {
	results := make([]R, len(g.jobIDs))
	errs := make([]error, len(g.jobIDs))

	for i, jobID := range g.jobIDs {
		state, err := g.backend.GetState(ctx, jobID)
		if err != nil {
			return false, fmt.Errorf("get state of job %s: %w", jobID, err)
		}

		switch state.State {
		case StateSuccess:
			res, err := g.backend.GetResult(ctx, jobID)
			if err != nil {
				return false, fmt.Errorf("get result of job %s: %w", jobID, err)
			}
			decoded, err := g.decode(res.Data)
			if err != nil {
				return false, err
			}
			results[i] = decoded
		case StateFailure:
			errs[i] = errors.New(state.Error)
		case StateDead:
			errs[i] = fmt.Errorf("job is dead: %s", state.Error)
		case StateCanceled:
			errs[i] = ErrJobCanceled
		default:
			return false, nil
		}
	}

	g.Results = results
	g.Errors = errs
	return true, nil
}

func (g *GroupResult[R]) pollInterval() time.Duration {
	if g.poll > 0 {
		return g.poll
	}
	return 50 * time.Millisecond
}
