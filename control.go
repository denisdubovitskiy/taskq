package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Cancel отменяет задачу. Если задача еще не начата — она не будет выполнена;
// если уже выполняется в этом процессе — ее прерывает Worker.Cancel.
//
// Отмена разрешена из states pending, received, started, retry.
// Для терминальных задач (success, failure, dead, canceled) возвращает
// ошибку, обёртывающую ErrStateConflict.
//
// Внимание: исполнение в другом процессе отмена не прерывает — воркер
// проверит отмену при получении задачи.
func (c *Client) Cancel(ctx context.Context, jobID string) error {
	if c == nil {
		return errors.New("client is nil")
	}
	if jobID == "" {
		return errors.New("job id is required")
	}

	state, err := c.backend.GetState(ctx, jobID)
	if err != nil {
		return fmt.Errorf("inspect job %s: %w", jobID, err)
	}

	switch state.State {
	case StatePending, StateReceived, StateStarted, StateRetry:
	default:
		return fmt.Errorf("cannot cancel job in state %s: %w", state.State, ErrStateConflict)
	}

	if err := c.backend.SetState(ctx, jobID, StateCanceled); err != nil {
		return fmt.Errorf("set canceled state: %w", err)
	}

	c.logger.Info("task canceled", "job_id", jobID)
	return nil
}

// Rescue возвращает упавшую задачу (failure или dead) в очередь:
// сбрасывает попытку и ограничения и перепубликует документ в брокер.
// Требует, чтобы backend реализовывал JobInspector и JobStore.
func (c *Client) Rescue(ctx context.Context, jobID string) error {
	if c == nil {
		return errors.New("client is nil")
	}
	if jobID == "" {
		return errors.New("job id is required")
	}

	inspector, ok := c.backend.(JobInspector)
	if !ok {
		return errors.New("backend does not implement JobInspector, rescue is not supported")
	}

	job, err := inspector.Inspect(ctx, jobID)
	if err != nil {
		return fmt.Errorf("inspect job %s: %w", jobID, err)
	}

	switch job.State {
	case StateFailure, StateDead:
	default:
		return fmt.Errorf("cannot rescue job in state %s: %w", job.State, ErrStateConflict)
	}

	if err := inspector.Reset(ctx, jobID); err != nil {
		return fmt.Errorf("reset job %s: %w", jobID, err)
	}

	job.State = StatePending
	job.Attempt = 0
	job.ETA = nil
	job.UpdatedAt = time.Now().UTC()

	if err := c.broker.Publish(ctx, job); err != nil {
		return fmt.Errorf("republish rescued job: %w", err)
	}

	c.meter.Counter("taskq.rescued").Inc(ctx, MetricAttr{Key: "task", Value: job.Name})
	c.logger.Info("task rescued", "task", job.Name, "job_id", jobID)
	return nil
}

// Inspect возвращает полный документ задачи с актуальным состоянием.
func (c *Client) Inspect(ctx context.Context, jobID string) (*Job, error) {
	if c == nil {
		return nil, errors.New("client is nil")
	}
	if jobID == "" {
		return nil, errors.New("job id is required")
	}

	inspector, ok := c.backend.(JobInspector)
	if !ok {
		return nil, errors.New("backend does not implement JobInspector, inspect is not supported")
	}

	return inspector.Inspect(ctx, jobID)
}

// List возвращает страницу задач по фильтру.
func (c *Client) List(ctx context.Context, q ListQuery) (ListResult, error) {
	if c == nil {
		return ListResult{}, errors.New("client is nil")
	}

	inspector, ok := c.backend.(JobInspector)
	if !ok {
		return ListResult{}, errors.New("backend does not implement JobInspector, list is not supported")
	}

	return inspector.List(ctx, q)
}

// Delete полностью удаляет задачу из backend (состояние, результат, документ).
func (c *Client) Delete(ctx context.Context, jobID string) error {
	if c == nil {
		return errors.New("client is nil")
	}
	if jobID == "" {
		return errors.New("job id is required")
	}

	inspector, ok := c.backend.(JobInspector)
	if !ok {
		return errors.New("backend does not implement JobInspector, delete is not supported")
	}

	return inspector.Delete(ctx, jobID)
}
