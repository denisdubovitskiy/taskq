package taskq

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// continueAfterSuccess продолжает оркестрацию после успешного завершения задачи:
// запускает следующий шаг цепочки и завершает аккорд, если вся группа выполнилась.
func (w *Worker) continueAfterSuccess(ctx context.Context, job *Job, result []byte) error {
	if err := w.continueChain(ctx, job, result); err != nil {
		return fmt.Errorf("continue chain: %w", err)
	}
	if err := w.finishChord(ctx, job, result); err != nil {
		return fmt.Errorf("finish chord: %w", err)
	}
	return nil
}

// chainStepInfo читает данные цепочки из заголовков задачи.
func (w *Worker) chainStepInfo(job *Job) ([]chainStepRef, int, error) {
	data, ok := job.Headers[HeaderChainSteps]
	if !ok {
		return nil, 0, fmt.Errorf("chain steps header is missing")
	}

	steps, err := decodeChainSteps(data)
	if err != nil {
		return nil, 0, err
	}

	stepStr, ok := job.Headers[HeaderChainStep]
	if !ok {
		return nil, 0, fmt.Errorf("chain step header is missing")
	}
	step, err := strconv.Atoi(stepStr)
	if err != nil {
		return nil, 0, fmt.Errorf("parse chain step: %w", err)
	}

	return steps, step, nil
}

// continueChain запускает следующий шаг цепочки, передавая результат текущего шага как payload.
func (w *Worker) continueChain(ctx context.Context, job *Job, result []byte) error {
	if _, ok := job.Headers[HeaderChainID]; !ok {
		return nil
	}

	steps, step, err := w.chainStepInfo(job)
	if err != nil {
		return err
	}
	if step+1 >= len(steps) {
		return nil
	}

	next := steps[step+1]
	now := time.Now().UTC()
	nextJob := &Job{
		ID:        next.ID,
		Name:      next.Task,
		Queue:     next.Queue,
		Payload:   result,
		State:     StatePending,
		CreatedAt: now,
		UpdatedAt: now,
		Headers: map[string]string{
			HeaderChainID:    job.Headers[HeaderChainID],
			HeaderChainStep:  strconv.Itoa(step + 1),
			HeaderChainSteps: job.Headers[HeaderChainSteps],
		},
	}

	if err := w.backend.SetState(ctx, nextJob.ID, StatePending); err != nil {
		return fmt.Errorf("set pending state of the next step: %w", err)
	}
	if err := w.broker.Publish(ctx, nextJob); err != nil {
		return fmt.Errorf("publish the next step: %w", err)
	}

	w.logger.Info("chain continued", "chain_id", job.Headers[HeaderChainID], "next_task", next.Task, "next_job_id", next.ID)
	return nil
}

// failChainRest помечает все остальные шаги цепочки как failed.
func (w *Worker) failChainRest(ctx context.Context, job *Job, cause error) {
	if _, ok := job.Headers[HeaderChainID]; !ok {
		return
	}

	steps, step, err := w.chainStepInfo(job)
	if err != nil {
		w.logger.Error("decode chain steps", "job_id", job.ID, "error", err.Error())
		return
	}

	msg := fmt.Sprintf("chain interrupted at step %d: %s", step, cause.Error())
	for i := step + 1; i < len(steps); i++ {
		if setErr := w.backend.SetError(ctx, steps[i].ID, msg); setErr != nil {
			w.logger.Error("failed to set chain step error", "job_id", steps[i].ID, "error", setErr.Error())
		}
		if setErr := w.backend.SetState(ctx, steps[i].ID, StateFailure); setErr != nil {
			w.logger.Error("failed to set chain step failure state", "job_id", steps[i].ID, "error", setErr.Error())
		}
	}
}

// finishChord фиксирует завершение задачи группы аккорда
// и запускает callback, когда вся группа выполнена успешно.
func (w *Worker) finishChord(ctx context.Context, job *Job, result []byte) error {
	chordID, ok := job.Headers[HeaderChordID]
	if !ok {
		return nil
	}

	cb, ok := w.backend.(ChordBackend)
	if !ok {
		w.logger.Error("backend does not support chords", "job_id", job.ID)
		return nil
	}

	index, err := strconv.Atoi(job.Headers[HeaderChordIndex])
	if err != nil {
		return fmt.Errorf("parse chord index: %w", err)
	}

	completed, results, err := cb.ChordFinish(ctx, chordID, index, result)
	if err != nil {
		return fmt.Errorf("chord finish: %w", err)
	}
	if !completed {
		return nil
	}

	return w.publishChordCallback(ctx, job, results)
}

// failChord помечает аккорд как упавший; первая ошибка в группе
// помечает callback как failed.
func (w *Worker) failChord(ctx context.Context, job *Job, cause error) {
	chordID, ok := job.Headers[HeaderChordID]
	if !ok {
		return
	}

	cb, ok := w.backend.(ChordBackend)
	if !ok {
		return
	}

	index, err := strconv.Atoi(job.Headers[HeaderChordIndex])
	if err != nil {
		w.logger.Error("parse chord index", "job_id", job.ID, "error", err.Error())
		return
	}

	triggered, err := cb.ChordFail(ctx, chordID, index, cause.Error())
	if err != nil {
		w.logger.Error("chord fail", "job_id", job.ID, "error", err.Error())
		return
	}
	if !triggered {
		return
	}

	callbackID := job.Headers[HeaderChordCallbackID]
	if callbackID == "" {
		return
	}

	msg := fmt.Sprintf("chord %s interrupted at task %d: %s", chordID, index, cause.Error())
	if setErr := w.backend.SetError(ctx, callbackID, msg); setErr != nil {
		w.logger.Error("failed to set chord callback error", "job_id", callbackID, "error", setErr.Error())
	}
	if setErr := w.backend.SetState(ctx, callbackID, StateFailure); setErr != nil {
		w.logger.Error("failed to set chord callback failure state", "job_id", callbackID, "error", setErr.Error())
	}
}

// publishChordCallback публикует callback-задачу с результатами группы.
func (w *Worker) publishChordCallback(ctx context.Context, job *Job, results []ChordResult) error {
	payload := make([]byte, 0, 64)
	payload = append(payload, '[')
	for i, r := range results {
		if i > 0 {
			payload = append(payload, ',')
		}
		payload = append(payload, r.Result...)
	}
	payload = append(payload, ']')

	now := time.Now().UTC()
	callback := &Job{
		ID:        job.Headers[HeaderChordCallbackID],
		Name:      job.Headers[HeaderChordCallbackTask],
		Queue:     job.Headers[HeaderChordCallbackQueue],
		Payload:   payload,
		State:     StatePending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := w.backend.SetState(ctx, callback.ID, StatePending); err != nil {
		return fmt.Errorf("set pending state of the callback: %w", err)
	}
	if err := w.broker.Publish(ctx, callback); err != nil {
		return fmt.Errorf("publish chord callback: %w", err)
	}

	w.logger.Info("chord callback published", "chord_id", job.Headers[HeaderChordID], "callback_task", callback.Name)
	return nil
}
