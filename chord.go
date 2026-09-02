package taskq

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/denisdubovitskiy/taskq/internal"
)

// ChordBackend — backend с поддержкой состояния аккордов.
// Нужен для отслеживания завершения задач группы аккорда
// и запуска callback-задачи ровно один раз.
type ChordBackend interface {
	Backend

	// ChordInit инициализирует состояние аккорда с total задачами в группе.
	ChordInit(ctx context.Context, chordID string, total int) error

	// ChordFinish фиксирует успешное завершение задачи с индексом index.
	// Когда завершены все задачи группы, возвращает true и результаты в порядке индексов.
	// Если аккорд уже завершён (какая-то задача упала), возвращает (false, nil, nil).
	ChordFinish(ctx context.Context, chordID string, index int, result []byte) (bool, []ChordResult, error)

	// ChordFail фиксирует окончательный сбой задачи с индексом index.
	// Если аккорд еще не завершён, помечает его упавшим и возвращает true.
	ChordFail(ctx context.Context, chordID string, index int, errMsg string) (bool, error)
}

// ChordResult — результат задачи в группе аккорда.
type ChordResult struct {
	// Index — индекс задачи в группе.
	Index int
	// Result — сериализованный результат задачи.
	Result []byte
}

// SubmitChord отправляет аккорд: группу задач одного типа и callback,
// выполняющийся после успешного завершения всех задач группы.
// Payload callback-задачи — список результатов группы []R в исходном порядке.
// Если какая-либо задача группы упала, callback не вызывается.
func SubmitChord[T, R, S any](ctx context.Context, c *Client, groupTask *Task[T, R], payloads []T, callback *Task[[]R, S], opts ...GroupOption) (*Future[S], error) {
	if c == nil {
		return nil, errors.New("client is nil")
	}
	if groupTask == nil {
		return nil, errors.New("group task is nil")
	}
	if callback == nil {
		return nil, errors.New("callback task is nil")
	}
	if len(payloads) == 0 {
		return nil, errors.New("payloads must not be empty")
	}

	cb, ok := c.backend.(ChordBackend)
	if !ok {
		return nil, errors.New("backend does not support chords: implement ChordBackend")
	}

	ctx, span := c.tracer.Start(ctx, "taskq.SubmitChord")
	defer span.End()

	chordID, err := internal.GenerateID()
	if err != nil {
		return nil, fmt.Errorf("generate chord id: %w", err)
	}
	callbackID, err := internal.GenerateID()
	if err != nil {
		return nil, fmt.Errorf("generate callback id: %w", err)
	}

	if err := cb.ChordInit(ctx, chordID, len(payloads)); err != nil {
		span.SetError(err)
		return nil, fmt.Errorf("init chord: %w", err)
	}

	if err := c.backend.SetState(ctx, callbackID, StatePending); err != nil {
		span.SetError(err)
		return nil, fmt.Errorf("set pending state of the callback: %w", err)
	}

	total := strconv.Itoa(len(payloads))
	for i, payload := range payloads {
		headers := map[string]string{
			HeaderChordID:            chordID,
			HeaderChordIndex:         strconv.Itoa(i),
			HeaderChordTotal:         total,
			HeaderChordCallbackTask:  callback.Name,
			HeaderChordCallbackQueue: callback.Queue,
			HeaderChordCallbackID:    callbackID,
		}

		if _, err := c.publishJob(ctx, groupTask.Name, groupTask.Queue, payload, headers, opts...); err != nil {
			span.SetError(err)
			return nil, err
		}
	}

	c.logger.Info("chord submitted", "chord_id", chordID, "task", groupTask.Name, "size", total)
	return newFuture[S](c, callbackID), nil
}
