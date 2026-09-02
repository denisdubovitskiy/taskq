package taskq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/denisdubovitskiy/taskq/internal"
)

// NewChain создает сборщик пустой цепочки задач.
func NewChain(c *Client) (*ChainBuilder[struct{}], error) {
	if c == nil {
		return nil, errors.New("client is nil")
	}
	return &ChainBuilder[struct{}]{core: &chainBuilderCore{client: c}}, nil
}

// ChainBuilder собирает цепочку задач разных типов.
// R — тип результата последнего добавленного шага.
type ChainBuilder[R any] struct {
	core *chainBuilderCore
}

// Add добавляет задачу в цепочку.
// Для первого шага payload передается обязательно, для последующих — нет:
// payload задачи десериализуется из результата предыдущего шага.
// Возвращает сборщик с типом результата добавленной задачи.
func Add[R, T, S any](b *ChainBuilder[R], task *Task[T, S], payload ...T) *ChainBuilder[S] {
	step := chainStep{}
	if task != nil {
		step.name = task.Name
		step.queue = task.Queue
	}
	if len(payload) > 0 {
		step.hasPayload = true
		step.payload = payload[0]
	}
	b.core.steps = append(b.core.steps, step)
	return &ChainBuilder[S]{core: b.core}
}

// chainBuilderCore — общее состояние цепочки, разделяемое сборщиками всех шагов.
type chainBuilderCore struct {
	client  *Client
	steps   []chainStep
	stepIDs []string
}

// chainStep — внутреннее представление шага цепочки.
type chainStep struct {
	name       string
	queue      string
	hasPayload bool
	payload    any
}

// StepIDs возвращает идентификаторы задач цепочки в порядке шагов.
// Осмысленно после Send.
func (b *ChainBuilder[R]) StepIDs() []string {
	ids := make([]string, len(b.core.stepIDs))
	copy(ids, b.core.stepIDs)
	return ids
}

// Send отправляет цепочку и возвращает handle для конечного результата.
// Публикуется только первый шаг; воркер переводит цепочку дальше по ходу выполнения.
func (b *ChainBuilder[R]) Send(ctx context.Context) (*Future[R], error) {
	if b.core.client == nil {
		return nil, errors.New("client is nil")
	}
	if len(b.core.steps) == 0 {
		return nil, errors.New("chain is empty")
	}
	if !b.core.steps[0].hasPayload {
		return nil, errors.New("the first step of the chain requires a payload")
	}
	for i := 1; i < len(b.core.steps); i++ {
		if b.core.steps[i].hasPayload {
			return nil, fmt.Errorf("step %d of the chain must not have a payload: it receives the result of the previous step", i)
		}
	}
	for i, s := range b.core.steps {
		if s.name == "" {
			return nil, fmt.Errorf("step %d of the chain: task is nil or the name is empty", i)
		}
	}

	c := b.core.client
	ctx, span := c.tracer.Start(ctx, "taskq.Chain.Send")
	defer span.End()

	chainID, err := internal.GenerateID()
	if err != nil {
		return nil, fmt.Errorf("generate chain id: %w", err)
	}

	ids := make([]string, len(b.core.steps))
	refs := make([]chainStepRef, len(b.core.steps))
	for i, s := range b.core.steps {
		id, err := internal.GenerateID()
		if err != nil {
			return nil, fmt.Errorf("generate job id: %w", err)
		}
		ids[i] = id
		refs[i] = chainStepRef{ID: id, Task: s.name, Queue: s.queue}
	}

	stepsJSON, err := encodeChainSteps(refs)
	if err != nil {
		return nil, err
	}

	for i, id := range ids {
		if err := c.backend.SetState(ctx, id, StatePending); err != nil {
			return nil, fmt.Errorf("set pending state of step %d: %w", i, err)
		}
	}

	firstPayload, err := c.codec.Encode(b.core.steps[0].payload)
	if err != nil {
		return nil, fmt.Errorf("encode payload of the first step: %w", err)
	}

	first := &Job{
		ID:        ids[0],
		Name:      b.core.steps[0].name,
		Queue:     b.core.steps[0].queue,
		Payload:   firstPayload,
		State:     StatePending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Retry: RetryPolicy{
			Multiplier: internal.DefaultRetryMultiplier,
		},
		Headers: map[string]string{
			HeaderChainID:    chainID,
			HeaderChainStep:  "0",
			HeaderChainSteps: stepsJSON,
		},
	}

	if err := c.broker.Publish(ctx, first); err != nil {
		span.SetError(err)
		return nil, fmt.Errorf("publish the first step: %w", err)
	}

	b.core.stepIDs = ids
	c.meter.Counter("taskq.submitted").Inc(ctx, MetricAttr{Key: "task", Value: first.Name})
	c.logger.Info("chain submitted", "chain_id", chainID, "steps", strconv.Itoa(len(b.core.steps)))

	return newFuture[R](c, ids[len(ids)-1]), nil
}
