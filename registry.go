package taskq

import (
	"context"
	"fmt"
	"sync"
)

// Registry содержит зарегистрированные обработчики.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]internalHandler
}

// NewRegistry создает пустой реестр обработчиков.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]internalHandler),
	}
}

// Register связывает Task[T, R] с функцией-обработчиком.
func Register[T, R any](r *Registry, task *Task[T, R], handler func(context.Context, T) (R, error)) error {
	if r == nil {
		return fmt.Errorf("registry is nil")
	}
	if task == nil {
		return fmt.Errorf("task is nil")
	}
	if task.Name == "" {
		return fmt.Errorf("task name is empty")
	}
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[task.Name]; exists {
		return fmt.Errorf("task %q already registered", task.Name)
	}

	r.handlers[task.Name] = &typedHandler[T, R]{
		name:    task.Name,
		handler: handler,
		codec:   NewJSONCodec(),
	}
	return nil
}

// internalHandler — type-erased обработчик, используемый внутри ядра.
type internalHandler interface {
	Name() string
	Run(ctx context.Context, payload []byte) ([]byte, error)
}

type typedHandler[T, R any] struct {
	name    string
	handler func(context.Context, T) (R, error)
	codec   Codec
}

func (h *typedHandler[T, R]) Name() string {
	return h.name
}

func (h *typedHandler[T, R]) Run(ctx context.Context, payload []byte) ([]byte, error) {
	var t T
	if err := h.codec.Decode(payload, &t); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}

	r, err := h.handler(ctx, t)
	if err != nil {
		return nil, err
	}

	data, err := h.codec.Encode(r)
	if err != nil {
		return nil, fmt.Errorf("encode result: %w", err)
	}

	return data, nil
}

func (r *Registry) get(name string) (internalHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}
