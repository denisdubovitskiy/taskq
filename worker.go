package taskq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/denisdubovitskiy/taskq/internal"
)

// inFlightJob — задача, выполняемая в данный момент этим воркером.
type inFlightJob struct {
	cancel          context.CancelFunc
	cancelRequested atomic.Bool
}

// Worker выбирает задачи из брокера и исполняет их.
type Worker struct {
	registry        *Registry
	broker          Broker
	backend         Backend
	codec           Codec
	logger          Logger
	tracer          Tracer
	meter           Meter
	concurrency     int
	semaphore       chan struct{}
	taskConcurrency map[string]int
	taskSemaphores  map[string]chan struct{}
	preExec         func(context.Context, *Job) error
	postExec        func(context.Context, *Job, State, error)
	errorHandler    func(context.Context, *Job, error) error
	deadHook        func(context.Context, *Job, error)
	// lifecycleMu сериализует старт RunQueues и Shutdown: защищает пару
	// «маркер Run в wg» + close(stopCh) от гонки Add/Wait по sync.WaitGroup.
	lifecycleMu sync.Mutex
	wg          sync.WaitGroup
	inFlightMu  sync.Mutex
	inFlight    map[string]*inFlightJob
	stopCh      chan struct{}
	stopOnce    sync.Once
}

// WorkerOption — опция для Worker.
type WorkerOption func(*Worker)

// NewWorker создает обработчик с заданной concurrency.
func NewWorker(registry *Registry, broker Broker, backend Backend, opts ...WorkerOption) (*Worker, error) {
	if registry == nil {
		return nil, errors.New("registry is required")
	}
	if broker == nil {
		return nil, errors.New("broker is required")
	}
	if backend == nil {
		return nil, errors.New("backend is required")
	}

	w := &Worker{
		registry:        registry,
		broker:          broker,
		backend:         backend,
		codec:           NewJSONCodec(),
		logger:          noopLogger{},
		tracer:          noopTracer{},
		meter:           noopMeter{},
		concurrency:     1,
		taskConcurrency: make(map[string]int),
		inFlight:        make(map[string]*inFlightJob),
		stopCh:          make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	if w.concurrency < 1 {
		w.concurrency = 1
	}
	w.semaphore = make(chan struct{}, w.concurrency)
	w.taskSemaphores = make(map[string]chan struct{}, len(w.taskConcurrency))
	for name, n := range w.taskConcurrency {
		if name != "" && n > 0 {
			w.taskSemaphores[name] = make(chan struct{}, n)
		}
	}

	return w, nil
}

// WithWorkerCodec задает кодек для worker.
func WithWorkerCodec(codec Codec) WorkerOption {
	return func(w *Worker) {
		w.codec = codec
	}
}

// WithWorkerLogger задает логгер для worker.
func WithWorkerLogger(logger Logger) WorkerOption {
	return func(w *Worker) {
		w.logger = logger
	}
}

// WithWorkerTracer задает трейсер для worker.
func WithWorkerTracer(tracer Tracer) WorkerOption {
	return func(w *Worker) {
		w.tracer = tracer
	}
}

// WithWorkerMeter задает метрики для worker.
func WithWorkerMeter(meter Meter) WorkerOption {
	return func(w *Worker) {
		w.meter = meter
	}
}

// WithConcurrency задает уровень конкурентности: общее число задач,
// выполняемых воркером одновременно по всем очередям и задачам.
func WithConcurrency(n int) WorkerOption {
	return func(w *Worker) {
		w.concurrency = n
	}
}

// WithTaskConcurrency задает максимальное число одновременных запусков
// задачи с указанным именем. Лимит действует внутри общего пула
// (WithConcurrency): суммарная конкурентность воркера по-прежнему
// ограничена им. Задачи без явного лимита ограничены только общим пулом.
// Значение n <= 0 игнорируется.
func WithTaskConcurrency(task string, n int) WorkerOption {
	return func(w *Worker) {
		if task != "" {
			w.taskConcurrency[task] = n
		}
	}
}

// WithPreExecuteHook добавляет перехватчик перед выполнением задачи.
func WithPreExecuteHook(hook func(context.Context, *Job) error) WorkerOption {
	return func(w *Worker) {
		w.preExec = hook
	}
}

// WithPostExecuteHook добавляет перехватчик после выполнения задачи.
func WithPostExecuteHook(hook func(context.Context, *Job, State, error)) WorkerOption {
	return func(w *Worker) {
		w.postExec = hook
	}
}

// WithErrorHandler добавляет обработчик ошибок.
func WithErrorHandler(handler func(context.Context, *Job, error) error) WorkerOption {
	return func(w *Worker) {
		w.errorHandler = handler
	}
}

// WithOnDeadHook добавляет обработчик dead-letter: вызывается один раз,
// когда ретраи задачи исчерпаны и она переводится в state dead.
func WithOnDeadHook(hook func(context.Context, *Job, error)) WorkerOption {
	return func(w *Worker) {
		w.deadHook = hook
	}
}

// Cancel прерывает задачу, выполняемую в данный момент этим воркером,
// отменяя контекст задачи. Возвращает false, если задача не выполняется
// в этом инстансе (межпроцессная отмена — через Client.Cancel).
func (w *Worker) Cancel(jobID string) bool {
	w.inFlightMu.Lock()
	entry, ok := w.inFlight[jobID]
	w.inFlightMu.Unlock()
	if !ok {
		return false
	}

	entry.cancelRequested.Store(true)
	entry.cancel()
	return true
}

// Run блокируется до отмены контекста или вызова Stop/Shutdown.
func (w *Worker) Run(ctx context.Context, queue string) error {
	return w.RunQueues(ctx, queue)
}

// RunQueues запускает воркера сразу на нескольких очередях (например,
// дефолтной и специализированной). Все очереди разделяют лимиты воркера:
// общий пул (WithConcurrency) и per-task лимиты (WithTaskConcurrency).
// Повторяющиеся имена очередей игнорируются; без аргументов — очередь
// по умолчанию. Блокируется до отмены контекста или вызова Stop/Shutdown.
func (w *Worker) RunQueues(ctx context.Context, queues ...string) error {
	seen := make(map[string]struct{}, len(queues))
	unique := make([]string, 0, len(queues))
	for _, q := range queues {
		if _, ok := seen[q]; ok {
			continue
		}
		seen[q] = struct{}{}
		unique = append(unique, q)
	}
	if len(unique) == 0 {
		unique = []string{""}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Маркер «живого Run»: счётчик wg остаётся > 0 всё время работы воркера,
	// поэтому динамические wg.Add(1) в Handle не гоняются с wg.Wait в Shutdown
	// (контракт sync.WaitGroup: Add при нулевом счётчике должен быть до Wait).
	w.lifecycleMu.Lock()
	w.wg.Add(1)
	w.lifecycleMu.Unlock()
	defer w.wg.Done()

	go func() {
		select {
		case <-w.stopCh:
			cancel()
		case <-runCtx.Done():
		}
	}()

	w.logger.Info("worker started", "queues", strings.Join(unique, ","), "concurrency", w.concurrency)

	var wg sync.WaitGroup
	errs := make(chan error, len(unique))
	for _, q := range unique {
		// Каждая Consume-горутина тоже держит счётчик: Add в Handle происходит
		// только пока жива какая-то из них (счётчик > 0 — контракт WaitGroup).
		w.wg.Add(1)
		wg.Add(1)
		go func(queue string) {
			defer w.wg.Done()
			defer wg.Done()
			if err := w.broker.Consume(runCtx, queue, w); err != nil && !errors.Is(err, context.Canceled) {
				errs <- err
			}
		}(q)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return runCtx.Err()
}

// Handle реализует DeliveryHandler.
func (w *Worker) Handle(ctx context.Context, delivery *Delivery) AckType {
	w.wg.Add(1)
	defer w.wg.Done()

	return w.process(ctx, delivery)
}

func (w *Worker) process(ctx context.Context, delivery *Delivery) AckType {
	ctx, span := w.tracer.Start(ctx, "taskq.Worker.process")
	defer span.End()

	var job Job
	if err := json.Unmarshal(delivery.Body, &job); err != nil {
		w.logger.Error("failed to unmarshal job", "error", err.Error())
		return AckNackDrop
	}

	span.SetAttributes(slog.String("job_id", job.ID), slog.String("task", job.Name))

	release, err := w.acquireLimits(ctx, job.Name)
	if err != nil {
		w.logger.Info("job skipped: worker is stopping", "job_id", job.ID, "task", job.Name)
		return AckNackRequeue
	}
	defer release()

	w.saveDoc(ctx, &job)

	if w.preExec != nil {
		if err := w.preExec(ctx, &job); err != nil {
			w.logger.Error("pre execute hook failed", "job_id", job.ID, "error", err.Error())
			w.fail(ctx, &job, delivery, err)
			return AckAck
		}
	}

	handler, ok := w.registry.get(job.Name)
	if !ok {
		w.logger.Warn("handler not found", "job_id", job.ID, "task", job.Name)
		w.fail(ctx, &job, delivery, fmt.Errorf("handler not found for task %q", job.Name))
		return AckAck
	}

	// Задачу отменили до старта — не исполняем и состояние не двигаем.
	if cur, err := w.backend.GetState(ctx, job.ID); err == nil && cur.State == StateCanceled {
		w.logger.Info("job canceled before start", "job_id", job.ID, "task", job.Name)
		return AckAck
	}

	if err := w.backend.SetState(ctx, job.ID, StateReceived); err != nil {
		w.logger.Error("failed to set received state", "job_id", job.ID, "error", err.Error())
		return AckNackRequeue
	}

	if err := w.backend.SetState(ctx, job.ID, StateStarted); err != nil {
		w.logger.Error("failed to set started state", "job_id", job.ID, "error", err.Error())
		return AckNackRequeue
	}

	w.meter.Counter("taskq.started").Inc(ctx, MetricAttr{Key: "task", Value: job.Name})

	execCtx, execCancel := w.applyDeadline(ctx, &job)
	defer execCancel()
	w.trackInFlight(job.ID, execCancel)
	defer w.untrackInFlight(job.ID)

	startedAt := time.Now()
	result, err := handler.Run(execCtx, job.Payload)
	duration := time.Since(startedAt)

	w.meter.Histogram("taskq.duration").Record(ctx, duration, MetricAttr{Key: "task", Value: job.Name})

	if err != nil {
		w.meter.Counter("taskq.failed").Inc(ctx, MetricAttr{Key: "task", Value: job.Name})
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("task timeout: %w", err)
		}
		if w.isCanceled(ctx, job.ID) {
			w.setCanceled(ctx, &job, delivery, err)
			return AckAck
		}
		w.fail(ctx, &job, delivery, err)
		return AckAck
	}

	if err := w.backend.SetResult(ctx, job.ID, result); err != nil {
		w.logger.Error("failed to set result", "job_id", job.ID, "error", err.Error())
		return AckNackRequeue
	}

	if err := w.backend.SetState(ctx, job.ID, StateSuccess); err != nil {
		w.logger.Error("failed to set success state", "job_id", job.ID, "error", err.Error())
		return AckNackRequeue
	}

	w.meter.Counter("taskq.succeeded").Inc(ctx, MetricAttr{Key: "task", Value: job.Name})
	w.logger.Info("task succeeded", "job_id", job.ID, "task", job.Name, "duration", duration.String())

	if w.postExec != nil {
		w.postExec(ctx, &job, StateSuccess, nil)
	}

	if err := w.continueAfterSuccess(ctx, &job, result); err != nil {
		w.logger.Error("failed to continue orchestration", "job_id", job.ID, "error", err.Error())
		return AckNackRequeue
	}

	return AckAck
}

func (w *Worker) fail(ctx context.Context, job *Job, delivery *Delivery, err error) {
	ctx, span := w.tracer.Start(ctx, "taskq.Worker.fail")
	defer span.End()
	span.SetError(err)

	if w.errorHandler != nil {
		if handlerErr := w.errorHandler(ctx, job, err); handlerErr != nil {
			w.logger.Error("error handler failed", "job_id", job.ID, "error", handlerErr.Error())
		}
	}

	if w.postExec != nil {
		w.postExec(ctx, job, StateFailure, err)
	}

	if job.Retry.MaxAttempts > 0 {
		attempt := job.Attempt + 1
		if attempt < job.Retry.MaxAttempts {
			delay := internal.NextBackoff(attempt, job.Retry.InitialDelay, job.Retry.MaxDelay, job.Retry.Multiplier)
			if retryErr := w.retry(ctx, job, attempt, delay); retryErr != nil {
				w.logger.Error("retry failed", "job_id", job.ID, "error", retryErr.Error())
				w.setDead(ctx, job, delivery, err)
				return
			}
			_ = delivery.Ack(ctx)
			return
		}

		// Ретраи исчерпаны — dead-letter.
		w.setDead(ctx, job, delivery, err)
		return
	}

	// Ретрай-политика не задана — окончательный failure.
	w.setFailure(ctx, job, delivery, err)
}

// acquireLimits резервирует слоты исполнения для задачи: сначала per-task
// лимит (если задан WithTaskConcurrency), затем общий пул. Порядок
// получения фиксирован — он исключает циклическое ожидание между лимитами.
// Если контекст отменен во время ожидания, возвращает ошибку; в успехе —
// функцию освобождения слотов (вызывается один раз).
func (w *Worker) acquireLimits(ctx context.Context, taskName string) (func(), error) {
	taskSem := w.taskSemaphores[taskName]

	if taskSem != nil {
		select {
		case taskSem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	select {
	case w.semaphore <- struct{}{}:
	case <-ctx.Done():
		if taskSem != nil {
			<-taskSem
		}
		return nil, ctx.Err()
	}

	return func() {
		<-w.semaphore
		if taskSem != nil {
			<-taskSem
		}
	}, nil
}

// applyDeadline применяет ограничения выполнения задачи (Timeout или Deadline)
// к контексту. Всегда возвращает рабочий cancel: без ограничений выполнения
// он используется для отмены задачи через Worker.Cancel.
func (w *Worker) applyDeadline(ctx context.Context, job *Job) (context.Context, context.CancelFunc) {
	if job.Deadline != nil {
		return context.WithDeadline(ctx, job.Deadline.UTC())
	}
	if job.Timeout > 0 {
		return context.WithTimeout(ctx, job.Timeout)
	}
	return context.WithCancel(ctx)
}

// trackInFlight регистрирует задачу в реестре выполняемых.
func (w *Worker) trackInFlight(jobID string, cancel context.CancelFunc) {
	w.inFlightMu.Lock()
	w.inFlight[jobID] = &inFlightJob{cancel: cancel}
	w.inFlightMu.Unlock()
}

// untrackInFlight снимает задачу с регистрации.
func (w *Worker) untrackInFlight(jobID string) {
	w.inFlightMu.Lock()
	delete(w.inFlight, jobID)
	w.inFlightMu.Unlock()
}

// isCanceled проверяет, запрошена ли отмена задачи: локально через Worker.Cancel
// либо по состоянию задачи в backend.
func (w *Worker) isCanceled(ctx context.Context, jobID string) bool {
	w.inFlightMu.Lock()
	if entry, ok := w.inFlight[jobID]; ok && entry.cancelRequested.Load() {
		w.inFlightMu.Unlock()
		return true
	}
	w.inFlightMu.Unlock()

	if cur, err := w.backend.GetState(ctx, jobID); err == nil && cur.State == StateCanceled {
		return true
	}
	return false
}

// setCanceled помечает задачу отмененной без ретраев.
func (w *Worker) setCanceled(ctx context.Context, job *Job, delivery *Delivery, err error) {
	if setErr := w.backend.SetState(ctx, job.ID, StateCanceled); setErr != nil {
		w.logger.Error("failed to set canceled state", "job_id", job.ID, "error", setErr.Error())
	}

	w.meter.Counter("taskq.canceled").Inc(ctx, MetricAttr{Key: "task", Value: job.Name})
	w.logger.Info("job canceled", "job_id", job.ID, "task", job.Name, "reason", err.Error())

	if w.postExec != nil {
		w.postExec(ctx, job, StateCanceled, err)
	}

	w.failChord(ctx, job, err)
	w.failChainRest(ctx, job, err)

	_ = delivery.Ack(ctx)
}

// setDead переводит задачу в dead-letter: ретраи исчерпаны.
func (w *Worker) setDead(ctx context.Context, job *Job, delivery *Delivery, err error) {
	if setErr := w.backend.SetError(ctx, job.ID, err.Error()); setErr != nil {
		w.logger.Error("failed to set error", "job_id", job.ID, "error", setErr.Error())
	}
	if setErr := w.backend.SetState(ctx, job.ID, StateDead); setErr != nil {
		w.logger.Error("failed to set dead state", "job_id", job.ID, "error", setErr.Error())
	}

	w.meter.Counter("taskq.dead").Inc(ctx, MetricAttr{Key: "task", Value: job.Name})
	w.logger.Error("job moved to dead letter", "job_id", job.ID, "task", job.Name, "error", err.Error())

	if w.deadHook != nil {
		w.deadHook(ctx, job, err)
	}

	w.failChord(ctx, job, err)
	w.failChainRest(ctx, job, err)

	_ = delivery.Ack(ctx)
}

// saveDoc обновляет документ задачи в backend, если он его поддерживает.
// Некритичная операция: ошибка логируется, но не прерывает обработку.
func (w *Worker) saveDoc(ctx context.Context, job *Job) {
	store, ok := w.backend.(JobStore)
	if !ok {
		return
	}

	job.UpdatedAt = time.Now().UTC()
	if err := store.SaveJob(ctx, *job); err != nil {
		w.logger.Warn("failed to save job document", "job_id", job.ID, "error", err.Error())
	}
}

func (w *Worker) retry(ctx context.Context, job *Job, attempt uint32, delay time.Duration) error {
	newJob := &Job{
		ID:        job.ID,
		Name:      job.Name,
		Queue:     job.Queue,
		Payload:   job.Payload,
		State:     StateRetry,
		Attempt:   attempt,
		CreatedAt: job.CreatedAt,
		UpdatedAt: time.Now().UTC(),
		ETA:       timePtr(time.Now().UTC().Add(delay)),
		Retry:     job.Retry,
		Headers:   job.Headers,
		Timeout:   job.Timeout,
		Deadline:  job.Deadline,
	}

	if err := w.backend.SetState(ctx, newJob.ID, StateRetry); err != nil {
		return fmt.Errorf("set retry state: %w", err)
	}

	return w.broker.Publish(ctx, newJob)
}

func (w *Worker) setFailure(ctx context.Context, job *Job, delivery *Delivery, err error) {
	if setErr := w.backend.SetError(ctx, job.ID, err.Error()); setErr != nil {
		w.logger.Error("failed to set error", "job_id", job.ID, "error", setErr.Error())
	}
	if setErr := w.backend.SetState(ctx, job.ID, StateFailure); setErr != nil {
		w.logger.Error("failed to set failure state", "job_id", job.ID, "error", setErr.Error())
	}

	w.failChord(ctx, job, err)
	w.failChainRest(ctx, job, err)

	_ = delivery.Ack(ctx)
}

// Stop останавливает worker, дожидаясь завершения активных задач.
func (w *Worker) Stop() error {
	return w.Shutdown(context.Background())
}

// Shutdown выполняет graceful shutdown с заданным таймаутом.
func (w *Worker) Shutdown(ctx context.Context) error {
	// lifecycleMu гарантирует: маркер Run в wg добавлен ДО close(stopCh),
	// поэтому Wait ниже либо видит активный воркер, либо Run ещё не стартовал
	// (и shutdown завершится мгновенно — стартовавший позже Run увидит
	// закрытый stopCh и выйдет сразу).
	w.lifecycleMu.Lock()
	w.stopOnce.Do(func() {
		close(w.stopCh)
	})
	w.lifecycleMu.Unlock()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close выполняет graceful shutdown и закрывает брокер.
func (w *Worker) Close(ctx context.Context) error {
	if err := w.Shutdown(ctx); err != nil {
		return err
	}

	return w.broker.Close(ctx)
}

func timePtr(t time.Time) *time.Time {
	return &t
}
