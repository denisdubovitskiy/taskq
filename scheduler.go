package taskq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/denisdubovitskiy/taskq/internal/cron"
)

// Schedule[T, R] — хендл зарегистрированной периодической задачи.
type Schedule[T, R any] struct {
	name string
	s    *Scheduler
}

// Name возвращает имя задачи.
func (sc *Schedule[T, R]) Name() string {
	return sc.name
}

// NextRun возвращает время следующего срабатывания задачи.
// Возвращает нулевое время, если задача удалена из планировщика.
func (sc *Schedule[T, R]) NextRun() time.Time {
	return sc.s.nextRun(sc.name)
}

// CatchUpPolicy — поведение планировщика для упущенного тика.
// Тик считается упущенным, когда его время уже в прошлом в момент,
// когда расписание становится активным (регистрация или рестарт):
// например, процесс стоял дольше интервала.
type CatchUpPolicy int

// Возможные политики для упущенных тиков.
const (
	// CatchUpSkip — по умолчанию: упущенный тик не выполняется,
	// следующий сдвигается в будущее.
	CatchUpSkip CatchUpPolicy = iota
	// CatchUpFireOnce — единовременный догонный запуск упущенного тика,
	// затем следующий сдвигается в будущее.
	CatchUpFireOnce
)

// ScheduleOption — опция уровня расписания (не задачи).
type ScheduleOption func(*scheduleConfig)

// scheduleConfig — внутренняя конфигурация расписания.
type scheduleConfig struct {
	timezone   *time.Location
	catchUp    CatchUpPolicy
	submitOpts []SubmitOption
}

// newScheduleConfig собирает конфигурацию из опций.
func newScheduleConfig(opts ...ScheduleOption) *scheduleConfig {
	cfg := &scheduleConfig{catchUp: CatchUpSkip}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithCronTimezone задает таймзону, в которой считается cron-выражение.
// По умолчанию — UTC. К AddEvery неприменимо: интервал не зависит от таймзоны.
func WithCronTimezone(loc *time.Location) ScheduleOption {
	return func(c *scheduleConfig) {
		if loc != nil {
			c.timezone = loc
		}
	}
}

// WithCatchUp задает политику для упущенных тиков.
func WithCatchUp(p CatchUpPolicy) ScheduleOption {
	return func(c *scheduleConfig) {
		if p == CatchUpSkip || p == CatchUpFireOnce {
			c.catchUp = p
		}
	}
}

// WithSubmitOpts передает SubmitOption-опции (ретраи, ETA и т.п.)
// в задачи, отправляемые при каждом срабатывании.
func WithSubmitOpts(opts ...SubmitOption) ScheduleOption {
	return func(c *scheduleConfig) {
		c.submitOpts = append(c.submitOpts, opts...)
	}
}

// defaultLockTTL — время жизни блокировки тика по умолчанию.
const defaultLockTTL = 10 * time.Second

// scheduleKind — тип периодической задачи.
type scheduleKind int

const (
	everySchedule scheduleKind = iota
	cronSchedule
)

// scheduleEntry — стирание типа над зарегистрированной задачей:
// планировщик не знает T/R и использует закрытую функцию отправки.
type scheduleEntry struct {
	name     string
	taskName string
	kind     scheduleKind
	interval time.Duration  // для everySchedule
	expr     *cron.Expr     // для cronSchedule
	exprStr  string         // для cronSchedule: исходное выражение
	timezone string         // для cronSchedule: loc.String(); пустая — UTC
	tz       *time.Location // для cronSchedule: таймзона вычисления тиков
	catchUp  CatchUpPolicy  // поведение для упущенного тика
	payload  []byte         // сериализованный payload (для документа)
	next     time.Time      // следующий тик; может быть в прошлом до catch-up тика
	send     func(ctx context.Context) error
}

// advance сдвигает next на следующий тик, пропуская все упущенные
// срабатывания: планировщик не «догоняет» пропущенные тики.
func (e *scheduleEntry) advance(now time.Time) {
	for {
		var next time.Time
		switch e.kind {
		case everySchedule:
			next = e.next.Add(e.interval)
		default:
			next = e.expr.Next(e.next)
		}
		if next.After(now) {
			e.next = next
			return
		}
		e.next = next
	}
}

// lockKey возвращает ключ блокировки для окна тика.
// Для every окно — интервал, квантованный от абсолютного времени,
// поэтому два экземпляра с одинаковым интервалом попадают в одно окно.
// Для cron окно — секунда срабатывания, которую оба экземпляра
// считают одинаково из wall clock.
func (e *scheduleEntry) lockKey(fireTime time.Time) string {
	var window string
	switch e.kind {
	case everySchedule:
		window = fireTime.Truncate(e.interval).UTC().Format("20060102T150405.000")
	default:
		window = fireTime.UTC().Truncate(time.Second).Format("20060102T150405")
	}
	return "taskq:scheduler:" + e.taskName + ":" + window
}

// Scheduler управляет выполнением периодических задач.
type Scheduler struct {
	client  *Client
	locker  Locker
	logger  Logger
	lockTTL time.Duration

	mu       sync.Mutex
	entries  map[string]*scheduleEntry
	running  bool
	runDone  chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
}

// SchedulerOption — опция для Scheduler.
type SchedulerOption func(*Scheduler)

// WithSchedulerLogger задает логгер для Scheduler.
func WithSchedulerLogger(logger Logger) SchedulerOption {
	return func(s *Scheduler) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithSchedulerLockTTL задает фиксированное время жизни блокировки тика.
func WithSchedulerLockTTL(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		if d > 0 {
			s.lockTTL = d
		}
	}
}

// NewScheduler создает планировщик. Client обязателен,
// Locker опционален: без него нет защиты от двойного запуска
// несколькими экземплярами.
func NewScheduler(c *Client, locker Locker, opts ...SchedulerOption) (*Scheduler, error) {
	if c == nil {
		return nil, errors.New("client is nil")
	}

	s := &Scheduler{
		client:  c,
		locker:  locker,
		logger:  noopLogger{},
		lockTTL: defaultLockTTL,
		entries: make(map[string]*scheduleEntry),
		stopCh:  make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// AddEvery регистрирует периодическую задачу с фиксированным интервалом.
// Первый запуск происходит через interval после регистрации, если для имени
// нет сохраненного в backend документа (см. ScheduleStore).
func AddEvery[T, R any](ctx context.Context, s *Scheduler, name string, interval time.Duration, task *Task[T, R], payload T, opts ...ScheduleOption) (*Schedule[T, R], error) {
	if s == nil {
		return nil, errors.New("scheduler is nil")
	}
	if name == "" {
		return nil, errors.New("name is required")
	}
	if task == nil {
		return nil, errors.New("task is nil")
	}
	if interval <= 0 {
		return nil, errors.New("interval must be positive")
	}

	cfg := newScheduleConfig(opts...)
	payloadData, err := s.client.codec.Encode(payload)
	if err != nil {
		return nil, fmt.Errorf("encode schedule payload: %w", err)
	}

	if err := s.addSchedule(ctx, name, task.Name, everySchedule, interval, nil, "", cfg, payloadData,
		func(ctx context.Context) error {
			_, err := Submit[T, R](ctx, s.client, task, payload, cfg.submitOpts...)
			return err
		},
	); err != nil {
		return nil, err
	}
	return &Schedule[T, R]{name: name, s: s}, nil
}

// AddCron регистрирует периодическую задачу по стандартному
// 5-полевому cron-выражению. Таймзона вычисления — UTC, если не задана
// WithCronTimezone.
func AddCron[T, R any](ctx context.Context, s *Scheduler, name string, expr string, task *Task[T, R], payload T, opts ...ScheduleOption) (*Schedule[T, R], error) {
	if s == nil {
		return nil, errors.New("scheduler is nil")
	}
	if name == "" {
		return nil, errors.New("name is required")
	}
	if task == nil {
		return nil, errors.New("task is nil")
	}

	parsed, err := cron.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}

	cfg := newScheduleConfig(opts...)
	payloadData, err := s.client.codec.Encode(payload)
	if err != nil {
		return nil, fmt.Errorf("encode schedule payload: %w", err)
	}

	if err := s.addSchedule(ctx, name, task.Name, cronSchedule, 0, parsed, expr, cfg, payloadData,
		func(ctx context.Context) error {
			_, err := Submit[T, R](ctx, s.client, task, payload, cfg.submitOpts...)
			return err
		},
	); err != nil {
		return nil, err
	}
	return &Schedule[T, R]{name: name, s: s}, nil
}

// addSchedule регистрирует задачу в планировщике и вычисляет первый тик.
// Если backend реализует ScheduleStore и для имени есть сохраненный документ,
// проверка совпадения определения и заимствуется время следующего тика
// (фаза расписания переживает рестарт и синхронизируется между экземплярами).
// send — функция отправки (внутренняя).
func (s *Scheduler) addSchedule(ctx context.Context, name, taskName string, kind scheduleKind, interval time.Duration, expr *cron.Expr, exprStr string, cfg *scheduleConfig, payload []byte, send func(ctx context.Context) error) error {
	s.mu.Lock()
	if _, ok := s.entries[name]; ok {
		s.mu.Unlock()
		return fmt.Errorf("schedule %q already exists", name)
	}
	s.mu.Unlock()

	now := time.Now().UTC()
	next := s.computeNext(kind, interval, expr, cfg.timezone, now)

	timezone := ""
	if cfg.timezone != nil {
		timezone = cfg.timezone.String()
	}

	if store, ok := s.store(); ok {
		doc, err := store.GetSchedule(ctx, name)
		switch {
		case err == nil:
			if !sameScheduleDefinition(doc, taskName, kind, interval, exprStr, timezone) {
				return fmt.Errorf("schedule %q: %w", name, ErrScheduleConflict)
			}
			next = doc.NextRun
		case errors.Is(err, ErrScheduleNotFound):
			// Документа нет — регистрируем с вычисленным next.
		default:
			return fmt.Errorf("load schedule %q: %w", name, err)
		}
	}

	if !next.After(time.Now().UTC()) && cfg.catchUp == CatchUpSkip {
		// Упущенный тик (документ из прошлого): сдвигаем в будущее без запуска.
		e := &scheduleEntry{kind: kind, interval: interval, expr: expr, next: next}
		e.advance(time.Now().UTC())
		next = e.next
	}

	e := &scheduleEntry{
		name:     name,
		taskName: taskName,
		kind:     kind,
		interval: interval,
		expr:     expr,
		exprStr:  exprStr,
		timezone: timezone,
		tz:       cfg.timezone,
		catchUp:  cfg.catchUp,
		payload:  payload,
		next:     next,
		send:     send,
	}

	s.mu.Lock()
	if _, ok := s.entries[name]; ok {
		s.mu.Unlock()
		return fmt.Errorf("schedule %q already exists", name)
	}
	s.entries[name] = e
	s.mu.Unlock()

	// Сохраняем документ, чтобы другие экземпляры сразу видели общий next.
	if store, ok := s.store(); ok {
		if err := store.SaveSchedule(ctx, s.scheduleDocument(e)); err != nil {
			s.logger.Warn("failed to persist schedule", "schedule", name, "error", err.Error())
		}
	}
	return nil
}

// computeNext вычисляет следующий тик: для every — now+interval,
// для cron — следующее срабатывание выражения в таймзоне расписания.
func (s *Scheduler) computeNext(kind scheduleKind, interval time.Duration, expr *cron.Expr, tz *time.Location, now time.Time) time.Time {
	if kind == everySchedule {
		return now.Add(interval)
	}
	if tz != nil {
		now = now.In(tz)
	}
	return expr.Next(now)
}

// sameScheduleDefinition проверяет совпадение сохраненного документа
// с новым определением. Payload сравнивать не нужно: его можно менять.
func sameScheduleDefinition(doc *ScheduleDocument, taskName string, kind scheduleKind, interval time.Duration, exprStr, timezone string) bool {
	kindStr := ScheduleEvery
	if kind == cronSchedule {
		kindStr = ScheduleCron
	}
	return doc.Task == taskName &&
		doc.Kind == kindStr &&
		doc.Interval == interval &&
		doc.Expr == exprStr &&
		doc.Timezone == timezone
}

// store возвращает backend как ScheduleStore, если он его реализует.
func (s *Scheduler) store() (ScheduleStore, bool) {
	store, ok := s.client.backend.(ScheduleStore)
	return store, ok
}

// scheduleDocument собирает документ расписания из записи.
// Вызывается без s.mu: e.next читается под блокировкой перед вызовом.
func (s *Scheduler) scheduleDocument(e *scheduleEntry) ScheduleDocument {
	s.mu.Lock()
	doc := ScheduleDocument{
		Name:      e.name,
		Task:      e.taskName,
		Kind:      scheduleKindString(e.kind),
		Interval:  e.interval,
		Expr:      e.exprStr,
		Timezone:  e.timezone,
		Payload:   append([]byte(nil), e.payload...),
		NextRun:   e.next,
		UpdatedAt: time.Now().UTC(),
	}
	s.mu.Unlock()
	return doc
}

// scheduleKindString переводит внутренний тип в публичный.
func scheduleKindString(k scheduleKind) ScheduleKind {
	if k == cronSchedule {
		return ScheduleCron
	}
	return ScheduleEvery
}

// Remove удаляет задачу из планировщика и, если backend поддерживает
// ScheduleStore, удаляет сохраненный документ (best-effort: ошибка
// документа не блокирует удаление из памяти).
func (s *Scheduler) Remove(name string) error {
	s.mu.Lock()
	if _, ok := s.entries[name]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("schedule %q not found", name)
	}
	delete(s.entries, name)
	s.mu.Unlock()

	if store, ok := s.store(); ok {
		if err := store.DeleteSchedule(context.Background(), name); err != nil && !errors.Is(err, ErrScheduleNotFound) {
			s.logger.Warn("failed to delete schedule document", "schedule", name, "error", err.Error())
		}
	}
	return nil
}

// nextRun возвращает время следующего тика по имени.
func (s *Scheduler) nextRun(name string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[name]; ok {
		return e.next
	}
	return time.Time{}
}

// nextDue возвращает ближайшее время срабатывания среди всех задач.
func (s *Scheduler) nextDue() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	var next time.Time
	for _, e := range s.entries {
		if next.IsZero() || e.next.Before(next) {
			next = e.next
		}
	}
	return next
}

// Run запускает планировщик и блокируется до вызова Stop/Shutdown
// или отмены контекста. Возвращает nil при штатной остановке.
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("scheduler is already running")
	}
	s.running = true
	s.runDone = make(chan struct{})
	count := len(s.entries)
	s.mu.Unlock()

	defer close(s.runDone)
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	s.logger.Info("scheduler started", "schedules", count)

	for {
		s.fireDue(ctx)

		deadline := s.nextDue()
		if deadline.IsZero() {
			// Задач нет: ждем остановки.
			s.waitForStop(ctx)
			s.logger.Info("scheduler stopped")
			return nil
		}

		timer := time.NewTimer(time.Until(deadline))
		stopped := false
		select {
		case <-s.stopCh:
			stopped = true
		case <-ctx.Done():
			stopped = true
		case <-timer.C:
		}
		timer.Stop()
		if stopped {
			s.logger.Info("scheduler stopped")
			return nil
		}
	}
}

// fireDue срабатывает все задачи, чье время наступило.
func (s *Scheduler) fireDue(ctx context.Context) {
	now := time.Now().UTC()

	s.mu.Lock()
	var due []*scheduleEntry
	for _, e := range s.entries {
		if !e.next.After(now) {
			due = append(due, e)
		}
	}
	s.mu.Unlock()

	for _, e := range due {
		s.fire(ctx, e)
	}
}

// fire срабатывает одну задачу: сдвигает следующий тик, захватывает
// блокировку окна (если задан Locker) и отправляет задачу.
func (s *Scheduler) fire(ctx context.Context, e *scheduleEntry) {
	fireTime := e.next

	s.mu.Lock()
	e.advance(time.Now().UTC())
	s.mu.Unlock()

	s.persistNext(ctx, e)

	if s.locker != nil {
		key := e.lockKey(fireTime)
		lock, err := s.locker.Lock(ctx, key, s.lockTTL)
		if err != nil {
			s.logger.Debug("schedule skipped: lock is held", "schedule", e.name, "key", key)
			// Победитель уже сдвинул общий next — сходимся к его фазе.
			s.adoptPersistedNext(ctx, e)
			return
		}
		// Блокировку не снимаем досрочно: в пределах окна тика
		// другие экземпляры не должны дублировать срабатывание.
		_ = lock
	}

	if err := e.send(ctx); err != nil {
		s.logger.Error("schedule submit failed", "schedule", e.name, "task", e.taskName, "error", err)
		return
	}
	s.logger.Info("schedule fired", "schedule", e.name, "task", e.taskName, "fire_time", fireTime)
}

// persistNext сохраняет время следующего тика в backend, если он поддерживает
// ScheduleStore. Некритичная операция: ошибка логируется.
func (s *Scheduler) persistNext(ctx context.Context, e *scheduleEntry) {
	store, ok := s.store()
	if !ok {
		return
	}
	if err := store.SaveSchedule(ctx, s.scheduleDocument(e)); err != nil {
		s.logger.Warn("failed to persist schedule", "schedule", e.name, "error", err.Error())
	}
}

// adoptPersistedNext забирает следующий тик из документа backend
// (победитель lock'а его уже сдвинул) — экземпляр сходится к общей фазе.
func (s *Scheduler) adoptPersistedNext(ctx context.Context, e *scheduleEntry) {
	store, ok := s.store()
	if !ok {
		return
	}
	doc, err := store.GetSchedule(ctx, e.name)
	if err != nil {
		return
	}
	s.mu.Lock()
	if doc.NextRun.After(time.Now().UTC()) {
		e.next = doc.NextRun
	}
	s.mu.Unlock()
}

// waitForStop ждет Stop или отмены контекста.
func (s *Scheduler) waitForStop(ctx context.Context) {
	select {
	case <-s.stopCh:
	case <-ctx.Done():
	}
}

// Stop останавливает планировщик без таймаута.
func (s *Scheduler) Stop() error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	return nil
}

// Shutdown останавливает планировщик и дожидается завершения
// текущего цикла в пределах контекста.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	if err := s.Stop(); err != nil {
		return err
	}

	s.mu.Lock()
	running := s.running
	done := s.runDone
	s.mu.Unlock()

	if !running || done == nil {
		return nil
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
