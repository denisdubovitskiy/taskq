package memory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/denisdubovitskiy/taskq"
)

// Backend — in-memory хранилище для тестов.
type Backend struct {
	mu        sync.RWMutex
	states    map[string]*taskq.JobState
	results   map[string]*taskq.JobResult
	docs      map[string]taskq.Job
	schedules map[string]taskq.ScheduleDocument
	chords    map[string]*chordState
}

// NewBackend создает in-memory backend.
func NewBackend() *Backend {
	return &Backend{
		states:    make(map[string]*taskq.JobState),
		results:   make(map[string]*taskq.JobResult),
		docs:      make(map[string]taskq.Job),
		schedules: make(map[string]taskq.ScheduleDocument),
		chords:    make(map[string]*chordState),
	}
}

// chordState — состояние аккорда: сколько задач осталось и какие результаты собраны.
type chordState struct {
	total      int
	remaining  int
	failed     bool
	complete   bool
	failIndex  int
	failReason string
	results    map[int][]byte
}

// SetState сохраняет состояние задачи.
func (b *Backend) SetState(ctx context.Context, jobID string, state taskq.State) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC()
	st, ok := b.states[jobID]
	if !ok {
		st = &taskq.JobState{CreatedAt: now}
		b.states[jobID] = st
	}
	st.State = state
	st.UpdatedAt = now
	return nil
}

// SetResult сохраняет результат задачи.
func (b *Backend) SetResult(ctx context.Context, jobID string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.results[jobID] = &taskq.JobResult{Data: data}
	return nil
}

// SetError сохраняет ошибку задачи.
func (b *Backend) SetError(ctx context.Context, jobID string, err string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.states[jobID]; !ok {
		b.states[jobID] = &taskq.JobState{CreatedAt: time.Now().UTC()}
	}
	b.states[jobID].Error = err
	return nil
}

// GetState возвращает состояние задачи.
func (b *Backend) GetState(ctx context.Context, jobID string) (*taskq.JobState, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	state, ok := b.states[jobID]
	if !ok {
		return nil, fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
	}

	return &taskq.JobState{
		State:     state.State,
		Error:     state.Error,
		CreatedAt: state.CreatedAt,
		UpdatedAt: state.UpdatedAt,
	}, nil
}

// GetResult возвращает результат задачи.
func (b *Backend) GetResult(ctx context.Context, jobID string) (*taskq.JobResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result, ok := b.results[jobID]
	if !ok {
		return nil, fmt.Errorf("result of job %s: %w", jobID, taskq.ErrJobNotFound)
	}

	return &taskq.JobResult{Data: result.Data}, nil
}

// Purge удаляет задачу.
func (b *Backend) Purge(ctx context.Context, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.states, jobID)
	delete(b.results, jobID)
	delete(b.docs, jobID)
	return nil
}

// Close закрывает backend.
func (b *Backend) Close(ctx context.Context) error {
	return nil
}

// ChordInit инициализирует состояние аккорда с total задачами в группе.
func (b *Backend) ChordInit(ctx context.Context, chordID string, total int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.chords[chordID] = &chordState{
		total:     total,
		remaining: total,
		results:   make(map[int][]byte, total),
	}
	return nil
}

// ChordFinish фиксирует успешное завершение задачи с индексом index.
// Когда завершены все задачи группы, возвращает true и результаты в порядке индексов.
func (b *Backend) ChordFinish(ctx context.Context, chordID string, index int, result []byte) (bool, []taskq.ChordResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.chords[chordID]
	if !ok {
		return false, nil, fmt.Errorf("chord %s not found", chordID)
	}
	if st.failed || st.complete {
		return false, nil, nil
	}
	if index < 0 || index >= st.total {
		return false, nil, fmt.Errorf("chord %s: index %d out of range", chordID, index)
	}

	st.results[index] = append([]byte(nil), result...)
	st.remaining--
	if st.remaining > 0 {
		return false, nil, nil
	}

	st.complete = true
	results := make([]taskq.ChordResult, 0, st.total)
	for i := 0; i < st.total; i++ {
		results = append(results, taskq.ChordResult{Index: i, Result: st.results[i]})
	}
	return true, results, nil
}

// ChordFail фиксирует окончательный сбой задачи с индексом index.
// Если аккорд еще не завершён, помечает его упавшим и возвращает true.
func (b *Backend) ChordFail(ctx context.Context, chordID string, index int, errMsg string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.chords[chordID]
	if !ok {
		return false, fmt.Errorf("chord %s not found", chordID)
	}
	if st.failed || st.complete {
		return false, nil
	}

	st.failed = true
	st.failIndex = index
	st.failReason = errMsg
	return true, nil
}

// SaveJob сохраняет документ задачи, не трогая состояние и ошибку.
func (b *Backend) SaveJob(ctx context.Context, job taskq.Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.states[job.ID]; !ok {
		b.states[job.ID] = &taskq.JobState{State: job.State, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt}
	}
	b.docs[job.ID] = job
	return nil
}

// Inspect возвращает полный документ задачи с актуальным состоянием.
func (b *Backend) Inspect(ctx context.Context, jobID string) (*taskq.Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st, ok := b.states[jobID]
	if !ok {
		return nil, fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
	}

	doc, hasDoc := b.docs[jobID]
	if !hasDoc {
		return &taskq.Job{ID: jobID, State: st.State, Error: st.Error, CreatedAt: st.CreatedAt, UpdatedAt: st.UpdatedAt}, nil
	}

	doc.State = st.State
	doc.Error = st.Error
	doc.UpdatedAt = st.UpdatedAt
	return &doc, nil
}

// List возвращает страницу задач по фильтру.
// Порядок детерминированный: по CreatedAt, затем по ID.
func (b *Backend) List(ctx context.Context, q taskq.ListQuery) (taskq.ListResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	items := make([]taskq.JobSummary, 0, len(b.states))
	for id, st := range b.states {
		if q.State != "" && st.State != q.State {
			continue
		}
		doc, hasDoc := b.docs[id]
		if q.Task != "" && (!hasDoc || doc.Name != q.Task) {
			continue
		}
		items = append(items, taskq.JobSummary{
			ID:        id,
			Task:      doc.Name,
			State:     st.State,
			Attempt:   doc.Attempt,
			CreatedAt: st.CreatedAt,
			UpdatedAt: st.UpdatedAt,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if !items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].CreatedAt.Before(items[j].CreatedAt)
		}
		return items[i].ID < items[j].ID
	})

	start := 0
	if q.Cursor != "" {
		if parsed, err := strconv.Atoi(q.Cursor); err == nil {
			start = parsed
		}
	}
	if start > len(items) {
		start = len(items)
	}

	end := start + limit
	done := end >= len(items)
	if end > len(items) {
		end = len(items)
	}

	res := taskq.ListResult{Items: items[start:end], Done: done}
	if !done {
		res.Cursor = strconv.Itoa(end)
	}
	return res, nil
}

// Reset возвращает задачу в pending: очищает ошибку и сбрасывает попытку.
// Разрешено только из states failure и dead.
func (b *Backend) Reset(ctx context.Context, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.states[jobID]
	if !ok {
		return fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
	}
	if st.State != taskq.StateFailure && st.State != taskq.StateDead {
		return fmt.Errorf("job %s in state %s: %w", jobID, st.State, taskq.ErrStateConflict)
	}

	now := time.Now().UTC()
	st.State = taskq.StatePending
	st.Error = ""
	st.UpdatedAt = now

	if doc, hasDoc := b.docs[jobID]; hasDoc {
		doc.State = taskq.StatePending
		doc.Attempt = 0
		doc.ETA = nil
		doc.UpdatedAt = now
		b.docs[jobID] = doc
	}
	return nil
}

// Delete полностью удаляет задачу (состояние, результат, документ).
func (b *Backend) Delete(ctx context.Context, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.states[jobID]; !ok {
		return fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
	}

	delete(b.states, jobID)
	delete(b.results, jobID)
	delete(b.docs, jobID)
	return nil
}

// SaveSchedule сохраняет документ расписания.
func (b *Backend) SaveSchedule(ctx context.Context, doc taskq.ScheduleDocument) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	payload := append([]byte(nil), doc.Payload...)
	doc.Payload = payload
	b.schedules[doc.Name] = doc
	return nil
}

// GetSchedule возвращает документ расписания.
func (b *Backend) GetSchedule(ctx context.Context, name string) (*taskq.ScheduleDocument, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	doc, ok := b.schedules[name]
	if !ok {
		return nil, fmt.Errorf("schedule %s: %w", name, taskq.ErrScheduleNotFound)
	}
	doc.Payload = append([]byte(nil), doc.Payload...)
	return &doc, nil
}

// ListSchedules возвращает все расписания в порядке имени.
func (b *Backend) ListSchedules(ctx context.Context) ([]taskq.ScheduleDocument, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.schedules))
	for name := range b.schedules {
		names = append(names, name)
	}
	sort.Strings(names)

	list := make([]taskq.ScheduleDocument, 0, len(names))
	for _, name := range names {
		doc := b.schedules[name]
		doc.Payload = append([]byte(nil), doc.Payload...)
		list = append(list, doc)
	}
	return list, nil
}

// DeleteSchedule удаляет документ расписания.
func (b *Backend) DeleteSchedule(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.schedules[name]; !ok {
		return fmt.Errorf("schedule %s: %w", name, taskq.ErrScheduleNotFound)
	}
	delete(b.schedules, name)
	return nil
}
