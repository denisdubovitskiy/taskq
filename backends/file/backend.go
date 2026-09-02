package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/denisdubovitskiy/taskq"
)

// Backend хранит каждое состояние/результат в отдельном JSON-файле.
type Backend struct {
	dir string
	mu  sync.RWMutex
}

// New создает файловый backend.
func New(dir string) (*Backend, error) {
	if dir == "" {
		return nil, errors.New("dir is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create backend dir: %w", err)
	}
	return &Backend{dir: dir}, nil
}

// SetState сохраняет состояние задачи.
func (b *Backend) SetState(ctx context.Context, jobID string, state taskq.State) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	record, err := b.load(jobID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if record == nil {
		record = &storedRecord{}
	}

	record.State = state
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	record.UpdatedAt = time.Now().UTC()
	return b.save(jobID, record)
}

// SetResult сохраняет результат задачи.
func (b *Backend) SetResult(ctx context.Context, jobID string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	record, err := b.load(jobID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if record == nil {
		record = &storedRecord{}
	}

	record.Result = data
	record.UpdatedAt = time.Now().UTC()
	return b.save(jobID, record)
}

// SetError сохраняет ошибку задачи.
func (b *Backend) SetError(ctx context.Context, jobID string, err string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	record, errLoad := b.load(jobID)
	if errLoad != nil && !errors.Is(errLoad, os.ErrNotExist) {
		return errLoad
	}
	if record == nil {
		record = &storedRecord{}
	}

	record.Error = err
	record.UpdatedAt = time.Now().UTC()
	return b.save(jobID, record)
}

// GetState возвращает состояние задачи.
func (b *Backend) GetState(ctx context.Context, jobID string) (*taskq.JobState, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	record, err := b.load(jobID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
		}
		return nil, err
	}
	return &taskq.JobState{
		State:     record.State,
		Error:     record.Error,
		CreatedAt: record.CreatedAt,
		UpdatedAt: record.UpdatedAt,
	}, nil
}

// GetResult возвращает результат задачи.
func (b *Backend) GetResult(ctx context.Context, jobID string) (*taskq.JobResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	record, err := b.load(jobID)
	if err != nil {
		return nil, err
	}
	return &taskq.JobResult{Data: record.Result}, nil
}

// Purge удаляет задачу.
func (b *Backend) Purge(ctx context.Context, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return os.Remove(b.path(jobID))
}

// Close закрывает backend.
func (b *Backend) Close(ctx context.Context) error { return nil }

// ChordInit инициализирует состояние аккорда с total задачами в группе.
func (b *Backend) ChordInit(ctx context.Context, chordID string, total int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	record := &chordRecord{
		Total:     total,
		Remaining: total,
		Results:   make(map[string][]byte, total),
	}
	return b.saveChord(chordID, record)
}

// ChordFinish фиксирует успешное завершение задачи с индексом index.
// Когда завершены все задачи группы, возвращает true и результаты в порядке индексов.
// Если аккорд уже завершён (какая-то задача упала), возвращает (false, nil, nil).
func (b *Backend) ChordFinish(ctx context.Context, chordID string, index int, result []byte) (bool, []taskq.ChordResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	record, err := b.loadChord(chordID)
	if err != nil {
		return false, nil, err
	}
	if record.Failed || record.Complete {
		return false, nil, nil
	}
	if index < 0 || index >= record.Total {
		return false, nil, fmt.Errorf("chord %s: index %d out of range", chordID, index)
	}

	record.Results[strconv.Itoa(index)] = append([]byte(nil), result...)
	record.Remaining--
	if record.Remaining > 0 {
		return false, nil, b.saveChord(chordID, record)
	}

	record.Complete = true
	if err := b.saveChord(chordID, record); err != nil {
		return false, nil, err
	}

	results := make([]taskq.ChordResult, 0, record.Total)
	for i := 0; i < record.Total; i++ {
		results = append(results, taskq.ChordResult{Index: i, Result: record.Results[strconv.Itoa(i)]})
	}
	return true, results, nil
}

// ChordFail фиксирует окончательный сбой задачи с индексом index.
// Если аккорд еще не завершён, помечает его упавшим и возвращает true.
func (b *Backend) ChordFail(ctx context.Context, chordID string, index int, errMsg string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	record, err := b.loadChord(chordID)
	if err != nil {
		return false, err
	}
	if record.Failed || record.Complete {
		return false, nil
	}

	record.Failed = true
	record.FailIndex = index
	record.FailReason = errMsg
	return true, b.saveChord(chordID, record)
}

// chordRecord — состояние аккорда, хранимое в отдельном файле.
type chordRecord struct {
	Total      int               `json:"total"`
	Remaining  int               `json:"remaining"`
	Failed     bool              `json:"failed,omitempty"`
	Complete   bool              `json:"complete,omitempty"`
	FailIndex  int               `json:"fail_index,omitempty"`
	FailReason string            `json:"fail_reason,omitempty"`
	Results    map[string][]byte `json:"results,omitempty"`
}

func (b *Backend) chordPath(chordID string) string {
	return filepath.Join(b.dir, "chord-"+chordID+".json")
}

// loadChord читает запись аккорда из файла.
func (b *Backend) loadChord(chordID string) (*chordRecord, error) {
	data, err := os.ReadFile(b.chordPath(chordID))
	if err != nil {
		return nil, err
	}
	var r chordRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("decode chord record: %w", err)
	}
	if r.Results == nil {
		r.Results = make(map[string][]byte, r.Total)
	}
	return &r, nil
}

func (b *Backend) saveChord(chordID string, r *chordRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode chord record: %w", err)
	}
	if err := os.WriteFile(b.chordPath(chordID), data, 0o640); err != nil {
		return fmt.Errorf("write chord record: %w", err)
	}
	return nil
}

func (b *Backend) path(jobID string) string {
	return filepath.Join(b.dir, jobID+".json")
}

type storedRecord struct {
	State     taskq.State `json:"state"`
	Error     string      `json:"error,omitempty"`
	Result    []byte      `json:"result,omitempty"`
	Doc       *taskq.Job  `json:"doc,omitempty"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

func (b *Backend) load(jobID string) (*storedRecord, error) {
	data, err := os.ReadFile(b.path(jobID))
	if err != nil {
		return nil, err
	}
	var r storedRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	return &r, nil
}

func (b *Backend) save(jobID string, r *storedRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}
	if err := os.WriteFile(b.path(jobID), data, 0o640); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	return nil
}

// SaveJob сохраняет документ задачи, не трогая состояние и ошибку.
func (b *Backend) SaveJob(ctx context.Context, job taskq.Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	record, err := b.load(job.ID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if record == nil {
		record = &storedRecord{CreatedAt: job.CreatedAt}
	}
	doc := job
	record.Doc = &doc
	return b.save(job.ID, record)
}

// Inspect возвращает полный документ задачи с актуальным состоянием.
func (b *Backend) Inspect(ctx context.Context, jobID string) (*taskq.Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	record, err := b.load(jobID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
		}
		return nil, err
	}

	if record.Doc == nil {
		return &taskq.Job{
			ID:        jobID,
			State:     record.State,
			Error:     record.Error,
			CreatedAt: record.CreatedAt,
			UpdatedAt: record.UpdatedAt,
		}, nil
	}

	doc := *record.Doc
	doc.State = record.State
	doc.Error = record.Error
	doc.UpdatedAt = record.UpdatedAt
	return &doc, nil
}

// List возвращает страницу задач по фильтру.
// Порядок детерминированный: по CreatedAt, затем по ID.
func (b *Backend) List(ctx context.Context, q taskq.ListQuery) (taskq.ListResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return taskq.ListResult{}, fmt.Errorf("read backend dir: %w", err)
	}

	items := make([]taskq.JobSummary, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || strings.HasPrefix(entry.Name(), "chord-") || strings.HasPrefix(entry.Name(), "schedule-") {
			continue
		}
		jobID := strings.TrimSuffix(entry.Name(), ".json")

		record, err := b.load(jobID)
		if err != nil {
			continue
		}
		if q.State != "" && record.State != q.State {
			continue
		}
		if q.Task != "" && (record.Doc == nil || record.Doc.Name != q.Task) {
			continue
		}

		summary := taskq.JobSummary{
			ID:        jobID,
			State:     record.State,
			CreatedAt: record.CreatedAt,
			UpdatedAt: record.UpdatedAt,
		}
		if record.Doc != nil {
			summary.Task = record.Doc.Name
			summary.Attempt = record.Doc.Attempt
		}
		items = append(items, summary)
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

	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
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

	record, err := b.load(jobID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
		}
		return err
	}
	if record.State != taskq.StateFailure && record.State != taskq.StateDead {
		return fmt.Errorf("job %s in state %s: %w", jobID, record.State, taskq.ErrStateConflict)
	}

	now := time.Now().UTC()
	record.State = taskq.StatePending
	record.Error = ""
	record.UpdatedAt = now
	if record.Doc != nil {
		record.Doc.State = taskq.StatePending
		record.Doc.Attempt = 0
		record.Doc.ETA = nil
		record.Doc.UpdatedAt = now
	}
	return b.save(jobID, record)
}

// Delete полностью удаляет задачу (файл с состоянием, результатом и документом).
func (b *Backend) Delete(ctx context.Context, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := os.Remove(b.path(jobID)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
		}
		return err
	}
	return nil
}

// schedulePath возвращает путь файла расписания.
func (b *Backend) schedulePath(name string) string {
	return filepath.Join(b.dir, "schedule-"+name+".json")
}

// SaveSchedule сохраняет документ расписания в отдельный файл.
func (b *Backend) SaveSchedule(ctx context.Context, doc taskq.ScheduleDocument) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	stored := doc
	stored.Payload = append([]byte(nil), doc.Payload...)
	data, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("encode schedule document: %w", err)
	}
	if err := os.WriteFile(b.schedulePath(doc.Name), data, 0o640); err != nil {
		return fmt.Errorf("write schedule document: %w", err)
	}
	return nil
}

// GetSchedule возвращает документ расписания.
func (b *Backend) GetSchedule(ctx context.Context, name string) (*taskq.ScheduleDocument, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data, err := os.ReadFile(b.schedulePath(name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("schedule %s: %w", name, taskq.ErrScheduleNotFound)
		}
		return nil, err
	}
	var doc taskq.ScheduleDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("decode schedule document: %w", err)
	}
	return &doc, nil
}

// ListSchedules возвращает все расписания в порядке имени.
func (b *Backend) ListSchedules(ctx context.Context) ([]taskq.ScheduleDocument, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, fmt.Errorf("read backend dir: %w", err)
	}

	var list []taskq.ScheduleDocument
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "schedule-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "schedule-"), ".json")
		doc, err := b.getScheduleLocked(name)
		if err != nil {
			continue
		}
		list = append(list, *doc)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list, nil
}

// getScheduleLocked читает файл расписания без блокировки (вызывается под b.mu).
func (b *Backend) getScheduleLocked(name string) (*taskq.ScheduleDocument, error) {
	data, err := os.ReadFile(b.schedulePath(name))
	if err != nil {
		return nil, err
	}
	var doc taskq.ScheduleDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// DeleteSchedule удаляет документ расписания.
func (b *Backend) DeleteSchedule(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := os.Remove(b.schedulePath(name)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("schedule %s: %w", name, taskq.ErrScheduleNotFound)
		}
		return err
	}
	return nil
}
