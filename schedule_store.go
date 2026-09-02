package taskq

import (
	"context"
	"errors"
	"time"
)

// ErrScheduleNotFound — расписание отсутствует в backend.
var ErrScheduleNotFound = errors.New("schedule not found")

// ErrScheduleConflict — новое определение расписания не совпадает
// с сохраненным документом (изменены задача, тип, интервал, выражение или таймзона).
var ErrScheduleConflict = errors.New("schedule definition conflict")

// ScheduleKind — тип периодической задачи.
type ScheduleKind string

// Возможные типы расписаний.
const (
	ScheduleEvery ScheduleKind = "every"
	ScheduleCron  ScheduleKind = "cron"
)

// ScheduleDocument — документ расписания, хранимый в backend.
// Единый источник правды для времени следующего срабатывания:
// переживает рестарты процесса и синхронизирует несколько экземпляров.
type ScheduleDocument struct {
	Name      string
	Task      string
	Kind      ScheduleKind
	Interval  time.Duration // для ScheduleEvery
	Expr      string        // для ScheduleCron
	Timezone  string        // таймзона cron (loc.String()); пустая — UTC
	Payload   []byte        // сериализованный payload
	NextRun   time.Time
	UpdatedAt time.Time
}

// ScheduleStore — опциональный интерфейс backend: хранение расписаний
// планировщика. Если backend его реализует, Scheduler автоматически
// сохраняет время следующего тика при регистрации и после каждого
// срабатывания, восстанавливает его при повторной регистрации (фаза
// расписания переживает рестарт) и удаляет документ при Remove.
type ScheduleStore interface {
	// SaveSchedule сохраняет (или обновляет) документ расписания.
	SaveSchedule(ctx context.Context, doc ScheduleDocument) error

	// GetSchedule возвращает документ расписания.
	// Возвращает ошибку, обёртывающую ErrScheduleNotFound, если его нет.
	GetSchedule(ctx context.Context, name string) (*ScheduleDocument, error)

	// ListSchedules возвращает все сохраненные расписания в порядке имени.
	ListSchedules(ctx context.Context) ([]ScheduleDocument, error)

	// DeleteSchedule удаляет документ расписания.
	DeleteSchedule(ctx context.Context, name string) error
}
