package taskq

import (
	"context"
	"time"
)

// JobStore — опциональный интерфейс backend: хранение полного документа задачи
// (payload, параметры, заголовки). Требуются для Inspect/List/Rescue.
type JobStore interface {
	// SaveJob сохраняет (или обновляет) документ задачи.
	// Состояние и ошибка при этом не меняются — им владеют SetState/SetError.
	SaveJob(ctx context.Context, job Job) error
}

// JobInspector — опциональный интерфейс backend: чтение и управление
// сохраненными задачами. Отдельный интерфейс, чтобы не раздувать Backend.
type JobInspector interface {
	// List возвращает страницу задач по фильтру.
	List(ctx context.Context, q ListQuery) (ListResult, error)

	// Inspect возвращает полный документ задачи с актуальным состоянием.
	// Возвращает ошибку, обёртывающую ErrJobNotFound, если задачи нет.
	Inspect(ctx context.Context, jobID string) (*Job, error)

	// Reset возвращает задачу в pending: очищает ошибку и сбрасывает попытку.
	// Разрешено только из states failure и dead. Перепубликация в брокер
	// не выполняется — это делает Client.Rescue.
	Reset(ctx context.Context, jobID string) error

	// Delete полностью удаляет задачу (состояние, результат, документ).
	Delete(ctx context.Context, jobID string) error
}

// ListQuery — параметры перечисления задач.
type ListQuery struct {
	// State — фильтр по состоянию; пустое значение — любые.
	State State
	// Task — фильтр по имени задачи; пустое значение — любые.
	Task string
	// Cursor — неопределяемый маркер страницы из предыдущего ListResult.
	Cursor string
	// Limit — размер страницы; по умолчанию 50, максимум 200.
	Limit int
}

// JobSummary — краткая информация о задаче для списка.
type JobSummary struct {
	ID        string
	Task      string
	State     State
	Attempt   uint32
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ListResult — страница результатов List.
type ListResult struct {
	// Items — задачи страницы.
	Items []JobSummary
	// Cursor — маркер для следующей страницы; пустой, если страниц нет.
	Cursor string
	// Done — true, когда следующих страниц нет.
	Done bool
}
