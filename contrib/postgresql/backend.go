package postgresql

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errBackendClosed — backend закрыт.
var errBackendClosed = errors.New("postgresql backend is closed")

// Backend — реализация taskq.Backend на PostgreSQL.
//
// Состояние задачи — таблица tq_job_states (id, state, error, created_at,
// updated_at), результат — tq_job_results (id, data). TTL отсутствует:
// строки живут до Purge или внешней очистки.
type Backend struct {
	pool   *pgxpool.Pool
	sql    backendSQL
	closed atomic.Bool
}

// backendSQL — SQL-запросы backend (схема подставлена).
type backendSQL struct {
	setState    string
	setResult   string
	setError    string
	getState    string
	getResult   string
	purgeState  string
	purgeResult string
}

// NewBackend создает backend: подключается к PostgreSQL, проверяет связь
// и применяет миграции. Close закрывает connection pool.
func NewBackend(ctx context.Context, dsn string, opts ...Option) (*Backend, error) {
	if dsn == "" {
		return nil, errors.New("dsn is empty")
	}

	cfg := defaultConfig()
	applyOptions(&cfg, opts)

	pool, err := newPool(ctx, dsn, cfg.maxConns)
	if err != nil {
		return nil, err
	}

	if err := migrate(ctx, pool, cfg.schema); err != nil {
		pool.Close()
		return nil, err
	}

	q := quoteIdent(cfg.schema)
	return &Backend{
		pool: pool,
		sql: backendSQL{
			setState: `
INSERT INTO ` + q + `.tq_job_states (id, state, created_at, updated_at)
VALUES ($1, $2, now(), now())
ON CONFLICT (id) DO UPDATE
SET state = EXCLUDED.state, updated_at = now()`,
			setResult: `
INSERT INTO ` + q + `.tq_job_results (id, data, created_at)
VALUES ($1, $2, now())
ON CONFLICT (id) DO UPDATE
SET data = EXCLUDED.data`,
			setError: `
INSERT INTO ` + q + `.tq_job_states (id, state, error, created_at, updated_at)
VALUES ($1, '', $2, now(), now())
ON CONFLICT (id) DO UPDATE
SET error = EXCLUDED.error, updated_at = now()`,
			getState:    `SELECT state, error, created_at, updated_at FROM ` + q + `.tq_job_states WHERE id = $1`,
			getResult:   `SELECT data FROM ` + q + `.tq_job_results WHERE id = $1`,
			purgeState:  `DELETE FROM ` + q + `.tq_job_states WHERE id = $1`,
			purgeResult: `DELETE FROM ` + q + `.tq_job_results WHERE id = $1`,
		},
	}, nil
}

// SetState сохраняет состояние задачи. created_at фиксируется один раз —
// при первом обращении, повторные вызовы обновляют только state.
func (b *Backend) SetState(ctx context.Context, jobID string, state taskq.State) error {
	if b.closed.Load() {
		return errBackendClosed
	}
	if jobID == "" {
		return errors.New("job id is empty")
	}

	if _, err := b.pool.Exec(ctx, b.sql.setState, jobID, string(state)); err != nil {
		return fmt.Errorf("set state of job %s: %w", jobID, err)
	}
	return nil
}

// SetResult сохраняет результат задачи.
func (b *Backend) SetResult(ctx context.Context, jobID string, data []byte) error {
	if b.closed.Load() {
		return errBackendClosed
	}
	if jobID == "" {
		return errors.New("job id is empty")
	}

	if _, err := b.pool.Exec(ctx, b.sql.setResult, jobID, data); err != nil {
		return fmt.Errorf("set result of job %s: %w", jobID, err)
	}
	return nil
}

// SetError сохраняет последнюю ошибку задачи. Если записи состояния еще нет,
// она создается (как в in-memory backend).
func (b *Backend) SetError(ctx context.Context, jobID string, err string) error {
	if b.closed.Load() {
		return errBackendClosed
	}
	if jobID == "" {
		return errors.New("job id is empty")
	}

	if _, err := b.pool.Exec(ctx, b.sql.setError, jobID, err); err != nil {
		return fmt.Errorf("set error of job %s: %w", jobID, err)
	}
	return nil
}

// GetState возвращает состояние задачи.
// Для неизвестной задачи возвращает ошибку, обертывающую taskq.ErrJobNotFound.
func (b *Backend) GetState(ctx context.Context, jobID string) (*taskq.JobState, error) {
	if b.closed.Load() {
		return nil, errBackendClosed
	}

	var (
		state     string
		errText   string
		createdAt time.Time
		updatedAt time.Time
	)
	err := b.pool.QueryRow(ctx, b.sql.getState, jobID).Scan(&state, &errText, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get state of job %s: %w", jobID, err)
	}

	return &taskq.JobState{
		State:     taskq.State(state),
		Error:     errText,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// GetResult возвращает результат задачи.
// Если результата нет, возвращает ошибку, обертывающую taskq.ErrJobNotFound.
func (b *Backend) GetResult(ctx context.Context, jobID string) (*taskq.JobResult, error) {
	if b.closed.Load() {
		return nil, errBackendClosed
	}

	var data []byte
	err := b.pool.QueryRow(ctx, b.sql.getResult, jobID).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("result of job %s: %w", jobID, taskq.ErrJobNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get result of job %s: %w", jobID, err)
	}

	return &taskq.JobResult{Data: data}, nil
}

// Purge удаляет состояние и результат задачи.
func (b *Backend) Purge(ctx context.Context, jobID string) error {
	if b.closed.Load() {
		return errBackendClosed
	}

	if _, err := b.pool.Exec(ctx, b.sql.purgeState, jobID); err != nil {
		return fmt.Errorf("purge job %s: %w", jobID, err)
	}
	if _, err := b.pool.Exec(ctx, b.sql.purgeResult, jobID); err != nil {
		return fmt.Errorf("purge job %s: %w", jobID, err)
	}
	return nil
}

// Close закрывает backend и connection pool.
// Дальнейшие операции возвращают ошибку. Повторный вызов безопасен.
func (b *Backend) Close(ctx context.Context) error {
	b.closed.Store(true)
	b.pool.Close()
	return nil
}
