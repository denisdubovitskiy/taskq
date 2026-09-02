package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/redis/go-redis/v9"
)

// errBackendClosed — backend закрыт.
var errBackendClosed = errors.New("redis backend is closed")

// Backend — реализация taskq.Backend на Redis.
//
// Состояние задачи — хэш "<prefix>job:<id>" (state, error, created_at,
// updated_at), результат — ключ "<prefix>job:<id>:result".
// Ключи имеют TTL (WithResultTTL, по умолчанию 24 часа).
type Backend struct {
	client *redis.Client
	cfg    config
	closed atomic.Bool
}

// NewBackend создает backend. Клиент передается вызывающим кодом и
// закрывается им же (Close backend не закрывает клиент).
func NewBackend(client *redis.Client, opts ...Option) (*Backend, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	cfg := defaultConfig()
	applyOptions(&cfg, opts)

	return &Backend{client: client, cfg: cfg}, nil
}

// SetState сохраняет состояние задачи. created_at фиксируется один раз —
// при первом обращении, повторные вызовы обновляют только state и updated_at.
func (b *Backend) SetState(ctx context.Context, jobID string, state taskq.State) error {
	if b.closed.Load() {
		return errBackendClosed
	}
	if jobID == "" {
		return errors.New("job id is empty")
	}

	now := time.Now().UTC().UnixMilli()
	pipe := b.client.Pipeline()
	pipe.HSet(ctx, b.keyFor(jobID), "state", string(state), "updated_at", now)
	pipe.HSetNX(ctx, b.keyFor(jobID), "created_at", now)
	if b.cfg.resultTTL > 0 {
		b.pexpire(pipe, ctx, b.keyFor(jobID))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
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

	pipe := b.client.Pipeline()
	pipe.Set(ctx, b.resultKeyFor(jobID), data, b.ttl())
	_, err := pipe.Exec(ctx)
	if err != nil {
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

	now := time.Now().UTC().UnixMilli()
	pipe := b.client.Pipeline()
	pipe.HSet(ctx, b.keyFor(jobID), "error", err, "updated_at", now)
	pipe.HSetNX(ctx, b.keyFor(jobID), "created_at", now)
	if b.cfg.resultTTL > 0 {
		b.pexpire(pipe, ctx, b.keyFor(jobID))
	}

	_, errExec := pipe.Exec(ctx)
	if errExec != nil {
		return fmt.Errorf("set error of job %s: %w", jobID, errExec)
	}
	return nil
}

// GetState возвращает состояние задачи.
// Для неизвестной задачи возвращает ошибку, обертывающую taskq.ErrJobNotFound.
func (b *Backend) GetState(ctx context.Context, jobID string) (*taskq.JobState, error) {
	if b.closed.Load() {
		return nil, errBackendClosed
	}

	vals, err := b.client.HGetAll(ctx, b.keyFor(jobID)).Result()
	if err != nil {
		return nil, fmt.Errorf("get state of job %s: %w", jobID, err)
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("job %s: %w", jobID, taskq.ErrJobNotFound)
	}

	return &taskq.JobState{
		State:     taskq.State(vals["state"]),
		Error:     vals["error"],
		CreatedAt: parseMillis(vals["created_at"]),
		UpdatedAt: parseMillis(vals["updated_at"]),
	}, nil
}

// GetResult возвращает результат задачи.
// Если результата нет, возвращает ошибку, обертывающую taskq.ErrJobNotFound.
func (b *Backend) GetResult(ctx context.Context, jobID string) (*taskq.JobResult, error) {
	if b.closed.Load() {
		return nil, errBackendClosed
	}

	data, err := b.client.Get(ctx, b.resultKeyFor(jobID)).Bytes()
	if errors.Is(err, redis.Nil) {
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

	if _, err := b.client.Del(ctx, b.keyFor(jobID), b.resultKeyFor(jobID)).Result(); err != nil {
		return fmt.Errorf("purge job %s: %w", jobID, err)
	}
	return nil
}

// Close помечает backend закрытым: дальнейшие операции возвращают ошибку.
// Переданный *redis.Client закрывается вызывающим кодом.
// Повторный вызов безопасен.
func (b *Backend) Close(ctx context.Context) error {
	b.closed.Store(true)
	return nil
}

// pexpire ставит TTL с миллисекундной точностью:
// go-redis округляет EXPIRE для значений меньше секунды.
func (b *Backend) pexpire(pipe redis.Pipeliner, ctx context.Context, key string) {
	pipe.Do(ctx, "pexpire", key, b.cfg.resultTTL.Milliseconds())
}

// keyFor возвращает ключ хэша состояния задачи.
func (b *Backend) keyFor(jobID string) string {
	return b.cfg.prefix + "job:" + jobID
}

// resultKeyFor возвращает ключ результата задачи.
func (b *Backend) resultKeyFor(jobID string) string {
	return b.cfg.prefix + "job:" + jobID + ":result"
}

// ttl возвращает TTL ключей (0 — без истечения).
func (b *Backend) ttl() time.Duration {
	return b.cfg.resultTTL
}

// parseMillis разбирает метку времени в миллисекундах UNIX;
// пустое или некорректное значение — zero time.
func parseMillis(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
