package postgresql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// lockerSQL — SQL-запросы locker (схема подставлена).
type lockerSQL struct {
	purge   string
	lock    string
	release string
}

// Locker — реализация taskq.Locker на PostgreSQL.
//
// Таблица tq_locks (key, token, expires_at): захват атомарен
// (INSERT ... ON CONFLICT DO UPDATE только для просроченной строки),
// Release снимает блокировку по токену — «старый» владелец после
// истечения TTL не снимет блокировку нового владельца.
type Locker struct {
	pool   *pgxpool.Pool
	sql    lockerSQL
	closed atomic.Bool
}

// NewLocker создает locker: подключается к PostgreSQL, проверяет связь
// и применяет миграции. Close закрывает connection pool.
func NewLocker(ctx context.Context, dsn string, opts ...Option) (*Locker, error) {
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
	return &Locker{
		pool: pool,
		sql: lockerSQL{
			purge: `DELETE FROM ` + q + `.tq_locks WHERE expires_at <= now()`,
			lock: `
INSERT INTO ` + q + `.tq_locks (key, token, expires_at)
VALUES ($1, $2, now() + ($3::float8 * interval '1 millisecond'))
ON CONFLICT (key) DO UPDATE
SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
WHERE ` + q + `.tq_locks.expires_at <= now()
RETURNING token`,
			release: `DELETE FROM ` + q + `.tq_locks WHERE key = $1 AND token = $2`,
		},
	}, nil
}

// Lock блокирует ключ на ttl.
// Возвращает ошибку, если ключ занят и еще не истек.
func (l *Locker) Lock(ctx context.Context, key string, ttl time.Duration) (taskq.Lock, error) {
	if l.closed.Load() {
		return nil, errors.New("postgresql locker is closed")
	}
	if key == "" {
		return nil, errors.New("lock key is empty")
	}
	if ttl <= 0 {
		return nil, errors.New("lock ttl must be positive")
	}

	token, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	// Очистка просроченных блокировок (best effort).
	if _, err := l.pool.Exec(ctx, l.sql.purge); err != nil {
		return nil, fmt.Errorf("purge expired locks: %w", err)
	}

	var got string
	err = l.pool.QueryRow(ctx, l.sql.lock, key, token, ttl.Milliseconds()).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("key %q is already locked", key)
	}
	if err != nil {
		return nil, fmt.Errorf("acquire lock %q: %w", key, err)
	}

	return &lock{locker: l, key: key, token: got}, nil
}

// Close закрывает locker и connection pool.
func (l *Locker) Close(ctx context.Context) error {
	l.closed.Store(true)
	l.pool.Close()
	return nil
}

// lock — захваченная блокировка с токеном владельца.
type lock struct {
	locker *Locker
	key    string
	token  string
}

// Release снимает блокировку, если она принадлежит этому владельцу.
// Снятие чужой блокировки невозможно (токен не совпадает — 0 строк).
func (lk *lock) Release(ctx context.Context) error {
	if _, err := lk.locker.pool.Exec(ctx, lk.locker.sql.release, lk.key, lk.token); err != nil {
		return fmt.Errorf("release lock %q: %w", lk.key, err)
	}
	return nil
}

// randomToken генерирует случайный токен владельца (32 hex).
func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
