package postgresql

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaNameRE — допустимое имя схемы (идентификатор без кавычек).
var schemaNameRE = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// quoteIdent экранирует SQL-идентификатор.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// migrate создает таблицы и индексы, если их еще нет. Идемпотентна.
func migrate(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	if !schemaNameRE.MatchString(schema) {
		return fmt.Errorf("invalid schema name %q", schema)
	}
	q := quoteIdent(schema)

	stmts := []string{
		// Брокер: очередь задач.
		`CREATE TABLE IF NOT EXISTS ` + q + `.tq_jobs (
			id         TEXT PRIMARY KEY,
			queue      TEXT NOT NULL,
			body       JSONB NOT NULL,
			eta        TIMESTAMPTZ,
			status     TEXT NOT NULL,
			owner      TEXT,
			claimed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS tq_jobs_claim_idx ON ` + q + `.tq_jobs (queue, status, created_at)`,

		// Backend: состояния задач.
		`CREATE TABLE IF NOT EXISTS ` + q + `.tq_job_states (
			id         TEXT PRIMARY KEY,
			state      TEXT NOT NULL,
			error      TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)`,

		// Backend: результаты задач.
		`CREATE TABLE IF NOT EXISTS ` + q + `.tq_job_results (
			id         TEXT PRIMARY KEY,
			data       BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		)`,

		// Locker: распределенные блокировки.
		`CREATE TABLE IF NOT EXISTS ` + q + `.tq_locks (
			key        TEXT PRIMARY KEY,
			token      TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL
		)`,
	}

	for _, stmt := range stmts {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migrate schema %s: %w", schema, err)
		}
	}
	return nil
}

// newPool создает connection pool, проверяет подключение и применяет
// миграции. Ошибка — пул закрыт.
func newPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	poolCfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgresql: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgresql: %w", err)
	}

	return pool, nil
}
