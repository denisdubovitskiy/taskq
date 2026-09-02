package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/redis/go-redis/v9"
)

// releaseScript снимает блокировку только если токен совпадает:
// Release «старого» владельца не снимет блокировку, захваченную
// другим владельцем после истечения TTL.
const releaseScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0`

// Locker — реализация taskq.Locker на Redis.
//
// Блокировка — SET key token NX PX ttl по ключу "<prefix>lock:<key>";
// снятие — Lua-скрипт compare-and-delete по токену.
type Locker struct {
	client *redis.Client
	prefix string
}

// NewLocker создает locker. Клиент передается вызывающим кодом и
// закрывается им же.
func NewLocker(client *redis.Client, opts ...Option) (*Locker, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	cfg := defaultConfig()
	applyOptions(&cfg, opts)

	return &Locker{client: client, prefix: cfg.prefix}, nil
}

// Lock блокирует ключ на ttl.
// Возвращает ошибку, если ключ занят и еще не истек.
func (l *Locker) Lock(ctx context.Context, key string, ttl time.Duration) (taskq.Lock, error) {
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

	locked, err := l.client.SetNX(ctx, l.keyFor(key), token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire lock %q: %w", key, err)
	}
	if !locked {
		return nil, fmt.Errorf("key %q is already locked", key)
	}

	return &lock{
		client: l.client,
		key:    l.keyFor(key),
		token:  token,
	}, nil
}

// lock — захваченная блокировка с токеном владельца.
type lock struct {
	client *redis.Client
	key    string
	token  string
}

// Release снимает блокировку, если она принадлежит этому владельцу.
// Повторный вызов и снятие после истечения TTL — не ошибка.
func (lk *lock) Release(ctx context.Context) error {
	_, err := lk.client.Eval(ctx, releaseScript, []string{lk.key}, lk.token).Result()
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	return nil
}

// keyFor возвращает ключ блокировки.
func (l *Locker) keyFor(key string) string {
	return l.prefix + "lock:" + key
}

// randomToken генерирует случайный токен владельца (32 hex).
func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
