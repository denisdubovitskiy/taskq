// Package memory содержит in-memory реализацию taskq.Locker для тестов.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/denisdubovitskiy/taskq"
)

// Locker — in-memory хранилище блокировок.
// Ключ блокируется до вызова Release или истечения TTL.
type Locker struct {
	mu    sync.Mutex
	now   func() time.Time
	seq   uint64
	locks map[string]lockRecord
}

// lockRecord — владелец ключа: токен блокировки и время истечения.
type lockRecord struct {
	token    uint64
	expireAt time.Time
}

// NewLocker создает in-memory Locker.
func NewLocker() *Locker {
	return &Locker{
		now:   time.Now,
		locks: make(map[string]lockRecord),
	}
}

// Lock блокирует ключ на ttl.
// Возвращает ошибку, если ключ занят и еще не истек.
func (l *Locker) Lock(ctx context.Context, key string, ttl time.Duration) (taskq.Lock, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if rec, ok := l.locks[key]; ok && l.now().Before(rec.expireAt) {
		return nil, fmt.Errorf("key %q is already locked", key)
	}

	l.seq++
	l.locks[key] = lockRecord{
		token:    l.seq,
		expireAt: l.now().Add(ttl),
	}

	return &lock{
		locker: l,
		key:    key,
		token:  l.seq,
	}, nil
}

// lock — acquired lock с токеном, чтобы Release не снял
// блокировку, захваченную другим владельцем после истечения TTL.
type lock struct {
	locker *Locker
	key    string
	token  uint64
}

// Release снимает блокировку, если она еще не истекла
// и принадлежит этому владельцу.
func (lk *lock) Release(ctx context.Context) error {
	lk.locker.mu.Lock()
	defer lk.locker.mu.Unlock()

	rec, ok := lk.locker.locks[lk.key]
	if !ok || rec.token != lk.token {
		return nil
	}
	if lk.locker.now().Before(rec.expireAt) {
		delete(lk.locker.locks, lk.key)
	}
	return nil
}
