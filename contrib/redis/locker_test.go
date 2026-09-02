package redis

import (
	"context"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/stretchr/testify/require"
)

// TestLocker_LockRelease проверяет базовый сценарий:
// блокировка -> повторная попытка дает ошибку -> снятие -> блокировка снова.
func TestLocker_LockRelease(t *testing.T) {
	locker := freshLocker(t)
	ctx := context.Background()

	lock, err := locker.Lock(ctx, "task-1", time.Minute)
	require.NoError(t, err)

	_, err = locker.Lock(ctx, "task-1", time.Minute)
	require.Error(t, err)

	require.NoError(t, lock.Release(ctx))

	lock2, err := locker.Lock(ctx, "task-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, lock2.Release(ctx))
}

// TestLocker_TTLExpiry проверяет, что блокировка снимается по истечении TTL,
// даже если Release не вызывали.
func TestLocker_TTLExpiry(t *testing.T) {
	locker := freshLocker(t)
	ctx := context.Background()

	_, err := locker.Lock(ctx, "task-1", 150*time.Millisecond)
	require.NoError(t, err)

	_, err = locker.Lock(ctx, "task-1", 150*time.Millisecond)
	require.Error(t, err)

	time.Sleep(300 * time.Millisecond)

	lock, err := locker.Lock(ctx, "task-1", 150*time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, lock.Release(ctx))
}

// TestLocker_StaleRelease проверяет, что Release «старого» владельца
// (после истечения его TTL) не снимает блокировку нового владельца.
func TestLocker_StaleRelease(t *testing.T) {
	locker := freshLocker(t)
	ctx := context.Background()

	lock1, err := locker.Lock(ctx, "task-1", 150*time.Millisecond)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	lock2, err := locker.Lock(ctx, "task-1", time.Minute)
	require.NoError(t, err)

	// «Старый» владелец снимает блокировку после истечения своего TTL.
	require.NoError(t, lock1.Release(ctx))

	// Блокировка нового владельца не пострадала.
	_, err = locker.Lock(ctx, "task-1", time.Minute)
	require.Error(t, err)

	require.NoError(t, lock2.Release(ctx))
}

// TestLocker_IndependentKeys проверяет независимость разных ключей.
func TestLocker_IndependentKeys(t *testing.T) {
	locker := freshLocker(t)
	ctx := context.Background()

	l1, err := locker.Lock(ctx, "key-1", time.Minute)
	require.NoError(t, err)

	l2, err := locker.Lock(ctx, "key-2", time.Minute)
	require.NoError(t, err)

	require.NoError(t, l1.Release(ctx))
	require.NoError(t, l2.Release(ctx))
}

// TestLocker_Validation проверяет валидацию аргументов.
func TestLocker_Validation(t *testing.T) {
	locker := freshLocker(t)
	ctx := context.Background()

	_, err := locker.Lock(ctx, "", time.Minute)
	require.Error(t, err)

	_, err = locker.Lock(ctx, "key-1", 0)
	require.Error(t, err)

	_, err = NewLocker(nil)
	require.Error(t, err)
}

// TestLocker_Interface проверяет, что Locker реализует taskq.Locker.
func TestLocker_Interface(t *testing.T) {
	var _ taskq.Locker = freshLocker(t)
}
