package memory_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq/lockers/memory"
)

func TestLocker_Lock(t *testing.T) {
	t.Parallel()

	// Проверяем, что свободный ключ блокируется.
	t.Run("free key", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		locker := memory.NewLocker()

		// act
		lock, err := locker.Lock(ctx, "key-1", time.Second)

		// assert
		require.NoError(t, err)
		require.NotNil(t, lock)
	})

	// Проверяем, что занятый ключ не блокируется повторно.
	t.Run("held key", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		locker := memory.NewLocker()
		_, err := locker.Lock(ctx, "key-1", time.Second)
		require.NoError(t, err)

		// act
		lock, err := locker.Lock(ctx, "key-1", time.Second)

		// assert
		require.Error(t, err)
		assert.Nil(t, lock)
		assert.Contains(t, err.Error(), "locked")
	})

	// Проверяем, что разные ключи не влияют друг на друга.
	t.Run("different keys", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		locker := memory.NewLocker()
		_, err := locker.Lock(ctx, "key-1", time.Second)
		require.NoError(t, err)

		// act
		lock, err := locker.Lock(ctx, "key-2", time.Second)

		// assert
		require.NoError(t, err)
		require.NotNil(t, lock)
	})
}

func TestLocker_Release(t *testing.T) {
	t.Parallel()

	// Проверяем, что после Release ключ снова блокируется.
	t.Run("relock after release", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		locker := memory.NewLocker()
		lock, err := locker.Lock(ctx, "key-1", time.Second)
		require.NoError(t, err)

		// act
		require.NoError(t, lock.Release(ctx))
		lockAgain, err := locker.Lock(ctx, "key-1", time.Second)

		// assert
		require.NoError(t, err)
		require.NotNil(t, lockAgain)
	})

	// Проверяем, что повторный Release не возвращает ошибку.
	t.Run("double release", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		locker := memory.NewLocker()
		lock, err := locker.Lock(ctx, "key-1", time.Second)
		require.NoError(t, err)

		// act
		require.NoError(t, lock.Release(ctx))
		err = lock.Release(ctx)

		// assert
		assert.NoError(t, err)
	})

	// Проверяем, что Release не снимает блокировку чужого ключа:
	// после истечения TTL и повторной блокировки старый Release ничего не делает.
	t.Run("release does not remove newer lock", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		locker := memory.NewLocker()
		lock, err := locker.Lock(ctx, "key-1", 30*time.Millisecond)
		require.NoError(t, err)

		// Ждем истечения TTL.
		time.Sleep(50 * time.Millisecond)

		// act: новый владелец заблокировал ключ
		newLock, err := locker.Lock(ctx, "key-1", time.Second)
		require.NoError(t, err)

		err = lock.Release(ctx)

		// assert: ключ все еще занят новым владельцем
		assert.NoError(t, err)
		_, err = locker.Lock(ctx, "key-1", time.Second)
		require.Error(t, err)
		require.NoError(t, newLock.Release(ctx))
	})
}

func TestLocker_TTL(t *testing.T) {
	t.Parallel()

	// Проверяем, что блокировка автоматически истекает по TTL.
	t.Run("expires after ttl", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		locker := memory.NewLocker()
		_, err := locker.Lock(ctx, "key-1", 30*time.Millisecond)
		require.NoError(t, err)

		// Ждем истечения TTL.
		time.Sleep(50 * time.Millisecond)

		// act
		lock, err := locker.Lock(ctx, "key-1", time.Second)

		// assert
		require.NoError(t, err)
		require.NotNil(t, lock)
	})
}
