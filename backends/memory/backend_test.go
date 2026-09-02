package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
)

func TestBackend_SetState(t *testing.T) {
	t.Parallel()

	// Проверяем сохранение и получение состояния.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend := NewBackend()

		// act
		err := backend.SetState(ctx, "job-1", taskq.StatePending)
		require.NoError(t, err)

		state, err := backend.GetState(ctx, "job-1")

		// assert
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, taskq.StatePending, state.State)
	})
}

func TestBackend_SetResult(t *testing.T) {
	t.Parallel()

	// Проверяем сохранение результата.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend := NewBackend()
		wantData := []byte(`{"sum":5}`)

		// act
		err := backend.SetResult(ctx, "job-1", wantData)
		require.NoError(t, err)

		result, err := backend.GetResult(ctx, "job-1")

		// assert
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, wantData, result.Data)
	})

	// Проверяем ошибку при отсутствии результата.
	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend := NewBackend()

		// act
		result, err := backend.GetResult(ctx, "missing")

		// assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestBackend_SetError(t *testing.T) {
	t.Parallel()

	// Проверяем сохранение ошибки.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend := NewBackend()

		// act
		err := backend.SetError(ctx, "job-1", "something went wrong")
		require.NoError(t, err)

		state, err := backend.GetState(ctx, "job-1")

		// assert
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, "something went wrong", state.Error)
	})
}

func TestBackend_GetState(t *testing.T) {
	t.Parallel()

	// Проверяем ошибку при отсутствии задачи.
	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend := NewBackend()

		// act
		state, err := backend.GetState(ctx, "missing")

		// assert
		require.Error(t, err)
		assert.Nil(t, state)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestBackend_Purge(t *testing.T) {
	t.Parallel()

	// Проверяем удаление задачи.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend := NewBackend()
		err := backend.SetState(ctx, "job-1", taskq.StateSuccess)
		require.NoError(t, err)
		err = backend.SetResult(ctx, "job-1", []byte(`{}`))
		require.NoError(t, err)

		// act
		err = backend.Purge(ctx, "job-1")
		require.NoError(t, err)

		// assert
		_, err = backend.GetState(ctx, "job-1")
		assert.Error(t, err)
		_, err = backend.GetResult(ctx, "job-1")
		assert.Error(t, err)
	})
}

func TestBackend_Close(t *testing.T) {
	t.Parallel()

	// Проверяем, что Close не возвращает ошибку.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend := NewBackend()

		// act
		err := backend.Close(ctx)

		// assert
		assert.NoError(t, err)
	})
}
