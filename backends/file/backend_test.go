package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/denisdubovitskiy/taskq"
)

func TestNew(t *testing.T) {
	t.Parallel()

	// Проверяем создание backend с валидной директорией.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		dir := t.TempDir()

		// act
		backend, err := New(dir)

		// assert
		require.NoError(t, err)
		require.NotNil(t, backend)
	})

	// Проверяем валидацию директории.
	t.Run("dir required", func(t *testing.T) {
		t.Parallel()

		// act
		backend, err := New("")

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dir")
		assert.Nil(t, backend)
	})
}

func TestFileBackend_SetState(t *testing.T) {
	t.Parallel()

	// Проверяем сохранение и получение состояния.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		// act
		err = backend.SetState(ctx, "job-1", taskq.StatePending)
		require.NoError(t, err)

		state, err := backend.GetState(ctx, "job-1")

		// assert
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, taskq.StatePending, state.State)
	})
}

func TestFileBackend_SetResult(t *testing.T) {
	t.Parallel()

	// Проверяем сохранение результата.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)
		wantData := []byte(`{"sum":5}`)

		// act
		err = backend.SetResult(ctx, "job-1", wantData)
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
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		// act
		result, err := backend.GetResult(ctx, "missing")

		// assert
		require.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestFileBackend_SetError(t *testing.T) {
	t.Parallel()

	// Проверяем сохранение ошибки.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		// act
		err = backend.SetError(ctx, "job-1", "boom")
		require.NoError(t, err)

		state, err := backend.GetState(ctx, "job-1")

		// assert
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, "boom", state.Error)
	})
}

func TestFileBackend_Purge(t *testing.T) {
	t.Parallel()

	// Проверяем удаление задачи.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)
		err = backend.SetState(ctx, "job-1", taskq.StateSuccess)
		require.NoError(t, err)

		// act
		err = backend.Purge(ctx, "job-1")
		require.NoError(t, err)

		// assert
		_, err = backend.GetState(ctx, "job-1")
		assert.Error(t, err)
	})
}

func TestFileBackend_Close(t *testing.T) {
	t.Parallel()

	// Проверяем, что Close не возвращает ошибку.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		// act
		err = backend.Close(ctx)

		// assert
		assert.NoError(t, err)
	})
}

func TestFileBackend_Chord(t *testing.T) {
	t.Parallel()

	// Проверяем полный жизненный цикл аккорда: init, finish задач,
	// возврат результатов в порядке индексов после последней задачи.
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, backend.ChordInit(ctx, "chord-1", 3))

		// act: завершаем задачи в переставленном порядке
		var results []taskq.ChordResult

		completed, _, err := backend.ChordFinish(ctx, "chord-1", 2, []byte(`{"sum":11}`))
		require.NoError(t, err)
		assert.False(t, completed)

		completed, _, err = backend.ChordFinish(ctx, "chord-1", 0, []byte(`{"sum":3}`))
		require.NoError(t, err)
		assert.False(t, completed)

		completed, results, err = backend.ChordFinish(ctx, "chord-1", 1, []byte(`{"sum":7}`))

		// assert
		require.NoError(t, err)
		assert.True(t, completed)
		require.Len(t, results, 3)
		assert.Equal(t, 0, results[0].Index)
		assert.Equal(t, []byte(`{"sum":3}`), results[0].Result)
		assert.Equal(t, 1, results[1].Index)
		assert.Equal(t, []byte(`{"sum":7}`), results[1].Result)
		assert.Equal(t, 2, results[2].Index)
		assert.Equal(t, []byte(`{"sum":11}`), results[2].Result)
	})

	// Проверяем, что повторный finish после завершения не возвращает результатов.
	t.Run("finish after complete", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, backend.ChordInit(ctx, "chord-1", 1))

		// act
		var results []taskq.ChordResult
		completed, _, err := backend.ChordFinish(ctx, "chord-1", 0, []byte(`{}`))
		require.NoError(t, err)
		assert.True(t, completed)

		completed, results, err = backend.ChordFinish(ctx, "chord-1", 0, []byte(`{}`))

		// assert
		require.NoError(t, err)
		assert.False(t, completed)
		assert.Nil(t, results)
	})

	// Проверяем, что сбой задачи блокирует callback: последующие finish
	// не завершают аккорд, а повторный fail не возвращает true.
	t.Run("failure blocks completion", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, backend.ChordInit(ctx, "chord-1", 2))

		// act
		triggered, err := backend.ChordFail(ctx, "chord-1", 0, "boom")
		require.NoError(t, err)
		assert.True(t, triggered)

		triggered, err = backend.ChordFail(ctx, "chord-1", 1, "another boom")
		require.NoError(t, err)
		assert.False(t, triggered)

		completed, _, err := backend.ChordFinish(ctx, "chord-1", 1, []byte(`{}`))

		// assert
		require.NoError(t, err)
		assert.False(t, completed)
	})

	// Проверяем ошибку на неизвестный аккорд и выход индекса за границы.
	t.Run("not found and out of range", func(t *testing.T) {
		t.Parallel()

		// arrange
		ctx := t.Context()
		backend, err := New(t.TempDir())
		require.NoError(t, err)

		// act и assert: неизвестные id
		_, _, err = backend.ChordFinish(ctx, "missing", 0, []byte(`{}`))
		require.Error(t, err)

		_, err = backend.ChordFail(ctx, "missing", 0, "boom")
		require.Error(t, err)

		// act и assert: выход индекса за границы
		require.NoError(t, backend.ChordInit(ctx, "chord-1", 1))
		_, _, err = backend.ChordFinish(ctx, "chord-1", 1, []byte(`{}`))
		require.Error(t, err)
	})
}
