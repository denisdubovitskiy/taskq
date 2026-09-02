package redis

import (
	"context"
	"testing"
	"time"

	"github.com/denisdubovitskiy/taskq"
	"github.com/stretchr/testify/require"
)

// TestBackend_StateRoundtrip проверяет сохранение и чтение состояния.
func TestBackend_StateRoundtrip(t *testing.T) {
	backend := freshBackend(t)
	ctx := context.Background()

	require.NoError(t, backend.SetState(ctx, "job-1", taskq.StatePending))

	state, err := backend.GetState(ctx, "job-1")
	require.NoError(t, err)
	require.Equal(t, taskq.StatePending, state.State)
	require.NotZero(t, state.CreatedAt)
	require.NotZero(t, state.UpdatedAt)
}

// TestBackend_StateUpdatePreservesCreatedAt проверяет, что повторные
// обновления состояния не сбрасывают created_at.
func TestBackend_StateUpdatePreservesCreatedAt(t *testing.T) {
	backend := freshBackend(t)
	ctx := context.Background()

	require.NoError(t, backend.SetState(ctx, "job-1", taskq.StatePending))
	first, err := backend.GetState(ctx, "job-1")
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	require.NoError(t, backend.SetState(ctx, "job-1", taskq.StateSuccess))

	second, err := backend.GetState(ctx, "job-1")
	require.NoError(t, err)
	require.Equal(t, taskq.StateSuccess, second.State)
	require.True(t, second.CreatedAt.Equal(first.CreatedAt), "created_at сместился")
}

// TestBackend_GetStateNotFound проверяет ошибку для неизвестной задачи.
func TestBackend_GetStateNotFound(t *testing.T) {
	backend := freshBackend(t)

	_, err := backend.GetState(context.Background(), "missing")
	require.ErrorIs(t, err, taskq.ErrJobNotFound)
}

// TestBackend_ResultRoundtrip проверяет сохранение и чтение результата.
func TestBackend_ResultRoundtrip(t *testing.T) {
	backend := freshBackend(t)
	ctx := context.Background()

	data := []byte(`{"sum":5}`)
	require.NoError(t, backend.SetResult(ctx, "job-1", data))

	res, err := backend.GetResult(ctx, "job-1")
	require.NoError(t, err)
	require.Equal(t, data, res.Data)
}

// TestBackend_GetResultNotFound проверяет ошибку для отсутствующего результата.
func TestBackend_GetResultNotFound(t *testing.T) {
	backend := freshBackend(t)

	_, err := backend.GetResult(context.Background(), "missing")
	require.ErrorIs(t, err, taskq.ErrJobNotFound)
}

// TestBackend_SetError проверяет сохранение ошибки в состоянии задачи.
func TestBackend_SetError(t *testing.T) {
	backend := freshBackend(t)
	ctx := context.Background()

	require.NoError(t, backend.SetState(ctx, "job-1", taskq.StatePending))
	require.NoError(t, backend.SetError(ctx, "job-1", "boom"))

	state, err := backend.GetState(ctx, "job-1")
	require.NoError(t, err)
	require.Equal(t, "boom", state.Error)
}

// TestBackend_Purge проверяет удаление состояния и результата.
func TestBackend_Purge(t *testing.T) {
	backend := freshBackend(t)
	ctx := context.Background()

	require.NoError(t, backend.SetState(ctx, "job-1", taskq.StateSuccess))
	require.NoError(t, backend.SetResult(ctx, "job-1", []byte(`{}`)))

	require.NoError(t, backend.Purge(ctx, "job-1"))

	_, err := backend.GetState(ctx, "job-1")
	require.ErrorIs(t, err, taskq.ErrJobNotFound)

	_, err = backend.GetResult(ctx, "job-1")
	require.ErrorIs(t, err, taskq.ErrJobNotFound)
}

// TestBackend_TTL проверяет, что ключи истекают по заданному TTL.
func TestBackend_TTL(t *testing.T) {
	backend := freshBackend(t, WithResultTTL(200*time.Millisecond))
	ctx := context.Background()

	require.NoError(t, backend.SetState(ctx, "job-1", taskq.StateSuccess))
	require.NoError(t, backend.SetResult(ctx, "job-1", []byte(`{}`)))

	time.Sleep(400 * time.Millisecond)

	_, err := backend.GetState(ctx, "job-1")
	require.ErrorIs(t, err, taskq.ErrJobNotFound)

	_, err = backend.GetResult(ctx, "job-1")
	require.ErrorIs(t, err, taskq.ErrJobNotFound)
}

// TestBackend_Close проверяет, что после Close операции возвращают ошибку.
func TestBackend_Close(t *testing.T) {
	backend := freshBackend(t)

	require.NoError(t, backend.Close(context.Background()))
	require.ErrorIs(t, backend.SetState(context.Background(), "job-1", taskq.StatePending), errBackendClosed)

	var err error
	_, err = backend.GetState(context.Background(), "job-1")
	require.ErrorIs(t, err, errBackendClosed)
}

// TestBackend_NilValidation проверяет валидацию пустых аргументов.
func TestBackend_NilValidation(t *testing.T) {
	backend := freshBackend(t)
	ctx := context.Background()

	require.Error(t, backend.SetState(ctx, "", taskq.StatePending))
	require.Error(t, backend.SetResult(ctx, "", []byte("{}")))
	require.Error(t, backend.SetError(ctx, "", "boom"))

	_, err := NewBackend(nil)
	require.Error(t, err)
}
