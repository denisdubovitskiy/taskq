package internal

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateID(t *testing.T) {
	t.Parallel()

	// Проверяем, что идентификатор начинается с префикса и имеет достаточную длину.
	t.Run("format", func(t *testing.T) {
		t.Parallel()

		// act
		id, err := GenerateID()

		// assert
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(id, JobIDPrefix))
		assert.Greater(t, len(id), len(JobIDPrefix))
	})

	// Проверяем уникальность сгенерированных идентификаторов.
	t.Run("unique", func(t *testing.T) {
		t.Parallel()

		// arrange
		const count = 100
		ids := make(map[string]struct{}, count)

		// act
		for range count {
			id, err := GenerateID()
			require.NoError(t, err)
			ids[id] = struct{}{}
		}

		// assert
		assert.Len(t, ids, count)
	})
}
