package taskq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestApplyMetricOptions проверяет применение опций метрики к спецификации:
// без опций — пустая спецификация, с опциями — описание и юнит.
func TestApplyMetricOptions(t *testing.T) {
	t.Parallel()

	t.Run("no options yield empty spec", func(t *testing.T) {
		t.Parallel()

		// act
		spec := ApplyMetricOptions()

		// assert
		assert.Empty(t, spec.Description)
		assert.Empty(t, spec.Unit)
	})

	t.Run("description and unit options are resolved", func(t *testing.T) {
		t.Parallel()

		// act
		spec := ApplyMetricOptions(
			WithMetricDescription("число обработанных задач"),
			WithMetricUnit("1"),
		)

		// assert
		assert.Equal(t, "число обработанных задач", spec.Description)
		assert.Equal(t, "1", spec.Unit)
	})
}
