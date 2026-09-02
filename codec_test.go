package taskq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONCodec(t *testing.T) {
	t.Parallel()

	// Проверяем сериализацию и десериализацию простой структуры.
	t.Run("encode and decode", func(t *testing.T) {
		t.Parallel()

		// arrange
		codec := NewJSONCodec()
		type payload struct {
			Value int    `json:"value"`
			Name  string `json:"name"`
		}
		input := payload{Value: 42, Name: "answer"}

		// act
		data, err := codec.Encode(input)
		require.NoError(t, err)

		var decoded payload
		err = codec.Decode(data, &decoded)

		// assert
		require.NoError(t, err)
		assert.Equal(t, input, decoded)
	})

	// Проверяем, что ContentType возвращает JSON.
	t.Run("content type", func(t *testing.T) {
		t.Parallel()

		// arrange
		codec := NewJSONCodec()

		// act
		contentType := codec.ContentType()

		// assert
		assert.Equal(t, "application/json", contentType)
	})

	// Проверяем ошибку при невалидном JSON.
	t.Run("decode invalid json", func(t *testing.T) {
		t.Parallel()

		// arrange
		codec := NewJSONCodec()
		var value int

		// act
		err := codec.Decode([]byte("not json"), &value)

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "decode payload")
	})

	// Проверяем ошибку при сериализации канала.
	t.Run("encode unsupported value", func(t *testing.T) {
		t.Parallel()

		// arrange
		codec := NewJSONCodec()

		// act
		_, err := codec.Encode(make(chan int))

		// assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "encode payload")
	})
}
