package taskq

import (
	"encoding/json"
	"fmt"
)

// Codec отвечает за сериализацию payload и результата.
// По умолчанию используется JSON.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	ContentType() string
}

// JSONCodec — кодек на основе encoding/json.
type JSONCodec struct{}

// NewJSONCodec создает JSON-кодек.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode сериализует значение в JSON.
func (c *JSONCodec) Encode(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return data, nil
}

// Decode десериализует JSON в значение.
func (c *JSONCodec) Decode(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	return nil
}

// ContentType возвращает MIME-тип JSON.
func (c *JSONCodec) ContentType() string {
	return "application/json"
}
