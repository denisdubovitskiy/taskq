package internal

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// JobIDPrefix — префикс для идентификаторов задач.
const JobIDPrefix = "job_"

// GenerateID создает случайный идентификатор задачи.
func GenerateID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}
	return JobIDPrefix + hex.EncodeToString(b[:]), nil
}
