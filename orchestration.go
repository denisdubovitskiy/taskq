package taskq

import (
	"encoding/json"
	"fmt"
)

// Заголовки оркестрационных задач (цепочки и аккорды).
const (
	// HeaderChainID — идентификатор цепочки.
	HeaderChainID = "taskq.chain_id"
	// HeaderChainStep — индекс текущего шага в цепочке, начиная с нуля.
	HeaderChainStep = "taskq.chain_step"
	// HeaderChainSteps — JSON-список всех шагов цепочки.
	HeaderChainSteps = "taskq.chain_steps"

	// HeaderChordID — идентификатор аккорда.
	HeaderChordID = "taskq.chord_id"
	// HeaderChordIndex — индекс задачи внутри группы аккорда.
	HeaderChordIndex = "taskq.chord_index"
	// HeaderChordTotal — количество задач в группе аккорда.
	HeaderChordTotal = "taskq.chord_total"
	// HeaderChordCallbackTask — имя callback-задачи.
	HeaderChordCallbackTask = "taskq.chord_callback_task"
	// HeaderChordCallbackQueue — очередь callback-задачи.
	HeaderChordCallbackQueue = "taskq.chord_callback_queue"
	// HeaderChordCallbackID — идентификатор callback-задачи.
	HeaderChordCallbackID = "taskq.chord_callback_id"
)

// chainStepRef — ссылка на шаг цепочки, хранится в заголовках задачи.
type chainStepRef struct {
	ID    string `json:"id"`
	Task  string `json:"task"`
	Queue string `json:"queue,omitempty"`
}

// encodeChainSteps сериализует список шагов цепочки для заголовка.
func encodeChainSteps(steps []chainStepRef) (string, error) {
	data, err := json.Marshal(steps)
	if err != nil {
		return "", fmt.Errorf("encode chain steps: %w", err)
	}
	return string(data), nil
}

// decodeChainSteps десериализует список шагов цепочки из заголовка.
func decodeChainSteps(data string) ([]chainStepRef, error) {
	var steps []chainStepRef
	if err := json.Unmarshal([]byte(data), &steps); err != nil {
		return nil, fmt.Errorf("decode chain steps: %w", err)
	}
	return steps, nil
}
