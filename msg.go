package nukafka

import (
	"context"
	"errors"
	"time"

	"encoding/json"
)

//PendingMessage custom message to handle pending, compatible for this lib only
type PendingMessage struct {
	OriginalMessage     interface{} `json:"original_message"`
	ProcessAfter        string      `json:"process_after"`
	ProcessCount        int         `json:"process_count"`
	ProcessRetryAttempt int         `json:"process_retry_attempt"`
}

//Message custom kafka message
type Message struct {
	Key     string
	Value   []byte
	Topic   string
	pclient *PubClient
	//retry indicator
	IsRetryMsg   bool
	RetryAttempt int
}

//ProcessAferDelaySecondsCtx processes the message after n second delay
func (m *Message) ProcessAferDelaySecondsCtx(ctx context.Context, second int) error {
	if m.pclient == nil {
		return errors.New("retry feature is disabled")
	}
	var iface interface{}
	json.Unmarshal(m.Value, &iface)
	pm := PendingMessage{
		OriginalMessage:     iface,
		ProcessAfter:        time.Now().Add(time.Second * time.Duration(second)).Format(time.RFC3339),
		ProcessRetryAttempt: m.RetryAttempt,
	}
	return m.pclient.SendCtx(ctx, pm)
}
