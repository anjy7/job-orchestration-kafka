package job

import "errors"

var (
	ErrEmptyID    = errors.New("job id is empty")
	ErrBadAttempt = errors.New("attempt must be at least 1")
)

// Envelope is one agent run. Attempt starts at 1 on the live topic.
// A retry carries the same job with the next attempt number.
type Envelope struct {
	ID      string
	Payload []byte
	Attempt int
	Error   string
}

func (e Envelope) Validate() error {
	if e.ID == "" {
		return ErrEmptyID
	}
	if e.Attempt < 1 {
		return ErrBadAttempt
	}
	return nil
}
