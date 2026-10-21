package job

import (
	"errors"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		env     Envelope
		wantErr error
	}{
		{name: "ok", env: Envelope{ID: "email-1", Attempt: 1}},
		{name: "empty id", env: Envelope{Attempt: 1}, wantErr: ErrEmptyID},
		{name: "zero attempt", env: Envelope{ID: "email-1"}, wantErr: ErrBadAttempt},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
