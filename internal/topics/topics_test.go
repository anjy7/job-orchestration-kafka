package topics

import (
	"testing"

	"reliable-orchestration/internal/route"
)

func TestFor(t *testing.T) {
	got, err := For(route.DestRetry)
	if err != nil || got != Retry {
		t.Fatalf("retry topic = %q, %v", got, err)
	}
	got, err = For(route.DestDLQ)
	if err != nil || got != DLQ {
		t.Fatalf("dead-letter topic = %q, %v", got, err)
	}
	if _, err := For(route.DestNone); err == nil {
		t.Fatal("expected error for an empty destination")
	}
}
