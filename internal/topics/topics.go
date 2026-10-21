package topics

import (
	"fmt"

	"reliable-orchestration/internal/route"
)

const (
	Live  = "agent.jobs"
	Retry = "agent.jobs.retry"
	DLQ   = "agent.jobs.dlq"
)

// For maps a routing decision onto the topic that receives the job.
func For(dest route.Destination) (string, error) {
	switch dest {
	case route.DestRetry:
		return Retry, nil
	case route.DestDLQ:
		return DLQ, nil
	default:
		return "", fmt.Errorf("no topic for destination %q", dest)
	}
}
