package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkatc "github.com/testcontainers/testcontainers-go/modules/kafka"

	"reliable-orchestration/internal/agent"
	"reliable-orchestration/internal/broker"
	"reliable-orchestration/internal/codec"
	"reliable-orchestration/internal/consume"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/pipeline"
	"reliable-orchestration/internal/replay"
	"reliable-orchestration/internal/topics"
)

// The container runs Apache Kafka in KRaft mode. The Confluent local image is
// the packaging that advertises the host port back to the test process.

type cluster struct {
	brokers string
	records *codec.Codec
}

var (
	startOnce       sync.Once
	shared          *cluster
	sharedContainer *kafkatc.KafkaContainer
	startErr        error
)

func TestMain(m *testing.M) {
	// Ryuk exits immediately under this Docker Desktop, which makes every
	// container start fail. The Kafka container is stopped below instead.
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	code := m.Run()
	if sharedContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sharedContainer.Terminate(ctx)
		cancel()
	}
	os.Exit(code)
}

func kafkaCluster(t *testing.T) *cluster {
	t.Helper()
	startOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		container, err := kafkatc.Run(ctx, "confluentinc/confluent-local:7.5.0", kafkatc.WithClusterID("reliable-orchestration"))
		if container != nil {
			sharedContainer = container
		}
		if err != nil {
			startErr = err
			return
		}
		brokers, err := container.Brokers(ctx)
		if err != nil {
			startErr = err
			return
		}
		records, err := codec.New("mock://integration")
		if err != nil {
			startErr = err
			return
		}
		shared = &cluster{brokers: strings.Join(brokers, ","), records: records}
	})
	if startErr != nil {
		if dockerDown(startErr) {
			t.Skipf("docker is not available: %v", startErr)
		}
		t.Fatal(startErr)
	}
	resetTopics(t, shared.brokers)
	return shared
}

func dockerDown(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "dockerDesktopLinuxEngine") ||
		strings.Contains(msg, "Cannot connect to the Docker daemon") ||
		strings.Contains(msg, "error during connect")
}

func TestLiveSuccess(t *testing.T) {
	cl := kafkaCluster(t)
	script := &agent.Script{Steps: 3}
	running := startConsumer(t, cl, "live-success", topics.Live, pipeline.Handler{
		Agent:       script,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	produce(t, cl, topics.Live, job.Envelope{ID: "email-1", Payload: []byte("ok"), Attempt: 1})
	waitCalls(t, script, 3)
	if err := running.stop(); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, cl, topics.Retry); len(got) != 0 {
		t.Fatalf("retry topic = %+v", got)
	}
	if got := collect(t, cl, topics.DLQ); len(got) != 0 {
		t.Fatalf("dead-letter topic = %+v", got)
	}
}

func TestRetryRestartsAtStepOne(t *testing.T) {
	cl := kafkaCluster(t)
	live := startConsumer(t, cl, "retry-live", topics.Live, pipeline.Handler{
		Agent: &agent.Script{
			Steps:    3,
			Failures: []agent.Failure{{Attempt: 1, Step: 2, Err: errors.New("down")}},
		},
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	produce(t, cl, topics.Live, job.Envelope{ID: "email-1", Payload: []byte("body"), Attempt: 1})
	waitTopic(t, cl, topics.Retry, 1)
	if err := live.stop(); err != nil {
		t.Fatal(err)
	}

	script := &agent.Script{Steps: 3}
	retry := startConsumer(t, cl, "retry-retry", topics.Retry, pipeline.Handler{
		Agent:       script,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	waitCalls(t, script, 3)
	if err := retry.stop(); err != nil {
		t.Fatal(err)
	}
	calls := script.Calls()
	for i, step := range []int{1, 2, 3} {
		if calls[i].Step != step || calls[i].Attempt != 2 {
			t.Fatalf("calls = %+v", calls)
		}
	}
	if got := collect(t, cl, topics.DLQ); len(got) != 0 {
		t.Fatalf("dead-letter topic = %+v", got)
	}
}

func TestLiveContinuesAfterFailure(t *testing.T) {
	cl := kafkaCluster(t)
	worker := &byID{fail: map[string]error{"bad": errors.New("down")}}
	running := startConsumer(t, cl, "continue", topics.Live, pipeline.Handler{
		Agent:       worker,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	produce(t, cl, topics.Live, job.Envelope{ID: "bad", Attempt: 1})
	produce(t, cl, topics.Live, job.Envelope{ID: "good", Attempt: 1})
	waitFor(t, func() bool { return len(worker.ids()) == 2 })
	if err := running.stop(); err != nil {
		t.Fatal(err)
	}
	if got := worker.ids(); len(got) != 2 || got[0] != "bad" || got[1] != "good" {
		t.Fatalf("order = %v", got)
	}
	retried := collect(t, cl, topics.Retry)
	if len(retried) != 1 || retried[0].ID != "bad" {
		t.Fatalf("retry topic = %+v", retried)
	}
}

func TestBudgetReachesDeadLetter(t *testing.T) {
	cl := kafkaCluster(t)
	failing := &agent.Script{
		Steps:    1,
		Failures: []agent.Failure{{Attempt: 1, Step: 1, Err: errors.New("down")}, {Attempt: 2, Step: 1, Err: errors.New("still down")}},
	}
	live := startConsumer(t, cl, "budget-live", topics.Live, pipeline.Handler{
		Agent:       failing,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	produce(t, cl, topics.Live, job.Envelope{ID: "email-1", Attempt: 1})
	waitTopic(t, cl, topics.Retry, 1)
	if err := live.stop(); err != nil {
		t.Fatal(err)
	}

	retry := startConsumer(t, cl, "budget-retry", topics.Retry, pipeline.Handler{
		Agent:       failing,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	waitTopic(t, cl, topics.DLQ, 1)
	if err := retry.stop(); err != nil {
		t.Fatal(err)
	}

	ok := &agent.Script{Steps: 1}
	again := startConsumer(t, cl, "budget-live-2", topics.Live, pipeline.Handler{
		Agent:       ok,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	produce(t, cl, topics.Live, job.Envelope{ID: "email-2", Attempt: 1})
	waitCalls(t, ok, 1)
	if err := again.stop(); err != nil {
		t.Fatal(err)
	}
	dead := collect(t, cl, topics.DLQ)
	if len(dead) != 1 || dead[0].ID != "email-1" || dead[0].Attempt != 2 {
		t.Fatalf("dead-letter topic = %+v", dead)
	}
}

func TestReplayGoesToRetryTopic(t *testing.T) {
	cl := kafkaCluster(t)
	produce(t, cl, topics.DLQ, job.Envelope{ID: "old-1", Payload: []byte("old"), Attempt: 2, Error: "down"})
	produce(t, cl, topics.DLQ, job.Envelope{ID: "old-2", Payload: []byte("older"), Attempt: 2, Error: "down"})

	liveScript := &agent.Script{Steps: 1}
	live := startConsumer(t, cl, "replay-live", topics.Live, pipeline.Handler{
		Agent:       liveScript,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	produce(t, cl, topics.Live, job.Envelope{ID: "new-mail", Attempt: 1})
	waitCalls(t, liveScript, 1)
	if err := live.stop(); err != nil {
		t.Fatal(err)
	}

	replayRun := startReplay(t, cl, "replay-group")
	waitTopic(t, cl, topics.Retry, 2)
	if err := replayRun.stop(); err != nil {
		t.Fatal(err)
	}

	liveRecords := collect(t, cl, topics.Live)
	if len(liveRecords) != 1 || liveRecords[0].ID != "new-mail" {
		t.Fatalf("live topic = %+v", liveRecords)
	}
	retried := collect(t, cl, topics.Retry)
	if len(retried) != 2 || retried[0].ID != "old-1" || retried[1].ID != "old-2" {
		t.Fatalf("retry topic = %+v", retried)
	}
}

func TestCrashBeforeCommitDuplicatesRetry(t *testing.T) {
	cl := kafkaCluster(t)
	cons, err := broker.NewConsumer(cl.brokers, "crash-commit", []string{topics.Live})
	if err != nil {
		t.Fatal(err)
	}
	reader := &failCommit{Consumer: cons}
	ctx, cancel := context.WithCancel(context.Background())
	publisher := newProducer(t, cl)
	errCh := make(chan error, 1)
	go func() {
		errCh <- consume.Run(ctx, reader, cl.records, pipeline.Handler{
			Agent: &agent.Script{
				Steps:    1,
				Failures: []agent.Failure{{Attempt: 1, Step: 1, Err: errors.New("down")}},
			},
			MaxAttempts: 2,
			Publisher:   publisher,
		})
	}()
	waitMembers(t, cl.brokers, "crash-commit", 1)
	produce(t, cl, topics.Live, job.Envelope{ID: "email-1", Attempt: 1})
	if err := <-errCh; err == nil || err.Error() != "commit failed" {
		cancel()
		cons.Close()
		t.Fatalf("error = %v", err)
	}
	cancel()
	cons.Close()
	if got := collect(t, cl, topics.Retry); len(got) != 1 || got[0].ID != "email-1" {
		t.Fatalf("retry topic after crash = %+v", got)
	}

	script := &agent.Script{
		Steps:    1,
		Failures: []agent.Failure{{Attempt: 1, Step: 1, Err: errors.New("down")}},
	}
	again := startConsumer(t, cl, "crash-commit", topics.Live, pipeline.Handler{
		Agent:       script,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	waitTopic(t, cl, topics.Retry, 2)
	if err := again.stop(); err != nil {
		t.Fatal(err)
	}
	retried := collect(t, cl, topics.Retry)
	if len(retried) != 2 || retried[0].ID != "email-1" || retried[1].ID != "email-1" {
		t.Fatalf("retry topic = %+v", retried)
	}
}

func TestCrashBeforePublishRedelivers(t *testing.T) {
	cl := kafkaCluster(t)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	cons, err := broker.NewConsumer(cl.brokers, "crash-publish", []string{topics.Live})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	publisher := newProducer(t, cl)
	go func() {
		errCh <- consume.Run(ctx, cons, cl.records, pipeline.Handler{
			Agent: runFunc(func(ctx context.Context, _ job.Envelope) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}),
			MaxAttempts: 2,
			Publisher:   publisher,
		})
	}()
	waitMembers(t, cl.brokers, "crash-publish", 1)
	produce(t, cl, topics.Live, job.Envelope{ID: "email-1", Payload: []byte("body"), Attempt: 1})
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		cancel()
		cons.Close()
		t.Fatal("agent did not start")
	}
	cancel()
	if err := <-errCh; err != nil {
		cons.Close()
		t.Fatal(err)
	}
	cons.Close()
	if got := collect(t, cl, topics.Retry); len(got) != 0 {
		t.Fatalf("retry topic = %+v", got)
	}

	script := &agent.Script{Steps: 3}
	again := startConsumer(t, cl, "crash-publish", topics.Live, pipeline.Handler{
		Agent:       script,
		MaxAttempts: 2,
		Publisher:   newProducer(t, cl),
	})
	waitCalls(t, script, 3)
	if err := again.stop(); err != nil {
		t.Fatal(err)
	}
	calls := script.Calls()
	if calls[0].Step != 1 || calls[0].Attempt != 1 || calls[0].JobID != "email-1" {
		t.Fatalf("calls = %+v", calls)
	}
}

type running struct {
	cancel context.CancelFunc
	errCh  <-chan error
	cons   *broker.Consumer
	once   sync.Once
	err    error
}

func (r *running) stop() error {
	r.once.Do(func() {
		r.cancel()
		r.err = <-r.errCh
		if cerr := r.cons.Close(); r.err == nil {
			r.err = cerr
		}
	})
	return r.err
}

func startConsumer(t *testing.T, cl *cluster, group, topic string, h pipeline.Handler) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cons, err := broker.NewConsumer(cl.brokers, group, []string{topic})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	ch := make(chan error, 1)
	go func() { ch <- consume.Run(ctx, cons, cl.records, h) }()
	run := &running{cancel: cancel, errCh: ch, cons: cons}
	t.Cleanup(func() { _ = run.stop() })
	waitMembers(t, cl.brokers, group, 1)
	return run
}

func startReplay(t *testing.T, cl *cluster, group string) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cons, err := broker.NewConsumer(cl.brokers, group, []string{topics.DLQ})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	ch := make(chan error, 1)
	publisher := newProducer(t, cl)
	go func() { ch <- replay.Run(ctx, cons, cl.records, publisher) }()
	run := &running{cancel: cancel, errCh: ch, cons: cons}
	t.Cleanup(func() { _ = run.stop() })
	waitMembers(t, cl.brokers, group, 1)
	return run
}

func newProducer(t *testing.T, cl *cluster) *broker.Producer {
	t.Helper()
	p, err := broker.NewProducer(cl.brokers, cl.records)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func produce(t *testing.T, cl *cluster, topic string, env job.Envelope) {
	t.Helper()
	p := newProducer(t, cl)
	if err := p.Produce(context.Background(), topic, env); err != nil {
		t.Fatal(err)
	}
}

func resetTopics(t *testing.T, brokers string) {
	t.Helper()
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	names := []string{topics.Live, topics.Retry, topics.DLQ}
	_, _ = admin.DeleteTopics(ctx, names)

	gone := time.Now().Add(20 * time.Second)
	for time.Now().Before(gone) && listedTopics(admin, names) != 0 {
		time.Sleep(200 * time.Millisecond)
	}
	if listedTopics(admin, names) != 0 {
		t.Fatal("topics were not deleted")
	}
	created := time.Now().Add(20 * time.Second)
	for time.Now().Before(created) {
		results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{
			{Topic: topics.Live, NumPartitions: 1, ReplicationFactor: 1},
			{Topic: topics.Retry, NumPartitions: 1, ReplicationFactor: 1},
			{Topic: topics.DLQ, NumPartitions: 1, ReplicationFactor: 1},
		})
		if err == nil && topicsReady(results) && listedTopics(admin, names) == len(names) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("topics were not recreated")
}

func topicsReady(results []kafka.TopicResult) bool {
	if len(results) != 3 {
		return false
	}
	for _, result := range results {
		code := result.Error.Code()
		if code != kafka.ErrNoError && code != kafka.ErrTopicAlreadyExists {
			return false
		}
	}
	return true
}

func listedTopics(admin *kafka.AdminClient, names []string) int {
	meta, err := admin.GetMetadata(nil, true, 5_000)
	if err != nil {
		return -1
	}
	n := 0
	for _, name := range names {
		info, ok := meta.Topics[name]
		if ok && info.Error.Code() == kafka.ErrNoError && len(info.Partitions) > 0 {
			n++
		}
	}
	return n
}

func waitMembers(t *testing.T, brokers, group string, n int) {
	t.Helper()
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := admin.DescribeConsumerGroups(ctx, []string{group})
		cancel()
		if err == nil && len(res.ConsumerGroupDescriptions) == 1 {
			desc := res.ConsumerGroupDescriptions[0]
			if desc.Error.Code() == kafka.ErrNoError && desc.State == kafka.ConsumerGroupStateStable && len(desc.Members) == n {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("group %s did not reach %d members", group, n)
}

func collect(t *testing.T, cl *cluster, topic string) []job.Envelope {
	t.Helper()
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cl.brokers,
		"group.id":           fmt.Sprintf("collect-%d", time.Now().UnixNano()),
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	if err := cons.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatal(err)
	}
	var out []job.Envelope
	assigned := false
	idle := 0
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := cons.ReadMessage(300 * time.Millisecond)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsTimeout() {
				if !assigned {
					parts, aerr := cons.Assignment()
					if aerr == nil && len(parts) > 0 {
						assigned = true
					}
					continue
				}
				idle++
				if idle >= 4 {
					return out
				}
				continue
			}
			t.Fatal(err)
		}
		assigned = true
		idle = 0
		env, err := cl.records.Unmarshal(topic, msg.Value)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, env)
	}
	t.Fatal("timed out reading " + topic)
	return nil
}

func waitTopic(t *testing.T, cl *cluster, topic string, n int) {
	t.Helper()
	waitFor(t, func() bool { return len(collect(t, cl, topic)) >= n })
}

func waitCalls(t *testing.T, script *agent.Script, n int) {
	t.Helper()
	waitFor(t, func() bool { return len(script.Calls()) >= n })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the broker flow")
}

type byID struct {
	mu    sync.Mutex
	order []string
	fail  map[string]error
}

func (b *byID) Run(_ context.Context, env job.Envelope) error {
	b.mu.Lock()
	b.order = append(b.order, env.ID)
	b.mu.Unlock()
	if err := b.fail[env.ID]; err != nil {
		return err
	}
	return nil
}

func (b *byID) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.order))
	copy(out, b.order)
	return out
}

type failCommit struct {
	*broker.Consumer
	failed bool
}

func (f *failCommit) Commit(ctx context.Context, msg consume.Message) error {
	if !f.failed {
		f.failed = true
		return errors.New("commit failed")
	}
	return f.Consumer.Commit(ctx, msg)
}

type runFunc func(context.Context, job.Envelope) error

func (f runFunc) Run(ctx context.Context, env job.Envelope) error {
	return f(ctx, env)
}
