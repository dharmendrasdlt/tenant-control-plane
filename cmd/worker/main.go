// Command worker is the provisioning worker simulator. It consumes
// tasks.created, "does the work" for a configurable random delay, and
// publishes in_progress then done|failed back onto tasks.progress.
// Timing and outcome are controlled by flags/env vars so failure and delay
// scenarios can be triggered just by restarting this process differently —
// no code changes needed to demo a negative path.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dharmendrasdlt/tenant-control-plane/internal/broker"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/messages"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	var (
		failRate   = flag.Float64("fail-rate", envFloat("FAIL_RATE", 0.0), "probability (0-1) that a task is reported as failed")
		minDelayMs = flag.Int("min-delay-ms", envInt("MIN_DELAY_MS", 200), "minimum simulated provisioning delay")
		maxDelayMs = flag.Int("max-delay-ms", envInt("MAX_DELAY_MS", 1500), "maximum simulated provisioning delay")
		natsURL    = flag.String("nats-url", env("NATS_URL", "nats://localhost:4222"), "NATS server URL")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	br, err := connectWithRetry(ctx, *natsURL, log)
	if err != nil {
		log.Error("broker connect failed", "error", err)
		os.Exit(1)
	}
	defer br.Close()

	cons, err := br.DurableConsumer(ctx, "worker-tasks", messages.SubjectTaskCreated, 5*time.Minute)
	if err != nil {
		log.Error("consumer setup failed", "error", err)
		os.Exit(1)
	}

	log.Info("worker started", "fail_rate", *failRate, "min_delay_ms", *minDelayMs, "max_delay_ms", *maxDelayMs)

	consumeCtx, err := cons.Consume(makeTaskHandler(ctx, br, log, *failRate, *minDelayMs, *maxDelayMs))
	if err != nil {
		log.Error("consume start failed", "error", err)
		os.Exit(1)
	}
	defer consumeCtx.Stop()

	<-ctx.Done()
	log.Info("worker shutting down")
}

func makeTaskHandler(ctx context.Context, br *broker.Broker, log *slog.Logger, failRate float64, minDelayMs, maxDelayMs int) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		var task messages.TaskCreated
		if err := json.Unmarshal(msg.Data(), &task); err != nil {
			log.Error("poison message: undecodable task", "error", err)
			_ = msg.Term()
			return
		}
		publishProgress(ctx, br, log, task.ID, "in_progress")

		delay := randDelay(minDelayMs, maxDelayMs)
		time.Sleep(delay)

		outcome := "done"
		if rand.Float64() < failRate { //nolint:gosec // simulated outcome, not a security decision; crypto/rand would be nonsensical here
			outcome = "failed"
		}
		publishProgress(ctx, br, log, task.ID, outcome)

		// Ack only after the terminal outcome is published. If this worker
		// crashes mid-flight, JetStream redelivers tasks.created and another
		// (or the restarted) worker re-simulates it; that's safe because the
		// control plane applies progress via compare-and-swap, so re-running
		// in_progress/done on an already-terminal task is a no-op, not a bug.
		_ = msg.Ack()
	}
}

func publishProgress(ctx context.Context, br *broker.Broker, log *slog.Logger, taskID, status string) {
	upd := messages.TaskProgress{
		UpdateID:   uuid.NewString(),
		TaskID:     taskID,
		Status:     status,
		OccurredAt: time.Now().UTC(),
	}
	if err := br.PublishJSON(ctx, messages.SubjectTaskProgress, upd); err != nil {
		log.Error("publish progress failed", "task_id", taskID, "status", status, "error", err)
		return
	}
	log.Info("published progress", "task_id", taskID, "status", status, "update_id", upd.UpdateID)
}

func randDelay(minMs, maxMs int) time.Duration {
	if maxMs <= minMs {
		return time.Duration(minMs) * time.Millisecond
	}
	return time.Duration(minMs+rand.IntN(maxMs-minMs)) * time.Millisecond //nolint:gosec // simulated jitter, not a security decision
}

func connectWithRetry(ctx context.Context, url string, log *slog.Logger) (*broker.Broker, error) {
	var lastErr error
	for i := 0; i < 20; i++ {
		br, err := broker.Connect(ctx, url)
		if err == nil {
			return br, nil
		}
		lastErr = err
		log.Warn("broker not ready, retrying", "attempt", i+1, "error", err)
		select {
		case <-time.After(1 * time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
