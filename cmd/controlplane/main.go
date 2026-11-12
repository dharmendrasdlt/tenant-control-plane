// Command controlplane runs the HTTP API and the inbound worker-progress
// consumer in one process. It is deliberately one binary: the brief asks
// for "one deployable unit" for the control plane side, and splitting the
// HTTP server from the consumer would just add an extra process to
// coordinate for no behavioral benefit at this scope.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dharmendrasdlt/tenant-control-plane/internal/api"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/broker"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/messages"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/service"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/store"
	"github.com/dharmendrasdlt/tenant-control-plane/migrations"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/controlplane?sslmode=disable")
	natsURL := env("NATS_URL", "nats://localhost:4222")
	port := env("PORT", "8080")

	if err := runMigrations(ctx, dsn); err != nil {
		log.Error("migrations failed", "error", err)
		os.Exit(1)
	}

	st, err := store.Open(ctx, dsn)
	if err != nil {
		log.Error("store open failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	br, err := connectBrokerWithRetry(ctx, natsURL, log)
	if err != nil {
		log.Error("broker connect failed", "error", err)
		os.Exit(1)
	}
	defer br.Close()

	svc := service.New(st, br, log)

	cons, err := br.DurableConsumer(ctx, "controlplane-progress", messages.SubjectTaskProgress, 30*time.Second)
	if err != nil {
		log.Error("consumer setup failed", "error", err)
		os.Exit(1)
	}
	consumeCtx, err := cons.Consume(makeProgressHandler(svc, log))
	if err != nil {
		log.Error("consume start failed", "error", err)
		os.Exit(1)
	}
	defer consumeCtx.Stop()

	srv := &http.Server{Addr: ":" + port, Handler: api.NewRouter(svc), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("control plane listening", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// makeProgressHandler is the inbound write path: apply the CAS, then ack or
// nak. A malformed payload can never be processed successfully no matter
// how many times it's redelivered, so it's termed (poison message) instead
// of nak'd (which would just retry it into the same dead end).
func makeProgressHandler(svc *service.Service, log *slog.Logger) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		var upd messages.TaskProgress
		if err := json.Unmarshal(msg.Data(), &upd); err != nil {
			log.Error("poison message: undecodable progress update", "error", err)
			_ = msg.Term()
			return
		}
		applied, err := svc.ApplyWorkerUpdate(context.Background(), upd)
		if err != nil {
			log.Error("apply worker update failed; will retry", "task_id", upd.TaskID, "error", err)
			_ = msg.Nak()
			return
		}
		if !applied {
			log.Info("dropped duplicate/stale progress update", "task_id", upd.TaskID, "status", upd.Status, "update_id", upd.UpdateID)
		} else {
			log.Info("applied progress update", "task_id", upd.TaskID, "status", upd.Status, "update_id", upd.UpdateID)
		}
		_ = msg.Ack()
	}
}

func connectBrokerWithRetry(ctx context.Context, url string, log *slog.Logger) (*broker.Broker, error) {
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

func runMigrations(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	var lastErr error
	for i := 0; i < 20; i++ {
		if err := pool.Ping(ctx); err == nil {
			lastErr = nil
			break
		} else {
			lastErr = err
		}
		time.Sleep(1 * time.Second)
	}
	if lastErr != nil {
		return lastErr
	}
	_, err = pool.Exec(ctx, migrations.InitSQL)
	return err
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
