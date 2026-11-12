// Package broker wraps NATS JetStream. All retry/backoff/poison-message
// handling required by the brief is consumer configuration here, not
// hand-rolled dead-letter logic.
package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const StreamName = "TASKS"

type Broker struct {
	nc *nats.Conn
	JS jetstream.JetStream
}

func Connect(ctx context.Context, url string) (*Broker, error) {
	nc, err := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	b := &Broker{nc: nc, JS: js}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{"tasks.>"},
		Storage:  jetstream.FileStorage,
	}); err != nil {
		nc.Close()
		return nil, err
	}
	return b, nil
}

func (b *Broker) Close() { b.nc.Close() }

func (b *Broker) PublishJSON(ctx context.Context, subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = b.JS.Publish(ctx, subject, data)
	return err
}

// DurableConsumer creates (or reattaches to) a pull consumer with bounded
// retry: transient failures get backoff via Nak, and a message that keeps
// failing past MaxDeliver stops being redelivered instead of looping forever
// and blocking the subject (the brief's "poison messages must not kill the
// consumer"). ackWait should exceed the slowest realistic processing time
// for that subject, or JetStream will redeliver a message that's still
// legitimately being worked (harmless here since both sides are idempotent,
// but noisy).
func (b *Broker) DurableConsumer(ctx context.Context, name, filterSubject string, ackWait time.Duration) (jetstream.Consumer, error) {
	return b.JS.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:       name,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    8,
		BackOff: []time.Duration{
			1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second,
		},
	})
}
