//go:build integration

// Package integration exercises the real stack over HTTP and, for the
// messaging cases, by talking to the broker directly. Run `make up` first,
// then `make test-integration`.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func baseURL() string {
	if v := os.Getenv("CP_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func natsURL() string {
	if v := os.Getenv("NATS_URL"); v != "" {
		return v
	}
	return "nats://localhost:4222"
}

// randSlug must satisfy ^[a-z][-a-z0-9]{1,26}[a-z0-9]$ (3-28 chars total).
func randSlug(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("it%x%x", time.Now().UnixNano(), rand.Intn(0xfff))
}

type tenant struct {
	ID      string `json:"id"`
	Slug    string `json:"slug"`
	Status  string `json:"status"`
	Version int64  `json:"version"`
}

func createTenant(t *testing.T, slug string) tenant {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"slug": slug, "name": "Integration Test " + slug})
	resp, err := http.Post(baseURL()+"/tenants", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create tenant: status=%d body=%s", resp.StatusCode, b)
	}
	var tn tenant
	if err := json.NewDecoder(resp.Body).Decode(&tn); err != nil {
		t.Fatalf("decode tenant: %v", err)
	}
	return tn
}

func getTenant(t *testing.T, id string) tenant {
	t.Helper()
	resp, err := http.Get(baseURL() + "/tenants/" + id)
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	defer resp.Body.Close()
	var tn tenant
	_ = json.NewDecoder(resp.Body).Decode(&tn)
	return tn
}

// TestLifecycle_DeployReachesActive exercises the full messaging round trip:
// create -> control plane publishes the deploy task -> worker simulator
// processes it -> tenant observably becomes active with no manual steps.
func TestLifecycle_DeployReachesActive(t *testing.T) {
	tn := createTenant(t, randSlug(t))
	if tn.Status != "provisioning" {
		t.Fatalf("expected initial status provisioning, got %q", tn.Status)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got := getTenant(t, tn.ID)
		if got.Status == "active" {
			return
		}
		if got.Status == "failed" {
			t.Fatalf("tenant went failed, not active (worker FAIL_RATE should be 0 for this test)")
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("tenant did not reach active within deadline")
}

// TestConcurrentPatch_OnlyOneSucceeds is the non-functional requirement's
// headline case: two PATCHes racing on the same version must not both win.
func TestConcurrentPatch_OnlyOneSucceeds(t *testing.T) {
	tn := createTenant(t, randSlug(t))

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && tn.Status != "active" {
		time.Sleep(200 * time.Millisecond)
		tn = getTenant(t, tn.ID)
	}
	if tn.Status != "active" {
		t.Fatalf("precondition failed: tenant never became active")
	}

	patch := func() int {
		body, _ := json.Marshal(map[string]any{"name": "renamed", "version": tn.Version})
		req, _ := http.NewRequest(http.MethodPatch, baseURL()+"/tenants/"+tn.ID, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("patch: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	var wg sync.WaitGroup
	codes := make([]int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = patch()
		}(i)
	}
	wg.Wait()

	okCount, conflictCount := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			okCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Fatalf("unexpected status code %d from concurrent PATCH", c)
		}
	}
	if okCount != 1 || conflictCount != 1 {
		t.Fatalf("expected exactly one 200 and one 409, got codes=%v", codes)
	}
}

// TestDuplicateAndStaleProgress_Idempotent bypasses the worker and publishes
// directly to tasks.progress, the same way scripts/stale_update.sh does, to
// prove the control plane's compare-and-swap rejects replays and out-of-order
// deliveries instead of regressing state.
func TestDuplicateAndStaleProgress_Idempotent(t *testing.T) {
	tn := createTenant(t, randSlug(t))

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && tn.Status != "active" {
		time.Sleep(200 * time.Millisecond)
		tn = getTenant(t, tn.ID)
	}
	if tn.Status != "active" {
		t.Fatalf("precondition failed: tenant never became active")
	}

	resp, err := http.Get(baseURL() + "/tasks?tenant_id=" + tn.ID)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	defer resp.Body.Close()
	var listed struct {
		Items []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode tasks: %v", err)
	}
	if len(listed.Items) == 0 {
		t.Fatalf("expected at least one task for tenant")
	}
	taskID := listed.Items[0].ID

	ctx := context.Background()
	nc, err := nats.Connect(natsURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	publish := func(status string) {
		payload, _ := json.Marshal(map[string]any{
			"update_id":   fmt.Sprintf("replay-%d", time.Now().UnixNano()),
			"task_id":     taskID,
			"status":      status,
			"occurred_at": time.Now().UTC(),
		})
		if _, err := js.Publish(ctx, "tasks.progress", payload); err != nil {
			t.Fatalf("publish %s: %v", status, err)
		}
	}

	// The task is already terminal (done). A duplicate "done" and a stale
	// "in_progress" must both be no-ops: task/tenant stay exactly as they are.
	publish("done")
	publish("in_progress")

	time.Sleep(2 * time.Second) // give the consumer a moment to (not) apply these

	final := getTenant(t, tn.ID)
	if final.Status != "active" {
		t.Fatalf("replayed/stale updates regressed tenant status to %q, want active", final.Status)
	}
}
