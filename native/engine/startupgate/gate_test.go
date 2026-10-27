package startupgate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReleaseContract(t *testing.T) {
	b := Binding{Session: "new", LedgerHash: "ledger", ConfigHash: "config", Shards: 2, Nodes: 4}
	now := time.Now()
	ready := now.Add(-time.Second)
	valid := Release{Binding: b, IssuedAt: now, ExpiresAt: now.Add(time.Second)}
	for _, mode := range []string{"valid", "old_session", "old_config", "premature", "expired"} {
		t.Run(mode, func(t *testing.T) {
			r := valid
			switch mode {
			case "old_session":
				r.Binding.Session = "old"
			case "old_config":
				r.Binding.ConfigHash = "old"
			case "premature":
				r.IssuedAt = ready.Add(-time.Second)
			case "expired":
				r.ExpiresAt = now
			}
			raw, _ := json.Marshal(r)
			e := ValidateRelease(raw, b, ready, now)
			if (e == nil) != (mode == "valid") {
				t.Fatal(e)
			}
		})
	}
}
func TestWaitNoAutomaticRelease(t *testing.T) {
	for _, nodesReady := range []bool{false, true} {
		t.Run(fmt.Sprint(nodesReady), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "gate")
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			probe := func(context.Context, []string) error {
				if nodesReady {
					return nil
				}
				return fmt.Errorf("not ready")
			}
			if e := Wait(ctx, dir, Binding{Shards: 1, Nodes: 1}, []string{"one"}, probe); e != context.DeadlineExceeded {
				t.Fatal(e)
			}
			if _, e := os.Stat(filepath.Join(dir, "released.json")); !os.IsNotExist(e) {
				t.Fatal("automatically released")
			}
		})
	}
}
func TestWaitExplicitRelease(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "gate")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			count := 0
			probe := func(context.Context, []string) error {
				count++
				if lost && count > 1 {
					return fmt.Errorf("node disappeared")
				}
				return nil
			}
			go func() { done <- Wait(ctx, dir, Binding{Shards: 1, Nodes: 1}, []string{"one"}, probe) }()
			var ready struct {
				Binding Binding   `json:"binding"`
				Ready   time.Time `json:"ready_at"`
			}
			for {
				raw, e := os.ReadFile(filepath.Join(dir, "ready.json"))
				if e == nil && json.Unmarshal(raw, &ready) == nil {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("no ready")
				case <-time.After(time.Millisecond):
				}
			}
			raw, _ := json.Marshal(Release{Binding: ready.Binding, IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Second)})
			tmp := filepath.Join(dir, "release.tmp")
			if e := os.WriteFile(tmp, raw, 0600); e != nil {
				t.Fatal(e)
			}
			if e := os.Rename(tmp, filepath.Join(dir, "release.json")); e != nil {
				t.Fatal(e)
			}
			if e := <-done; (e == nil) == lost {
				t.Fatal("wrong release result", e)
			}
		})
	}
}
