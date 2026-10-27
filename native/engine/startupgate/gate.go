// Package startupgate gates workload start, not first-placement or consensus.
// The controller and local endpoints are trusted. TCP readiness is not a PBFT proof.
package startupgate

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

type Binding struct {
	Session    string `json:"session"`
	LedgerHash string `json:"ledger_sha256"`
	ConfigHash string `json:"config_sha256"`
	Shards     uint64 `json:"shards"`
	Nodes      uint64 `json:"nodes_per_shard"`
}
type Release struct {
	Binding   Binding   `json:"binding"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func writeNew(path string, v interface{}) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if e = json.NewEncoder(f).Encode(v); e != nil {
		f.Close()
		return e
	}
	return f.Close()
}
func ValidateRelease(raw []byte, b Binding, ready, now time.Time) error {
	var r Release
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(&r); e != nil {
		return e
	}
	var extra interface{}
	if d.Decode(&extra) != io.EOF {
		return fmt.Errorf("trailing release data")
	}
	if r.Binding != b || r.IssuedAt.Before(ready) || r.IssuedAt.After(now) || !r.ExpiresAt.After(now) || r.ExpiresAt.Sub(r.IssuedAt) > 30*time.Second {
		return fmt.Errorf("stale or mismatched startup release")
	}
	return nil
}
func Probe(ctx context.Context, endpoints []string) error {
	for _, address := range endpoints {
		c, e := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
		if e != nil {
			return e
		}
		c.Close()
	}
	return nil
}

// Wait requires a new per-run directory. It never creates release.json.
// Publish release.json with atomic rename; malformed tickets fail closed.
func Wait(ctx context.Context, dir string, b Binding, endpoints []string, probe func(context.Context, []string) error) error {
	if b.Shards == 0 || b.Nodes == 0 || uint64(len(endpoints)) != b.Shards*b.Nodes {
		return fmt.Errorf("startup topology incomplete")
	}
	seen := map[string]bool{}
	for _, a := range endpoints {
		if a == "" || seen[a] {
			return fmt.Errorf("startup endpoint duplicate")
		}
		seen[a] = true
	}
	if e := os.Mkdir(dir, 0700); e != nil {
		return fmt.Errorf("fresh startup gate directory required: %w", e)
	}
	token := make([]byte, 32)
	if _, e := rand.Read(token); e != nil {
		return e
	}
	b.Session = hex.EncodeToString(token)
	if e := writeNew(filepath.Join(dir, "challenge.json"), b); e != nil {
		return e
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		if probe(ctx, endpoints) == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	ready := time.Now()
	if e := writeNew(filepath.Join(dir, "ready.json"), map[string]interface{}{"binding": b, "ready_at": ready, "endpoints": endpoints, "evidence": "TCP listening only, not consensus or state-root agreement"}); e != nil {
		return e
	}
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		raw, e := os.ReadFile(filepath.Join(dir, "release.json"))
		if e == nil {
			if e = ValidateRelease(raw, b, ready, time.Now()); e != nil {
				return e
			}
			if e = probe(ctx, endpoints); e != nil {
				return fmt.Errorf("readiness lost before release: %w", e)
			}
			if e = ctx.Err(); e != nil {
				return e
			}
			if e = ValidateRelease(raw, b, ready, time.Now()); e != nil {
				return e
			}
			return writeNew(filepath.Join(dir, "released.json"), map[string]interface{}{"binding": b, "released_at": time.Now()})
		}
		if !os.IsNotExist(e) {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
