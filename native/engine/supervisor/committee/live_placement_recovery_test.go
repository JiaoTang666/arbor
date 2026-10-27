package committee

import (
	"blockEmulator/core"
	"blockEmulator/ownership"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestLivePlacementLostResponseRecoversIdenticalRequest(t *testing.T) {
	a, b := fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4)
	m := securedPlacementFixture(t, []ownership.Binding{{Account: a, Shard: 3}, {Account: b, Shard: 3}})
	var audit bytes.Buffer
	m.nativeAudit = &batchAudit{w: bufio.NewWriter(&audit)}
	var mu sync.Mutex
	var original []byte
	attempts, decisions := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		attempts++
		if attempts == 1 {
			original = append([]byte(nil), raw...)
			decisions++ // The durable decision exists, but its HTTP reply is lost.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		if !bytes.Equal(original, raw) {
			t.Error("retry changed request identity or endpoints")
		}
		json.NewEncoder(w).Encode(livePlacementPlan{Routes: [][]uint64{{3, 3}}, EvidenceVersion: 1, EvidenceDigest: strings.Repeat("a", 64), RegistryVersion: 1})
	}))
	defer server.Close()
	t.Setenv("SAA_LIVE_PLAN_URL", server.URL)
	if err := m.prepareLivePlacement([]*core.Transaction{{Sender: a, Recipient: b}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if attempts != 2 || decisions != 1 {
		t.Errorf("attempts=%d decisions=%d", attempts, decisions)
	}
	mu.Unlock()
	if m.nativeCount != 0 || len(m.nativeRoutes) != 0 || m.livePlan.cursor != 0 {
		t.Fatal("recovery dispatched or consumed a transaction")
	}
	m.nativeAudit.w.Flush()
	var event struct {
		Transport livePlacementTransportMetrics `json:"transport"`
		Refresh   map[string]interface{}        `json:"ownership_refresh"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(audit.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	if event.Transport.Attempts != 2 || event.Transport.HTTPSeconds <= 0 || event.Transport.RetryWaitSeconds <= 0 || event.Refresh == nil {
		t.Fatal("exclusive phase audit missing")
	}
}

func TestLivePlacementRecoveryIsBoundedAndRejectsDeterministicErrors(t *testing.T) {
	for _, status := range []int{400, 409, 503, 502, 504} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var mu sync.Mutex
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				attempts++
				mu.Unlock()
				w.Header().Set("Content-Length", "10")
				w.WriteHeader(status)
				io.WriteString(w, "x") // Truncated body cannot make a 400/409 retryable.
			}))
			defer server.Close()
			_, metrics, err := requestLivePlacement(server.URL, []byte(`{"first_sequence":0,"transactions":[]}`))
			want := 1
			if status >= 500 {
				want = 3
			}
			mu.Lock()
			got := attempts
			mu.Unlock()
			if err == nil || got != want || metrics.Attempts != want {
				t.Fatalf("got attempts=%d metrics=%+v error=%v", got, metrics, err)
			}
		})
	}
}

func TestLivePlacementMalformedSuccessDoesNotRetryOrPublish(t *testing.T) {
	m := securedPlacementFixture(t, nil)
	var mu sync.Mutex
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		mu.Unlock()
		io.WriteString(w, `{"routes":`)
	}))
	defer server.Close()
	t.Setenv("SAA_LIVE_PLAN_URL", server.URL)
	if err := m.prepareLivePlacement([]*core.Transaction{{Sender: fmt.Sprintf("%040x", 3), Recipient: fmt.Sprintf("%040x", 4)}}); err == nil {
		t.Fatal("malformed successful reply accepted")
	}
	mu.Lock()
	got := attempts
	mu.Unlock()
	if got != 1 || m.livePlan != nil || len(m.nativeRoutes) != 0 {
		t.Fatal("invalid success retried or mutated route state")
	}
}
