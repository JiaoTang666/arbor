package committee

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

type livePlacementEndpoint struct {
	Sender    string `json:"sender"`
	Recipient string `json:"recipient"`
}

type livePlacementPlan struct {
	Routes          [][]uint64 `json:"routes"`
	EvidenceVersion uint64     `json:"evidence_version"`
	EvidenceDigest  string     `json:"evidence_digest"`
	EvidenceCutoff  uint64     `json:"evidence_cutoff"`
	RegistryVersion uint64     `json:"registry_version"`
	endpoints       []livePlacementEndpoint
	firstSequence   uint64
	cursor          int
}

type livePlacementTransportMetrics struct {
	TotalSeconds     float64 `json:"total_seconds"`
	Attempts         int     `json:"attempts"`
	HTTPSeconds      float64 `json:"http_seconds"`
	RetryWaitSeconds float64 `json:"retry_wait_seconds"`
}

// A retry recovers the same durable decision, never a new plan or a native
// dispatch. One deadline bounds all attempts, including response-body reads.
func requestLivePlacement(address string, raw []byte) (responseBody []byte, metrics livePlacementTransportMetrics, resultErr error) {
	totalStarted := time.Now()
	defer func() { metrics.TotalSeconds = time.Since(totalStarted).Seconds() }()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			started := time.Now()
			timer := time.NewTimer(time.Duration(attempt) * 50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				metrics.RetryWaitSeconds += time.Since(started).Seconds()
				return nil, metrics, fmt.Errorf("live placement request recovery deadline: %w", ctx.Err())
			case <-timer.C:
			}
			metrics.RetryWaitSeconds += time.Since(started).Seconds()
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(raw))
		if err != nil {
			return nil, metrics, err
		}
		request.Header.Set("Content-Type", "application/json")
		metrics.Attempts++
		started := time.Now()
		response, err := client.Do(request)
		if err != nil {
			metrics.HTTPSeconds += time.Since(started).Seconds()
			lastErr = err
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusBadGateway && response.StatusCode != http.StatusServiceUnavailable && response.StatusCode != http.StatusGatewayTimeout {
			response.Body.Close()
			metrics.HTTPSeconds += time.Since(started).Seconds()
			return nil, metrics, fmt.Errorf("live placement HTTP status %d", response.StatusCode)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		response.Body.Close()
		metrics.HTTPSeconds += time.Since(started).Seconds()
		if len(body) > 1<<20 {
			return nil, metrics, fmt.Errorf("live placement response oversized")
		}
		if readErr != nil {
			lastErr = fmt.Errorf("live placement response unavailable: %w", readErr)
			continue
		}
		if response.StatusCode == http.StatusOK {
			return body, metrics, nil
		}
		lastErr = fmt.Errorf("live placement HTTP status %d", response.StatusCode)
		if response.StatusCode != http.StatusBadGateway && response.StatusCode != http.StatusServiceUnavailable && response.StatusCode != http.StatusGatewayTimeout {
			return nil, metrics, lastErr
		}
	}
	return nil, metrics, fmt.Errorf("live placement same-request recovery exhausted: %w", lastErr)
}

// The sidecar owns no consensus state. It uses only the admitted input prefix,
// persists a quorum authorization, and then returns routes for this batch.
// Its local HTTP transport is a trusted OS boundary, not authenticated RPC.
func (m *RelayCommitteeModule) prepareLivePlacement(txs []*core.Transaction) error {
	address := os.Getenv("SAA_LIVE_PLAN_URL")
	if address == "" {
		return nil
	}
	if m.ownership == nil || m.initialLedger == nil || len(txs) == 0 || m.livePlan != nil {
		return fmt.Errorf("live placement requires secured explicit genesis and one batch")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "http" || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") || parsed.User != nil {
		return fmt.Errorf("live placement URL must be trusted loopback HTTP")
	}
	endpoints := make([]livePlacementEndpoint, len(txs))
	for i, tx := range txs {
		if tx == nil || tx.IngressSequence != uint64(m.nativeCount+i) {
			return fmt.Errorf("live placement input prefix mismatch")
		}
		endpoints[i] = livePlacementEndpoint{tx.Sender, tx.Recipient}
	}
	raw, err := json.Marshal(struct {
		FirstSequence uint64                  `json:"first_sequence"`
		Transactions  []livePlacementEndpoint `json:"transactions"`
	}{uint64(m.nativeCount), endpoints})
	if err != nil {
		return err
	}
	started := time.Now()
	responseBytes, transport, err := requestLivePlacement(address, raw)
	if err != nil {
		return err
	}
	decodeStarted := time.Now()
	var plan livePlacementPlan
	decoder := json.NewDecoder(bytes.NewReader(responseBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return err
	}
	if decoder.Decode(new(interface{})) != io.EOF {
		return fmt.Errorf("trailing live placement response")
	}
	digest, err := hex.DecodeString(plan.EvidenceDigest)
	if err != nil || len(digest) != 32 || plan.EvidenceVersion == 0 || plan.EvidenceCutoff > uint64(m.nativeCount) || len(plan.Routes) != len(txs) {
		return fmt.Errorf("invalid causal live placement evidence or batch length")
	}
	for _, route := range plan.Routes {
		if len(route) != 2 || route[0] >= uint64(params.ShardNum) || route[1] >= uint64(params.ShardNum) {
			return fmt.Errorf("invalid live placement shard pair")
		}
	}
	decodeSeconds := time.Since(decodeStarted).Seconds()
	refresh, err := m.ownership.RefreshMeasured()
	if err != nil {
		return err
	}
	headStarted := time.Now()
	registryVersion := m.ownership.Version()
	headSeconds := time.Since(headStarted).Seconds()
	if registryVersion != plan.RegistryVersion {
		return fmt.Errorf("live placement certificate head mismatch")
	}
	plan.endpoints, plan.firstSequence = endpoints, uint64(m.nativeCount)
	m.livePlan = &plan
	m.nativeAudit.write(map[string]interface{}{"event": "live_plan_adopted", "start": m.nativeCount, "count": len(txs),
		"evidence_version": plan.EvidenceVersion, "evidence_digest": plan.EvidenceDigest, "evidence_cutoff": plan.EvidenceCutoff,
		"registry_version": plan.RegistryVersion, "started_at": started.UTC(), "ended_at": time.Now().UTC(),
		"transport": transport, "decode_validate_seconds": decodeSeconds, "ownership_refresh": refresh,
		"ownership_head_check_seconds": headSeconds})
	return nil
}

func (m *RelayCommitteeModule) liveNativePlacement(sender, recipient string) (uint64, uint64) {
	plan := m.livePlan
	if plan.cursor >= len(plan.Routes) || plan.endpoints[plan.cursor] != (livePlacementEndpoint{sender, recipient}) {
		panic("live placement occurrence or endpoint mismatch")
	}
	a, b := nativeAdmissionAccount(sender), nativeAdmissionAccount(recipient)
	lookup := func(account string) (uint64, bool) {
		if shard, ok := m.initialLedger.Routes[account]; ok {
			return shard, true
		}
		shard, ok := m.nativeRoutes[account]
		return shard, ok
	}
	oldSender, knownSender := lookup(a)
	oldRecipient, knownRecipient := lookup(b)
	source, target := plan.Routes[plan.cursor][0], plan.Routes[plan.cursor][1]
	if (knownSender && oldSender != source) || (knownRecipient && oldRecipient != target) {
		panic("live placement attempted to change an existing owner")
	}
	mode := os.Getenv("SAA_LIVE_ADMISSION_MODE")
	if mode != "hash" {
		if mode != "" && mode != "full" {
			panic("invalid live admission mode")
		}
		if (!knownSender || !knownRecipient) && source != target {
			panic("live NO/NN placement must preserve colocation")
		}
	}
	m.checkAuthorizedPlacement(a, b, source, target)
	sequence := plan.firstSequence + uint64(plan.cursor)
	for _, account := range []string{a, b} {
		if err := m.ownership.CheckPlanDependency(account, sequence, plan.EvidenceVersion, plan.EvidenceDigest, plan.EvidenceCutoff); err != nil {
			panic(err)
		}
	}
	if !knownSender {
		m.nativeRoutes[a] = source
	}
	if !knownRecipient {
		m.nativeRoutes[b] = target
	}
	m.nativePlacementVersion = 0
	plan.cursor++
	return source, target
}
