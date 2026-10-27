package committee

import (
	"blockEmulator/core"
	"blockEmulator/ownership"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLivePlacementHTTPConsumesOneOrderedPlan(t *testing.T) {
	a, b := fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4)
	m := securedPlacementFixture(t, []ownership.Binding{{Account: a, Shard: 3}, {Account: b, Shard: 3}})
	m.nativeAudit = &batchAudit{w: bufio.NewWriter(io.Discard)}
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		atomic.AddInt32(&requests, 1)
		var body struct {
			FirstSequence uint64                  `json:"first_sequence"`
			Transactions  []livePlacementEndpoint `json:"transactions"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.FirstSequence != 0 || len(body.Transactions) != 2 || body.Transactions[0].Sender != a {
			t.Error("request did not bind the accepted input batch")
		}
		json.NewEncoder(writer).Encode(livePlacementPlan{Routes: [][]uint64{{3, 3}, {3, 3}}, EvidenceVersion: 1, EvidenceDigest: strings.Repeat("a", 64), RegistryVersion: 1})
	}))
	defer server.Close()
	t.Setenv("SAA_LIVE_PLAN_URL", server.URL+"/plan")
	t.Setenv("SAA_LIVE_ADMISSION_MODE", "full")
	txs := []*core.Transaction{{Sender: a, Recipient: b, IngressSequence: 0}, {Sender: b, Recipient: a, IngressSequence: 1}}
	if err := m.prepareLivePlacement(txs); err != nil {
		t.Fatal(err)
	}
	for _, tx := range txs {
		if source, target := m.nativePlacement(tx.Sender, tx.Recipient); source != 3 || target != 3 {
			t.Fatal("sidecar plan was overwritten by legacy NN placement")
		}
	}
	if atomic.LoadInt32(&requests) != 1 || m.livePlan.cursor != 2 || m.nativePlacementVersion != 0 || len(m.nativeRoutes) != 2 || m.newNewWork != nil {
		t.Fatal("plan did not preserve one batch, immutable routes and separate evidence version")
	}
}

func TestLivePlacementRejectsFutureEvidenceAndWrongCertificateHead(t *testing.T) {
	for _, invalid := range []string{"future", "wrong-head", "missing-route", "wrong-shard"} {
		t.Run(invalid, func(t *testing.T) {
			a, b := fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4)
			m := securedPlacementFixture(t, nil)
			plan := livePlacementPlan{Routes: [][]uint64{{3, 3}}, EvidenceVersion: 1, EvidenceDigest: strings.Repeat("a", 64)}
			switch invalid {
			case "future":
				plan.EvidenceCutoff = 1
			case "wrong-head":
				plan.RegistryVersion = 1
			case "missing-route":
				plan.Routes = nil
			case "wrong-shard":
				plan.Routes = [][]uint64{{4, 4}}
			}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { json.NewEncoder(writer).Encode(plan) }))
			defer server.Close()
			t.Setenv("SAA_LIVE_PLAN_URL", server.URL)
			if err := m.prepareLivePlacement([]*core.Transaction{{Sender: a, Recipient: b}}); err == nil || m.livePlan != nil || len(m.nativeRoutes) != 0 {
				t.Fatal("invalid plan mutated native placement")
			}
		})
	}
}

func TestLivePlacementRejectsExistingOwnerChangeBeforeMutation(t *testing.T) {
	a, b := fmt.Sprintf("%040x", 1), fmt.Sprintf("%040x", 3)
	m := securedPlacementFixture(t, []ownership.Binding{{Account: b, Shard: 3}})
	m.livePlan = &livePlacementPlan{Routes: [][]uint64{{3, 3}}, endpoints: []livePlacementEndpoint{{a, b}}}
	defer func() {
		if recover() == nil || len(m.nativeRoutes) != 0 {
			t.Fatal("existing owner modification was accepted or partially published")
		}
	}()
	m.nativePlacement(a, b)
}
