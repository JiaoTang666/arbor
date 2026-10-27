package committee

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"strings"
	"testing"
)

func TestStreamClaimsCausalRule(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = old })
	a, b, u, v := strings.Repeat("0", 38)+"10", strings.Repeat("0", 38)+"20", strings.Repeat("0", 38)+"30", strings.Repeat("0", 38)+"41"
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{a: 1, b: 0}}, admission: &RuntimeUnknownAdmissionManager{shardNum: 2, runtimeShardByAccount: map[string]uint64{}}}
	for _, c := range []struct {
		s, r      string
		n, target int
	}{{a, b, 0, 0}, {a, u, 1, 1}, {u, b, 1, 0}, {u, v, 2, 0}} {
		claims := m.streamClaims(&core.Transaction{Sender: c.s, Recipient: c.r})
		if len(claims) != c.n {
			t.Fatal(claims)
		}
		for _, q := range claims {
			if q.TargetShard != c.target {
				t.Fatal(q)
			}
		}
	}
	m.admission.runtimeShardByAccount[normalizeAdmissionAddress(u)] = 1
	if q := m.streamClaims(&core.Transaction{Sender: u, Recipient: v}); len(q) != 1 || q[0].TargetShard != 1 {
		t.Fatal(q)
	}
}
