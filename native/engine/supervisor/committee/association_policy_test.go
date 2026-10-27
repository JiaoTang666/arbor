package committee

import (
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestAssociationHashContractAndPrefix(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	addr := func(n int) string { return fmt.Sprintf("%040x", n) }
	// Equal prefixes, different suffixes and pressure: J must not read pressure.
	prefix := [][2]int{{1, 2}, {4, 1}, {9, 10}, {9, 10}, {11, 11}, {12, 9}, {9, 4}}
	var baseline [][2]uint64
	for variant := 0; variant < 2; variant++ {
		m, log := shareFixture(t)
		m.newNewPolicy.mode = "association_hash"
		// A nil prior would panic if J accidentally went through the feature scorer.
		m.newNewPolicy.prior = nil
		m.newNewWork.work[9] = int64(variant * 999999)
		seq := append(append([][2]int{}, prefix...), [2]int{100 + variant, 200 + variant})
		got := make([][2]uint64, 0, len(prefix))
		for i, pair := range seq {
			m.placementSequence = uint64(i)
			s, d := m.nativePlacement(addr(pair[0]), addr(pair[1]))
			m.placementSignals.assign(s, d)
			if i < len(prefix) {
				got = append(got, [2]uint64{s, d})
			}
		}
		expected := [][2]uint64{{3, 3}, {3, 3}, {9, 9}, {9, 9}, {11, 11}, {9, 9}, {9, 3}}
		for i, x := range expected {
			if got[i] != x {
				t.Fatalf("binding %d: got %v expected %v", i, got[i], x)
			}
		}
		if variant == 0 {
			baseline = got
		} else {
			for i := range baseline {
				if baseline[i] != got[i] {
					t.Fatal("prefix depends on suffix/pressure")
				}
			}
		}
		if m.newNewWork.population[11] != 1 {
			t.Fatal("self bound twice")
		}
		if m.newNewWork.population[9] != 3 {
			t.Fatal("repeat rebound or family follow lost")
		}
		if m.newNewPolicy.share.model != nil || m.newNewPolicy.share.batch != -1 {
			t.Fatal("J loaded model/scored batch")
		}
		m.nativeAudit.w.Flush()
		if bytes.Contains(log.Bytes(), []byte(`"event":"share_batch_decision"`)) {
			t.Fatal("J queried pressure")
		}
		for _, line := range bytes.Split(bytes.TrimSpace(log.Bytes()), []byte("\n")) {
			var r map[string]interface{}
			if err := json.Unmarshal(line, &r); err != nil {
				t.Fatal(err)
			}
			if r["event"] == "association_placement" && uint64(r["action"].(float64)) != uint64(utils.Addr2Shard(r["sender"].(string))) {
				t.Fatal("not the real sender hash")
			}
		}
	}
}
