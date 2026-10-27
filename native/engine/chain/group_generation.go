package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

var groupGenerationKey = []byte("__sef_source_generation_v1__")

// One active migration slot, distinct from retained routing/recovery history.
// Retired does NOT mean old-route traffic can no longer arrive. No fence,
// original snapshot, outbox or historical installation is removed here.
type SourceGeneration struct {
	Schema         string `json:"schema"`
	Generation     uint64 `json:"generation"`
	LatestProposal string `json:"latest_proposal"`
	ActiveProposal string `json:"active_proposal"`
	RetiredGroups  uint64 `json:"retired_groups"`
}

func readRetirementAt(st *trie.Trie, id string) (*GroupSourceRetirement, error) {
	raw, e := st.Get(groupRecordKey(groupRetirementKey, id))
	if e != nil {
		return nil, e
	}
	if len(raw) == 0 {
		raw, e = st.Get(groupRetirementKey)
		if e != nil {
			return nil, e
		}
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var r GroupSourceRetirement
	if e = json.Unmarshal(raw, &r); e != nil {
		return nil, e
	}
	actual, e := retirementProposalID(r.Command)
	if e != nil {
		return nil, e
	}
	if actual != id {
		return nil, nil
	} // legacy latest-record fallback belongs elsewhere
	digest, e := hex.DecodeString(r.Digest)
	if e != nil || len(digest) != 32 || r.Block == 0 || r.Sequence == 0 {
		return nil, fmt.Errorf("invalid archived retirement identity")
	}
	return &r, nil
}

// Derive the expected head from committed history. Missing metadata is legacy
// format, not evidence of an empty history; the next commit upgrades it atomically.
func sourceGenerationAt(st *trie.Trie, history []*core.GroupPreparation) (SourceGeneration, error) {
	expected := SourceGeneration{Schema: "source_generation_v1", Generation: uint64(len(history))}
	for i, p := range history {
		id := p.Command.Members[0].ProposalID
		r, e := readRetirementAt(st, id)
		if e != nil {
			return expected, e
		}
		if r != nil {
			expected.RetiredGroups++
		} else {
			if i != len(history)-1 {
				return expected, fmt.Errorf("unfinished historical generation")
			}
			expected.ActiveProposal = id
		}
		expected.LatestProposal = id
	}
	raw, e := st.Get(groupGenerationKey)
	if e != nil {
		return expected, e
	}
	if len(raw) == 0 {
		return expected, nil
	}
	var recorded SourceGeneration
	if e = json.Unmarshal(raw, &recorded); e != nil {
		return expected, e
	}
	if recorded != expected {
		return expected, fmt.Errorf("source generation/head differs from committed history")
	}
	return expected, nil
}

func (bc *BlockChain) SourceMigrationGeneration() (SourceGeneration, error) {
	st, e := bc.stateTrie()
	if e != nil {
		return SourceGeneration{}, e
	}
	return sourceGenerationAt(st, bc.GroupSourcePreparations())
}

func writeSourceGeneration(st *trie.Trie, g SourceGeneration) error {
	raw, e := json.Marshal(g)
	if e != nil {
		return e
	}
	return st.Update(groupGenerationKey, raw)
}

func prepareSourceGeneration(st *trie.Trie, history []*core.GroupPreparation, next string) error {
	g, e := sourceGenerationAt(st, history)
	if e != nil {
		return e
	}
	if g.ActiveProposal != "" {
		return fmt.Errorf("source migration slot still active")
	}
	if g.Generation == ^uint64(0) {
		return fmt.Errorf("source generation overflow")
	}
	g.Generation++
	g.LatestProposal = next
	g.ActiveProposal = next
	return writeSourceGeneration(st, g)
}

func retireSourceGeneration(st *trie.Trie, history []*core.GroupPreparation, id string) error {
	g, e := sourceGenerationAt(st, history)
	if e != nil {
		return e
	}
	if g.ActiveProposal != id || g.LatestProposal != id {
		return fmt.Errorf("retirement does not own active generation")
	}
	g.ActiveProposal = ""
	g.RetiredGroups++
	return writeSourceGeneration(st, g)
}
