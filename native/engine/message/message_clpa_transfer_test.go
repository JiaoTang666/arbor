package message

import (
	"encoding/json"
	"testing"
)

func TestAccountStateAndTxMigrationMetadataRoundTrip(t *testing.T) {
	in := AccountStateAndTx{FromShard: 1, MigrationEnvelopes: []StateTransferEnvelope{{ProposalID: "p", Account: "a", SourceShard: 1, TargetShard: 0, MappingVersion: 2, AnchorVersion: 3, SourceStateRoot: "root", StateHash: "hash"}}}
	b, e := json.Marshal(in)
	if e != nil {
		t.Fatal(e)
	}
	var out AccountStateAndTx
	if e = json.Unmarshal(b, &out); e != nil {
		t.Fatal(e)
	}
	if len(out.MigrationEnvelopes) != 1 || out.MigrationEnvelopes[0].ProposalID != "p" {
		t.Fatal("metadata lost")
	}
}

func TestAccountTransferMsgMigrationMetadataRoundTrip(t *testing.T) {
	in := AccountTransferMsg{ModifiedMap: map[string]uint64{"0xaa": 1}, Addrs: []string{"acct"}, MigrationEnvelopes: []StateTransferEnvelope{{ProposalID: "proposal-1", Account: "acct", SourceShard: 1, TargetShard: 0, StateHash: "0123456789abcdef"}}}
	out := DecodeAccountTransferMsg(in.Encode())
	if len(out.MigrationEnvelopes) != 1 || out.MigrationEnvelopes[0].ProposalID != "proposal-1" || out.MigrationEnvelopes[0].Account != "acct" || out.MigrationEnvelopes[0].StateHash != "0123456789abcdef" {
		t.Fatalf("migration metadata lost: %+v", out.MigrationEnvelopes)
	}
}
