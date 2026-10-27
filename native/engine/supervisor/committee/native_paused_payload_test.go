package committee

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Only for the stopped-node native harness. Read-only database locks reject
// running nodes. Never relabel old observations with the current round, or
// use this adapter as an online telemetry source.
func supplementPausedPayloads(root string, snapshot map[string]interface{}) error {
	routes, ok := snapshot["routes"].(map[string]uint64)
	if !ok {
		return fmt.Errorf("paused payload refresh requires typed route snapshot")
	}
	sizes := map[string]uint64{}
	observations := map[string]forecastStatePayloadObservation{}
	blockHashes := map[string]string{}
	missing := map[string]string{}
	// Remove stale values before attempting refresh; a failed read must not
	// leave a previously cached cost usable by the caller.
	snapshot["state_bytes"] = sizes
	snapshot["account_state_payload_evidence"] = observations
	snapshot["state_bytes_missing_accounts"] = missing
	snapshot["state_bytes_block_hashes"] = blockHashes
	snapshot["state_bytes_observation_kind"] = "stopped_node_committed_head_read_not_online_telemetry"
	snapshot["state_bytes_scope"] = "committed_trie_payload_only_excludes_protocol_queue_retries"
	for account, shard := range routes {
		if normalizeMappingAuthorityAccount(account) != account || shard >= 2 {
			return fmt.Errorf("invalid two-shard paused route")
		}
		head, _, raw, err := readSAAAccount(root, shard, strings.TrimPrefix(account, "0x"))
		if err != nil {
			if err.Error() == "account absent" {
				missing[account] = "account_absent_at_committed_source_root"
				continue
			}
			return fmt.Errorf("paused payload read %s shard %d: %w", account, shard, err)
		}
		sizes[account] = uint64(len(raw))
		observations[account] = forecastStatePayloadObservation{Bytes: uint64(len(raw)), Shard: shard, Epoch: int(head.Header.Number), StateRoot: hex.EncodeToString(head.Header.StateRoot)}
		blockHashes[account] = hex.EncodeToString(head.Hash)
	}
	return nil
}

func TestPausedPayloadReadExistingNativeEvidence(t *testing.T) {
	root := os.Getenv("SAA_PAUSED_PAYLOAD_REPLAY_DIR")
	if root == "" {
		t.Skip("explicit stopped native database replay only")
	}
	a := "0x0000000000000000000000000000000000000010"
	absent := "0xfffffffffffffffffffffffffffffffffffffffe"
	snapshot := map[string]interface{}{"routes": map[string]uint64{a: 0, absent: 0}, "state_bytes": map[string]uint64{a: 999999, absent: 999999}}
	if err := supplementPausedPayloads(root, snapshot); err != nil {
		t.Fatal(err)
	}
	head, _, raw, err := readSAAAccount(root, 0, strings.TrimPrefix(a, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	sizes := snapshot["state_bytes"].(map[string]uint64)
	sample := snapshot["account_state_payload_evidence"].(map[string]forecastStatePayloadObservation)[a]
	if sizes[a] != uint64(len(raw)) || sizes[a] == 0 || sample.Epoch != int(head.Header.Number) || sample.StateRoot != hex.EncodeToString(head.Header.StateRoot) {
		t.Fatal("payload is not bound to persisted head")
	}
	if _, exists := sizes[absent]; exists {
		t.Fatal("absent account retained fabricated cached size")
	}
	if snapshot["state_bytes_missing_accounts"].(map[string]string)[absent] == "" {
		t.Fatal("missing account not explained")
	}
	encoded, _ := json.Marshal(snapshot)
	t.Logf("READ ONLY NATIVE PAYLOAD PASS %s", encoded)
}
