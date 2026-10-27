package core

// SourcePrepareCommand is issued by SEF for one accepted proposal. State/block
// expectations bind preparation to a committed source snapshot, not a timer.
type SourcePrepareCommand struct {
	// Explicit SEF intent: bind the exact committed snapshot under the source
	// proposal lock. The resulting PBFT request still contains all three hashes.
	BindAtProposal    bool   `json:"bind_at_proposal,omitempty"`
	ProposalID        string `json:"proposal_id"`
	Account           string `json:"account"`
	SourceShard       uint64 `json:"source_shard"`
	TargetShard       uint64 `json:"target_shard"`
	MappingVersion    uint64 `json:"mapping_version"`
	AnchorVersion     uint64 `json:"anchor_version"`
	ExpectedBlockHash string `json:"expected_block_hash"`
	ExpectedStateRoot string `json:"expected_state_root"`
	ExpectedStateHash string `json:"expected_state_hash"`
}

// Stored in the source trie by a consensus-ordered preparation request.
// The route map and the account's actual state remain unchanged.
type SourcePreparation struct {
	RequestJSON     []byte               `json:"request_json,omitempty"` // exact PBFT request for restart indexing
	Command         SourcePrepareCommand `json:"command"`
	State           []byte               `json:"state"`
	StateHash       string               `json:"state_hash"`
	SourceStateRoot string               `json:"source_state_root"`
	PrepareBlock    uint64               `json:"prepare_block"`
	PrepareSequence uint64               `json:"prepare_sequence"`
	CommitDigest    string               `json:"commit_digest"`
}
