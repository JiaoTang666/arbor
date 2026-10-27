package core

// One source shard, one target shard, one mapping generation. Members are
// canonical-address sorted; their exact order is part of consensus identity.
// A group is prepared atomically, never by committing members in a loop.
type GroupPrepareCommand struct {
	Members []SourcePrepareCommand `json:"members"`
}

type GroupPreparation struct {
	Command            GroupPrepareCommand         `json:"command"`
	Members            []SourcePreparation         `json:"members"`
	PrepareBlock       uint64                      `json:"prepare_block"`
	PrepareSequence    uint64                      `json:"prepare_sequence"`
	CommitDigest       string                      `json:"commit_digest"`
	RequestJSON        []byte                      `json:"request_json,omitempty"`
	EffectIndexVersion uint64                      `json:"effect_index_version,omitempty"`
	EffectRecords      []TransactionEffectSnapshot `json:"effect_records,omitempty"`
}

// Executed effects travel with account state, not with the current route.
// The trusted source exports them from the same frozen snapshot root.
type TransactionEffectSnapshot struct {
	Original []byte `json:"original"`
	Receipt  []byte `json:"receipt"`
	Binding  []byte `json:"binding"`
}
