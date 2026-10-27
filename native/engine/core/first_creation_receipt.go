package core

import "time"

// Trusted node commit report, not a cryptographic consensus certificate.
type FirstCreationReceipt struct {
	Binding                                          FirstPlacementBinding
	Account                                          string
	Shard, BlockHeight                               uint64
	BlockHash, ParentStateRoot, StateRoot, StateHash string
	ObservedAt                                       time.Time
}
