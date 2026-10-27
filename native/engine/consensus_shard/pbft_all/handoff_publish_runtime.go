package pbft_all

import "blockEmulator/core"

func (n *PbftConsensusNode) publishSourceHandoff(dir string, h *core.MigrationHandoff) error {
	key := "handoff-durable-publisher-v1:" + dir
	value, ok := n.sourcePrepareSeen.Load(key)
	if !ok {
		value, _ = n.sourcePrepareSeen.LoadOrStore(key, core.NewHandoffPublisher(dir, 4096))
	}
	return value.(*core.HandoffPublisher).Publish(h)
}
