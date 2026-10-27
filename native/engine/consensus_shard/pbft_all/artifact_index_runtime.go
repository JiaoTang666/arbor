package pbft_all

import "blockEmulator/core"

// One shared index per node/directory/kind, not per historical migration group.
// Restart rebuilds it from durable files. It never supplies consensus evidence.
func (n *PbftConsensusNode) indexedArtifacts(dir, kind string) (core.MigrationArtifacts, error) {
	key := "artifact-index-v1:" + kind + ":" + dir
	v, ok := n.sourcePrepareSeen.Load(key)
	if !ok {
		v, _ = n.sourcePrepareSeen.LoadOrStore(key, core.NewMigrationArtifactIndex(dir, kind, 32<<20))
	}
	records, _, err := v.(*core.MigrationArtifactIndex).Load()
	return records, err
}
