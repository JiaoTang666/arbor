package chain

import "fmt"

// This declaration binds the trusted source snapshot to its indexing regime;
// it is not a Byzantine certificate or a proof of remote execution history.
func (bc *BlockChain) validateGroupEffectCoverage(version uint64, bindRemote bool) error {
	enabled, err := NativeEffectIndexConfiguration()
	if err != nil {
		return err
	}
	want := uint64(0)
	if enabled {
		want = 1
	}
	if bindRemote && version != want {
		return fmt.Errorf("group transaction effect coverage mismatch")
	}
	if !enabled {
		return nil
	}
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	return RequireMigrationEffectIndex(st)
}
