package pbft_all

import (
	"blockEmulator/message"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Called by the real constructor after maps and commit flags are initialized.
// Restore only evidence actually persisted with this source preparation.
func (n *PbftConsensusNode) restorePreparedRequestProgress() error {
	p := n.CurChain.SourcePreparation()
	if p == nil {
		return nil
	}
	if len(p.RequestJSON) == 0 {
		return fmt.Errorf("prepared state lacks original PBFT request; automatic restart indexing refused")
	}
	var r message.Request
	if err := json.Unmarshal(p.RequestJSON, &r); err != nil {
		return err
	}
	c, err := decodeSourcePrepare(&r)
	if err != nil || c != p.Command || hex.EncodeToString(getDigest(&r)) != p.CommitDigest || p.PrepareSequence == 0 || p.PrepareSequence == ^uint64(0) || p.PrepareBlock > n.CurChain.CurrentBlock.Header.Number {
		return fmt.Errorf("invalid persisted preparation request identity")
	}
	n.recordHistoricalProgress(&r, p.PrepareSequence)
	return nil
}
