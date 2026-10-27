package pbft_all

import (
	"blockEmulator/message"
	"encoding/hex"
	"time"
)

// Called only after a request's state application succeeds, under pbftLock.
func (n *PbftConsensusNode) recordHistoricalProgress(r *message.Request, height uint64) {
	digest := string(getDigest(r))
	if n.requestPool == nil {
		n.requestPool = make(map[string]*message.Request)
	}
	if n.height2Digest == nil {
		n.height2Digest = make(map[uint64]string)
	}
	if n.isReply == nil {
		n.isReply = make(map[string]bool)
	}
	n.requestPool[digest] = r
	n.height2Digest[height] = digest
	n.isReply[digest] = true
	if n.sequenceID <= height {
		n.sequenceID = height + 1
	}
	n.refreshHistoryProgress()
	n.hasCommittedBlock.Store(true)
	n.lastCommitTime.Store(time.Now().UnixMilli())
}

// Source preparation has a durable sequence/digest marker in the same trie
// transition as its state. Replay must match that marker, not only proposal ID.
func (n *PbftConsensusNode) validatePreparedHistoryIdentity(r *message.Request, height uint64) bool {
	if n.CurChain == nil {
		return false
	}
	p := n.CurChain.SourcePreparation()
	if p == nil {
		return true
	}
	c, err := decodeSourcePrepare(r)
	if err != nil {
		return false
	}
	return c == p.Command && height == p.PrepareSequence && hex.EncodeToString(getDigest(r)) == p.CommitDigest
}
