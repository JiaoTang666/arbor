package pbft_all

import "blockEmulator/message"

// Validate the entire envelope before any request is executed. Subtract only
// after range checks, avoiding unsigned wraparound and end+1 overflow.
func validHistoryEnvelope(s *message.SendOldMessage) bool {
	if s == nil || s.SeqStartHeight > s.SeqEndHeight || s.SeqEndHeight == ^uint64(0) || len(s.OldRequest) == 0 {
		return false
	}
	if s.SeqEndHeight-s.SeqStartHeight != uint64(len(s.OldRequest)-1) {
		return false
	}
	for _, r := range s.OldRequest {
		if r == nil {
			return false
		}
	}
	return true
}
