package core

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// A trusted-supervisor binding carried unchanged through RawRelay. The token
// is a correlation nonce, NOT a signature or Byzantine authentication proof.
type FirstPlacementBinding struct {
	Session, RequestID, Token, AdmissionID, TxHash       string
	Generation, MappingVersion, SourceShard, TargetShard uint64
	Claims                                               map[string]uint64
}

func (b *FirstPlacementBinding) Validate(tx *Transaction, session string) error {
	if b == nil || tx == nil {
		return fmt.Errorf("missing_creator_binding")
	}
	token, e := hex.DecodeString(b.Token)
	if e != nil || len(token) != 32 || b.Session != session || session == "" || b.RequestID == "" || b.Generation == 0 || len(b.Claims) == 0 || !strings.HasPrefix(b.AdmissionID, session+"/") {
		return fmt.Errorf("invalid_creator_binding")
	}
	if tx.AdmissionID != b.AdmissionID || hex.EncodeToString(tx.TxHash) != b.TxHash || !tx.RoutingFrozen || tx.HasBroker || tx.MappingVersion != b.MappingVersion || tx.RoutedSenderShard != b.SourceShard || tx.RoutedRecipientShard != b.TargetShard {
		return fmt.Errorf("creator_transport_mismatch")
	}
	for a, s := range b.Claims {
		raw, e := hex.DecodeString(a)
		if e != nil || len(raw) != 20 || strings.ToLower(a) != a {
			return fmt.Errorf("invalid_creator_account")
		}
		if !(strings.TrimPrefix(strings.ToLower(tx.Sender), "0x") == a && b.SourceShard == s) && !(strings.TrimPrefix(strings.ToLower(tx.Recipient), "0x") == a && b.TargetShard == s) {
			return fmt.Errorf("creator_claim_mismatch")
		}
	}
	return nil
}
