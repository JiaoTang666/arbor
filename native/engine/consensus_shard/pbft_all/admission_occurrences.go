package pbft_all

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// Only enabled with the exclusive fresh-run admission fence. Errors stop the
// run; silently dropping unknown work could falsely satisfy drain.
func (p *PbftConsensusNode) filterAdmissionOccurrences(txs []*core.Transaction, relay bool) []*core.Transaction {
	if os.Getenv("SAA_FIRST_PLACEMENT_FENCE") != "true" {
		return txs
	}
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	out := make([]*core.Transaction, 0, len(txs))
	if p.admissionLegSeen == nil {
		p.admissionLegSeen = map[string]string{}
	}
	for _, tx := range txs {
		if tx == nil || p.presenceSession == "" || !strings.HasPrefix(tx.AdmissionID, p.presenceSession+"/") || !tx.RoutingFrozen || tx.HasBroker || tx.Relayed != relay {
			panic("unregistered admission transport")
		}
		target := tx.RoutedSenderShard
		if relay {
			target = tx.RoutedRecipientShard
		}
		if target != p.ShardID {
			panic("wrong admission execution shard")
		}
		if b := tx.FirstPlacement; b != nil {
			if err := b.Validate(tx, p.presenceSession); err != nil {
				panic(err)
			}
			for a := range b.Claims {
				proof, ok := p.presenceCache[b.RequestID+"/"+a]
				if !ok || proof.Query.Session != b.Session || proof.Query.RequestID != b.RequestID || proof.Query.Account != a || proof.Query.Generation != b.Generation || proof.Query.Shard != int(p.ShardID) || proof.Response.Status != "ABSENT" {
					panic("creator_without_matching_absence_generation")
				}
			}
		}
		key := fmt.Sprintf("%s/%t", tx.AdmissionID, relay)
		digest := sha256.Sum256(tx.Encode())
		fingerprint := hex.EncodeToString(digest[:])
		if old, ok := p.admissionLegSeen[key]; ok {
			if old != fingerprint {
				panic("conflicting admission transport replay")
			}
			continue
		}
		p.admissionLegSeen[key] = fingerprint
		out = append(out, tx)
	}
	return out
}
