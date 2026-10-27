package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/hex"
	"fmt"
)

func epochLegKey(id string, relayed bool) string {
	return fmt.Sprintf("%s/%t", id, relayed)
}

func epochCanonicalAdmissionID(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for _, b := range []byte(id) {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}

func epochCanonicalHash(hash string) bool {
	b, err := hex.DecodeString(hash)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == hash
}

func epochAsyncTxValid(tx *core.Transaction) error {
	if tx == nil || !epochCanonicalAdmissionID(tx.AdmissionID) || len(tx.TxHash) != 32 {
		return fmt.Errorf("noncanonical async epoch admission identity or transaction hash")
	}
	return nil
}

func epochOwnerAccount(tx *core.Transaction) string {
	if tx.Relayed {
		return tx.Recipient
	}
	return tx.Sender
}

// Ingress creates the fingerprint once, after its timestamp is initialized.
// Pending objects are immutable while queued. An ordinary proposal removes its
// objects before the leader sets Relayed on committed first legs.
func (e *clpaEpochRuntime) epochPendingLeg(tx *core.Transaction, destination uint64) (message.CLPAEpochPendingLeg, error) {
	if err := epochAsyncTxValid(tx); err != nil {
		return message.CLPAEpochPendingLeg{}, err
	}
	hash, ok := e.admissions[epochLegKey(tx.AdmissionID, tx.Relayed)]
	if !ok || !epochCanonicalHash(hash) {
		return message.CLPAEpochPendingLeg{}, fmt.Errorf("pending leg has no explicit immutable source admission: %s", tx.AdmissionID)
	}
	return message.CLPAEpochPendingLeg{AdmissionID: tx.AdmissionID, Relayed: tx.Relayed, PayloadHash: hash, OwnerAccount: epochOwnerAccount(tx), Destination: destination}, nil
}

// Manifest hashes always cover JSON arrays, including [] for an empty list.
func epochManifestHash(legs []message.CLPAEpochPendingLeg) string {
	return epochFingerprint(append([]message.CLPAEpochPendingLeg{}, legs...))
}

func (c *CLPAPbftInsideExtraHandleMod) validateEpochManifestLocked(b message.CLPAEpochTransfer, owner func(string) uint64, seen map[string]bool) error {
	e, n := c.epoch, c.pbftNode
	local := b.FromShard == n.ShardID
	if local {
		if b.SourceRounds != e.rounds || len(b.Txs) != 0 || len(b.Addrs) != 0 {
			return fmt.Errorf("async local source bundle does not bind the actual cut or has retained payload")
		}
	} else if len(b.PendingManifest) != len(b.Txs) {
		return fmt.Errorf("async incoming payload does not match its manifest length")
	}
	if ready, ok := e.ready[b.FromShard]; ok && ready.RelayCut != b.SourceRounds {
		return fmt.Errorf("async source bundle differs from its sealed relay cut")
	}
	for i, leg := range b.PendingManifest {
		decoded, err := hex.DecodeString(leg.OwnerAccount)
		key := epochLegKey(leg.AdmissionID, leg.Relayed)
		if !epochCanonicalAdmissionID(leg.AdmissionID) || !epochCanonicalHash(leg.PayloadHash) || err != nil || len(decoded) != 20 || hex.EncodeToString(decoded) != leg.OwnerAccount || seen[key] || leg.Destination >= n.pbftChainConfig.ShardNums || n.CurChain.Get_PartitionMap(leg.OwnerAccount) != b.FromShard || owner(leg.OwnerAccount) != leg.Destination {
			return fmt.Errorf("invalid, repeated, or wrongly routed async pending identity")
		}
		seen[key] = true
		if !local {
			tx := b.Txs[i]
			if epochTxValid(tx, leg.Relayed) != nil || epochAsyncTxValid(tx) != nil || leg.Destination != n.ShardID || tx.AdmissionID != leg.AdmissionID || epochOwnerAccount(tx) != leg.OwnerAccount || epochFingerprint(tx) != leg.PayloadHash {
				return fmt.Errorf("async incoming transaction does not match its bound admission payload")
			}
		}
	}
	return nil
}

// A fixed leader must own every payload in its source manifest. Followers have
// no ordinary ingress and may hold only previously installed incoming work;
// their available subset is checked explicitly, never treated as a full queue.
// These metadata checkpoints do not recover a failed leader's missing payload.
func (c *CLPAPbftInsideExtraHandleMod) epochDeltaPendingLocked(payload clpaEpochInstallPayload) ([]*core.Transaction, []*core.Transaction, error) {
	e, n := c.epoch, c.pbftNode
	var manifest []message.CLPAEpochPendingLeg
	for _, b := range payload.Transfers {
		if b.FromShard == n.ShardID {
			manifest = b.PendingManifest
			break
		}
	}
	snapshot, err := n.CurChain.Txpool.SnapshotCLPAEpochPendingReferences()
	if err != nil {
		return nil, nil, err
	}
	leader := n.NodeID == uint64(n.view.Load())
	if leader && (len(snapshot) != len(manifest) || e.input.prefix != payload.Plan.InputCuts[n.ShardID]) {
		return nil, nil, fmt.Errorf("async fixed leader source payload or sealed input prefix is incomplete")
	}
	if leader && e.sentTransfers {
		if len(snapshot) != len(e.sourcePending) {
			return nil, nil, fmt.Errorf("async leader source queue changed after closure")
		}
		for i, tx := range snapshot {
			if tx != e.sourcePending[i] {
				return nil, nil, fmt.Errorf("async leader source queue changed after closure")
			}
		}
	}
	byKey := make(map[string]message.CLPAEpochPendingLeg, len(manifest))
	for _, leg := range manifest {
		byKey[epochLegKey(leg.AdmissionID, leg.Relayed)] = leg
	}
	retained := make([]*core.Transaction, 0, len(snapshot))
	seenLocal := map[string]bool{}
	for i, tx := range snapshot {
		if epochTxValid(tx, tx.Relayed) != nil || epochAsyncTxValid(tx) != nil {
			return nil, nil, fmt.Errorf("invalid local async pending payload")
		}
		key := epochLegKey(tx.AdmissionID, tx.Relayed)
		leg, ok := byKey[key]
		cached, admitted := e.admissions[key]
		if !ok || !admitted || seenLocal[key] || leg.PayloadHash != cached || leg.OwnerAccount != epochOwnerAccount(tx) || (leader && manifest[i] != leg) {
			return nil, nil, fmt.Errorf("async local pending payload differs from the explicit source manifest")
		}
		seenLocal[key] = true
		if leg.Destination == n.ShardID {
			retained = append(retained, tx)
		}
	}
	return snapshot, retained, nil
}

func epochPayloadManifests(payload clpaEpochInstallPayload, shard uint64) (source, retained, incoming []message.CLPAEpochPendingLeg) {
	source, retained, incoming = []message.CLPAEpochPendingLeg{}, []message.CLPAEpochPendingLeg{}, []message.CLPAEpochPendingLeg{}
	for _, b := range payload.Transfers {
		if b.FromShard == shard {
			source = append(source, b.PendingManifest...)
			for _, leg := range b.PendingManifest {
				if leg.Destination == shard {
					retained = append(retained, leg)
				}
			}
		} else {
			incoming = append(incoming, b.PendingManifest...)
		}
	}
	return
}
