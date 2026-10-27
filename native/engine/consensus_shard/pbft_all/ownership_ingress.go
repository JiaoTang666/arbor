package pbft_all

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"log"
)

func (rrom *RawRelayOutsideModule) ownershipIngressEnabled() bool {
	return rrom != nil && rrom.pbftNode != nil && rrom.pbftNode.CurChain != nil && rrom.pbftNode.CurChain.OwnershipSecurityEnabled()
}

// Network rejection must not be an internal assertion panic. Security-off
// preserves the original decoder, filters and admission path.
func (rrom *RawRelayOutsideModule) decodeOutside(content []byte, output interface{}) bool {
	// Unmarshal accepts null without populating an allocated envelope. Reject
	// that missing transport before an empty relay can advance seqIDMap.
	if rrom.ownershipIngressEnabled() && bytes.Equal(bytes.TrimSpace(content), []byte("null")) {
		rrom.pbftNode.pl.Plog.Println("rejected malformed ownership transport: null envelope")
		return false
	}
	if err := json.Unmarshal(content, output); err != nil {
		if !rrom.ownershipIngressEnabled() {
			log.Panic(err)
		}
		rrom.pbftNode.pl.Plog.Printf("rejected malformed ownership transport: %v", err)
		return false
	}
	return true
}

// Validate every unfiltered transaction first, before replay filters can
// observe identities or change occurrence bookkeeping. The message type must
// agree with the native leg; authorization is not a remote debit proof.
func (rrom *RawRelayOutsideModule) checkOwnershipTransport(txs []*core.Transaction, relay bool, envelopeShard uint64) bool {
	if !rrom.ownershipIngressEnabled() {
		return true
	}
	if err := rrom.pbftNode.CurChain.RefreshOwnershipAuthorization(); err != nil {
		rrom.pbftNode.pl.Plog.Printf("rejected ownership transport refresh: %v", err)
		return false
	}
	for _, tx := range txs {
		if tx == nil || tx.Relayed != relay || tx.RoutedSenderShard != envelopeShard {
			rrom.pbftNode.pl.Plog.Println("rejected ownership transport stage or shard mismatch")
			return false
		}
		if err := rrom.pbftNode.CurChain.CheckOwnershipTransaction(tx); err != nil {
			rrom.pbftNode.pl.Plog.Printf("rejected unauthorized ownership transport: %v", err)
			return false
		}
	}
	return true
}

func (rrom *RawRelayOutsideModule) enqueueOutside(txs []*core.Transaction, source string) bool {
	if rrom.ownershipIngressEnabled() {
		if err := rrom.pbftNode.CurChain.TrySendAuthorizedTxs(txs); err != nil {
			rrom.pbftNode.pl.Plog.Printf("rejected ownership queue admission: %v", err)
			return false
		}
	} else {
		rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(txs)
	}
	rrom.pbftNode.noteWorkloadArrival(len(txs), source)
	return true
}
