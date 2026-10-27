package chain

import (
	"blockEmulator/core"
	"blockEmulator/ownership"
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func ownershipRuntimeFixture(t *testing.T) (func(uint64) *BlockChain, ownership.Config, string) {
	t.Helper()
	open := ownershipSecurityFixture(t)
	var config ownership.Config
	if err := ownership.ReadPinnedJSON(os.Getenv("SAA_OWNERSHIP_CONFIG_PATH"), os.Getenv("SAA_OWNERSHIP_CONFIG_SHA256"), &config, 1<<20); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(t.TempDir(), "ownership.jsonl")
	if err := os.WriteFile(journal, nil, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_OWNERSHIP_CERTIFICATES_PATH", "")
	t.Setenv("SAA_OWNERSHIP_CERTIFICATES_SHA256", "")
	t.Setenv("SAA_OWNERSHIP_RUNTIME_JOURNAL_PATH", journal)
	return open, config, journal
}

func ownershipRuntimeCertificate(t *testing.T, config ownership.Config, base, start, end uint64, account string, shard uint64) ownership.Certificate {
	t.Helper()
	verifier, err := ownership.NewVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	batch := ownership.Batch{Schema: ownership.Schema, Context: config.Context, CommitteeDigest: verifier.CommitteeDigest(),
		BaseVersion: base, ProposalID: account, Bindings: []ownership.Binding{{Account: account, Shard: shard}},
		EvidenceVersion: base + 1, EvidenceDigest: strings.Repeat("4", 64), EvidenceCutoff: start, AdmissionStart: start, AdmissionEnd: end}
	payload, err := ownership.SigningBytes(batch)
	if err != nil {
		t.Fatal(err)
	}
	cert := ownership.Certificate{Batch: batch}
	// These deterministic keys belong to ownershipSecurityFixture only.
	for i := 0; i < 3; i++ {
		private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize))
		cert.Signatures = append(cert.Signatures, ownership.Signature{MemberID: config.Members[i].ID, Value: ed25519.Sign(private, payload)})
	}
	return cert
}

func appendOwnershipRuntimeCertificate(t *testing.T, journal string, cert ownership.Certificate) {
	t.Helper()
	f, err := os.OpenFile(journal, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(f).Encode(cert)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal("append runtime certificate", err, closeErr)
	}
}

func TestOwnershipRuntimeRefreshBeforePoolLock(t *testing.T) {
	open, config, journal := ownershipRuntimeFixture(t)
	bc := open(0)
	appendOwnershipRuntimeCertificate(t, journal, ownershipRuntimeCertificate(t, config, 0, 2, 4, securityLocalNew, 0))
	tx := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 2)
	if err := bc.Txpool.AddAuthorizedTxs([]*core.Transaction{tx}); err == nil || bc.ownership.Version() != 0 {
		t.Fatal("pool callback refreshed or trusted an unverified owner")
	}
	bc.Txpool.GetLocked()
	locked := true
	defer func() {
		if locked {
			bc.Txpool.GetUnlocked()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- bc.TrySendAuthorizedTxs([]*core.Transaction{tx}) }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(5 * time.Second)
	for bc.ownership.Version() == 0 {
		select {
		case err := <-done:
			t.Fatal("admission finished while the pool was locked", err)
		case <-ticker.C:
		case <-timeout:
			t.Fatal("batch refresh waited for the pool lock")
		}
	}
	bc.Txpool.GetUnlocked()
	locked = false
	if err := <-done; err != nil || bc.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("refreshed batch did not enter the queue", err)
	}
	if err := bc.CheckOwnershipTransaction(nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 1)); err == nil {
		t.Fatal("batch boundary lost the signed admission start")
	}
}

func TestOwnershipRuntimeRejectsBadSuffixBeforeQueueOrProposal(t *testing.T) {
	open, config, journal := ownershipRuntimeFixture(t)
	bc := open(0)
	appendOwnershipRuntimeCertificate(t, journal, ownershipRuntimeCertificate(t, config, 0, 0, 2, securityLocalNew, 0))
	valid := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 0)
	bc.SendTx2Pool([]*core.Transaction{valid})
	bad := ownershipRuntimeCertificate(t, config, 1, 2, 4, securityRemoteNew, 1)
	bad.Signatures[0].Value[0] ^= 1
	appendOwnershipRuntimeCertificate(t, journal, bad)
	if err := bc.CheckOwnershipTransaction(valid); err != nil {
		t.Fatal("pure check read the unverified suffix", err)
	}
	if err := bc.TrySendAuthorizedTxs([]*core.Transaction{valid}); err == nil {
		t.Fatal("batch boundary ignored an invalid new journal record")
	}
	if err := bc.checkOwnershipTransactions([]*core.Transaction{valid}); err == nil {
		t.Fatal("execution boundary ignored an invalid new journal record")
	}
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	before := st.Hash()
	if _, err := bc.applyIndexedRawRelay(st, valid, false); err == nil || st.Hash() != before {
		t.Fatal("direct indexed boundary applied state after a failed refresh", err)
	}
	requireSecurityPanic(t, func() { bc.GenerateBlock(0) })
	if bc.Txpool.GetTxQueueLen() != 1 || bc.ownership.Version() != 1 || bc.CurrentBlock.Header.Number != 0 {
		t.Fatal("rejected suffix changed queue, verified prefix or committed state")
	}
}
