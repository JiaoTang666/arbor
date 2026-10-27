// ownership_member is one persistent signing member using trusted local IPC.
// It never opens a network listener or loads the rest of the committee's keys.
package main

import (
	"blockEmulator/initialledger"
	"blockEmulator/ownership"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func run() error {
	genesisPath := flag.String("genesis", "", "initial ledger file")
	genesisHash := flag.String("genesis-sha256", "", "trusted ledger hash")
	configPath := flag.String("config", "", "pinned public committee config")
	configHash := flag.String("config-sha256", "", "trusted config hash")
	shards := flag.Uint64("shards", 0, "expected shard count")
	memberID := flag.String("member", "", "one pinned member ID")
	keyPath := flag.String("key", "", "0600 regular file: 64-byte private key encoded as hex, no newline")
	storePath := flag.String("store", "", "this key's persistent signer DB; parent must exist")
	storageMode := flag.String("storage-mode", "", "pin delta_v1, certificate_v1 or append_v1; never convert an existing store")
	requirePlacement := flag.Bool("require-placement-witness", false, "pin independent OO/NO/NN placement validation")
	flag.Parse()
	ledger, err := initialledger.Load(*genesisPath, *genesisHash, *shards)
	if err != nil {
		return err
	}
	var config ownership.Config
	if err := ownership.ReadPinnedJSON(*configPath, *configHash, &config, 1<<20); err != nil {
		return err
	}
	verifier, err := ownership.NewVerifier(config)
	if err != nil {
		return err
	}
	if verifier.Context().GenesisDigest != ledger.FileHash || verifier.Context().Shards != *shards {
		return fmt.Errorf("signer genesis/topology mismatch")
	}
	info, err := os.Lstat(*keyPath)
	if err != nil {
		return fmt.Errorf("private key file unavailable")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() != 2*ed25519.PrivateKeySize {
		return fmt.Errorf("private key requires a private regular hex file")
	}
	raw, err := os.ReadFile(*keyPath)
	if err != nil {
		return fmt.Errorf("private key file unreadable")
	}
	key, err := hex.DecodeString(string(raw))
	if err != nil {
		return fmt.Errorf("invalid private key encoding")
	}
	var signer ownership.MemberSigner
	if *storageMode == ownership.AppendSignerMode {
		signer, err = ownership.OpenAppendSignerWithPolicy(*storePath, verifier, ledger.Routes, *memberID, ed25519.PrivateKey(key), *requirePlacement)
	} else {
		signer, err = ownership.OpenSignerWithPolicy(*storePath, verifier, ledger.Routes, *memberID, ed25519.PrivateKey(key), *storageMode, *requirePlacement)
	}
	if err != nil {
		return err
	}
	defer signer.Close()
	return ownership.ServeMember(signer, os.Stdin, os.Stdout)
}

func main() {
	if err := run(); err != nil {
		json.NewEncoder(os.Stderr).Encode(map[string]string{"status": "FAIL", "error": err.Error()})
		os.Exit(1)
	}
}
