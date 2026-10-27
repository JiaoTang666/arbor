// ownership_audit validates public first-owner authorization evidence. It does
// not generate keys, sign approvals, run consensus or launch transactions.
package main

import (
	"blockEmulator/initialledger"
	"blockEmulator/ownership"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func run() error {
	genesisPath := flag.String("genesis", "", "initial ledger file")
	genesisHash := flag.String("genesis-sha256", "", "trusted initial ledger hash")
	configPath := flag.String("config", "", "public committee configuration")
	configHash := flag.String("config-sha256", "", "trusted configuration hash")
	bundlePath := flag.String("certificates", "", "ordered certificate bundle")
	bundleHash := flag.String("certificates-sha256", "", "trusted bundle hash")
	shards := flag.Uint64("shards", 0, "expected shard count")
	registryPath := flag.String("registry", "", "optional local durable registry; parent must exist")
	flag.Parse()
	ledger, err := initialledger.Load(*genesisPath, *genesisHash, *shards)
	if err != nil {
		return err
	}
	view, err := ownership.LoadView(*configPath, *configHash, *bundlePath, *bundleHash, ledger.Routes, ledger.FileHash, *shards)
	if err != nil {
		return err
	}
	if *registryPath != "" {
		var cfg ownership.Config
		var certificates []ownership.Certificate
		if err := ownership.ReadPinnedJSON(*configPath, *configHash, &cfg, 1<<20); err != nil {
			return err
		}
		if err := ownership.ReadPinnedJSON(*bundlePath, *bundleHash, &certificates, 64<<20); err != nil {
			return err
		}
		verifier, err := ownership.NewVerifier(cfg)
		if err != nil {
			return err
		}
		registry, err := ownership.Open(*registryPath, verifier, ledger.Routes)
		if err != nil {
			return err
		}
		defer registry.Close()
		for _, cert := range certificates {
			if _, err := registry.Apply(cert); err != nil {
				return err
			}
		}
		restored, err := registry.Snapshot(ledger.Routes)
		if err != nil || restored.Version() != view.Version() {
			return fmt.Errorf("durable registry differs from bundle: %v", err)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
		"schema": "coolchain/ownership-audit-v1", "status": "PASS",
		"registry_version": view.Version(), "genesis_accounts": len(ledger.Routes),
		"context": view.Context(), "durable_registry": *registryPath != "",
		"committee_consensus": "NOT_IMPLEMENTED", "transaction_authorization": "NOT_IMPLEMENTED",
	})
}

func main() {
	if err := run(); err != nil {
		json.NewEncoder(os.Stderr).Encode(map[string]string{"status": "FAIL", "error": err.Error()})
		os.Exit(1)
	}
}
