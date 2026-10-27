package main

import (
	"fmt"
	"os"
	"path/filepath"

	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/supervisor/committee"
)

func main() {
	path := filepath.Join(os.TempDir(), "saa_native_receipts_smoke.jsonl")
	_ = os.Remove(path)
	_ = os.Setenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH", path)
	pbft_all.EmitNativeInstallationReceipt("protocol-smoke-proposal", "0xsmoke", "smoke-hash")
	m := &committee.RuntimeUnknownAdmissionManager{}
	var offset int64
	n, err := m.ConsumeNativeInstallationReceipts(path, &offset)
	if err != nil || n != 1 {
		panic(fmt.Sprintf("receipt smoke failed: n=%d err=%v", n, err))
	}
	fmt.Printf("protocol smoke passed: receipts=%d offset=%d path=%s\n", n, offset, path)
}
