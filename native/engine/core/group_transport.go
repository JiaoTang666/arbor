package core

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

func SerialGroupEnabled() bool { return os.Getenv("SAA_SERIAL_GROUP_CONTROL") == "true" }

// Proposal identities never become filesystem paths. Legacy one-round runs
// retain their exact filenames; serial runs use immutable per-proposal files.
func GroupTransportPath(base, proposal string) string {
	if base == "" || !SerialGroupEnabled() {
		return base
	}
	hash := sha256.Sum256([]byte(proposal))
	return base + ".g-" + hex.EncodeToString(hash[:])
}

func GroupTransportPaths(base string) []string {
	if base == "" {
		return nil
	}
	if !SerialGroupEnabled() {
		return []string{base}
	}
	paths, _ := filepath.Glob(base + ".g-*")
	var result []string
	for _, path := range paths {
		suffix := path[len(base)+3:]
		b, err := hex.DecodeString(suffix)
		if err == nil && len(b) == 32 && len(suffix) == 64 {
			result = append(result, path)
		}
	}
	return result
}
