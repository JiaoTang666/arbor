package ownership

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// ReadPinnedJSON never treats a file path or self-reported digest as trust.
// The expected hash must come from the caller's trusted launch configuration.
func ReadPinnedJSON(path, expected string, output interface{}, maxBytes int64) error {
	if !validDigest(expected) || maxBytes <= 0 {
		return fmt.Errorf("missing pinned digest")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > maxBytes || digestBytes(raw) != expected {
		return fmt.Errorf("file hash/size mismatch")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(output); err != nil {
		return err
	}
	var tail interface{}
	if err := dec.Decode(&tail); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

// LoadView intentionally supports only a pinned, preauthorized bundle. It
// does not simulate a distributed committee or issue runtime certificates.
func LoadView(configPath, configHash, bundlePath, bundleHash string, genesis map[string]uint64, genesisHash string, shards uint64) (*View, error) {
	base, err := NewGenesis(genesis, shards)
	if err != nil {
		return nil, err
	}
	return loadViewWithGenesis(configPath, configHash, bundlePath, bundleHash, base, genesisHash, shards)
}

func loadViewWithGenesis(configPath, configHash, bundlePath, bundleHash string, genesis *Genesis, genesisHash string, shards uint64) (*View, error) {
	var config Config
	if err := ReadPinnedJSON(configPath, configHash, &config, 1<<20); err != nil {
		return nil, err
	}
	v, err := NewVerifier(config)
	if err != nil {
		return nil, err
	}
	if v.Context().GenesisDigest != genesisHash || v.Context().Shards != shards {
		return nil, fmt.Errorf("native genesis/topology mismatch")
	}
	var certificates []Certificate
	if err := ReadPinnedJSON(bundlePath, bundleHash, &certificates, 64<<20); err != nil {
		return nil, err
	}
	view, err := buildViewWithGenesis(v, genesis, certificates)
	if err != nil {
		return nil, err
	}
	// Capture verified identities, not a later read of mutable environment.
	view.configHash, view.bundleHash = configHash, bundleHash
	return view, nil
}

// FromEnv is shared by the trusted bootstrap at the producer and execution
// shards. A partially configured security mode cannot silently become legacy.
func FromEnv(genesis map[string]uint64, genesisHash string, shards uint64) (*View, error) {
	return fromEnv(func() (*Genesis, error) {
		if genesis == nil {
			return nil, fmt.Errorf("security requires explicit initial ledger")
		}
		return NewGenesis(genesis, shards)
	}, genesisHash, shards)
}

// FromEnvWithGenesis shares a sealed ledger index, not a caller-owned map.
// The loader is evaluated only when enforcement is enabled and configured.
func FromEnvWithGenesis(load func() (*Genesis, error), genesisHash string, shards uint64) (*View, error) {
	return fromEnv(load, genesisHash, shards)
}

func fromEnv(load func() (*Genesis, error), genesisHash string, shards uint64) (*View, error) {
	keys := []string{"SAA_OWNERSHIP_CONFIG_PATH", "SAA_OWNERSHIP_CONFIG_SHA256", "SAA_OWNERSHIP_CERTIFICATES_PATH", "SAA_OWNERSHIP_CERTIFICATES_SHA256"}
	runtimePath := os.Getenv("SAA_OWNERSHIP_RUNTIME_JOURNAL_PATH")
	switch os.Getenv("SAA_OWNERSHIP_SECURITY_V1") {
	case "", "false":
		if runtimePath != "" {
			return nil, fmt.Errorf("runtime ownership journal without enforcement")
		}
		for _, key := range keys {
			if os.Getenv(key) != "" {
				return nil, fmt.Errorf("ownership settings without enforcement")
			}
		}
		return nil, nil
	case "true":
	default:
		return nil, fmt.Errorf("invalid ownership security mode")
	}
	required := keys
	if runtimePath != "" {
		required = keys[:2]
		if os.Getenv(keys[2]) != "" || os.Getenv(keys[3]) != "" {
			return nil, fmt.Errorf("runtime ownership excludes future offline bundles")
		}
	}
	for _, key := range required {
		if os.Getenv(key) == "" {
			return nil, fmt.Errorf("missing %s", key)
		}
	}
	if load == nil || genesisHash == "" {
		return nil, fmt.Errorf("security requires explicit initial ledger")
	}
	genesis, err := load()
	if err != nil {
		return nil, err
	}
	if genesis == nil {
		return nil, fmt.Errorf("security requires explicit initial ledger")
	}
	if runtimePath != "" {
		return loadRuntimeViewWithGenesis(os.Getenv(keys[0]), os.Getenv(keys[1]), runtimePath, genesis, genesisHash, shards)
	}
	return loadViewWithGenesis(os.Getenv(keys[0]), os.Getenv(keys[1]), os.Getenv(keys[2]), os.Getenv(keys[3]), genesis, genesisHash, shards)
}
