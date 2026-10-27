package committee

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type nativeInputEOP struct {
	Schema                string    `json:"schema"`
	Event                 string    `json:"event"`
	At                    time.Time `json:"at"`
	AcceptedOccurrences   int64     `json:"accepted_occurrences"`
	TotalOccurrences      int64     `json:"total_occurrences"`
	InputSHA256           string    `json:"input_sha256"`
	DatasetFile           string    `json:"dataset_file"`
	ConfigPath            string    `json:"config_path"`
	NativeCommitsComplete bool      `json:"native_commits_complete"`
}

// Only the finite native input producer calls this, immediately after storing
// inputCount. Observer lag must not keep the command window open after EOP.
// This is admission evidence, never a transaction or migration completion.
func (g *nativeGroupControl) publishNativeInputEOP() error {
	if g.serial == nil || !g.serial.continuous {
		return nil
	}
	accepted, total := atomic.LoadInt64(&g.inputCount), int64(params.TotalDataSize)
	if total <= 0 || accepted > total {
		return fmt.Errorf("native input EOP watermark exceeds finite input")
	}
	if accepted < total {
		return nil
	}
	inputHash := os.Getenv("SAA_FOREST_INPUT_SHA256")
	configPath := os.Getenv("BLOCK_EMULATOR_CONFIG_PATH")
	if !validTransferDigest(inputHash) || configPath == "" || params.DatasetFile == "" {
		return fmt.Errorf("native input EOP requires input/config binding")
	}
	marker := nativeInputEOP{
		Schema: "native_input_eop_v1", Event: "native_input_ended", At: time.Now().UTC(),
		AcceptedOccurrences: accepted, TotalOccurrences: total,
		InputSHA256: inputHash, DatasetFile: params.DatasetFile, ConfigPath: configPath,
		NativeCommitsComplete: false,
	}
	raw, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	if err = core.PublishSourcePrepareFile(filepath.Join(g.serial.directory, "native-input-ended.json"), raw); err != nil {
		return err
	}
	if g.audit != nil {
		g.audit.write(marker)
	}
	return nil
}
