package committee

import (
	"blockEmulator/params"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func inputEOPFixture(t *testing.T) (*nativeGroupControl, string) {
	t.Helper()
	oldTotal, oldDataset := params.TotalDataSize, params.DatasetFile
	params.TotalDataSize, params.DatasetFile = 10, "/fixture/input.csv"
	t.Cleanup(func() { params.TotalDataSize, params.DatasetFile = oldTotal, oldDataset })
	t.Setenv("SAA_FOREST_INPUT_SHA256", strings.Repeat("a", 64))
	t.Setenv("BLOCK_EMULATOR_CONFIG_PATH", "/fixture/run/paramsConfig.json")
	dir := t.TempDir()
	g := &nativeGroupControl{serial: &serialGroupControl{directory: dir, continuous: true, next: 2, loaded: true}, stage: 4, active: true}
	return g, filepath.Join(dir, "native-input-ended.json")
}

func TestInputEOPUsesNativeAdmissionAndPreservesPendingMigration(t *testing.T) {
	g, path := inputEOPFixture(t)
	atomic.StoreInt64(&g.inputCount, 9)
	if err := g.publishNativeInputEOP(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("partial admission published EOP: %v", err)
	}
	atomic.StoreInt64(&g.inputCount, 10)
	if err := g.publishNativeInputEOP(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker nativeInputEOP
	if err = json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	if marker.Schema != "native_input_eop_v1" || marker.Event != "native_input_ended" || marker.At.IsZero() || marker.AcceptedOccurrences != 10 || marker.TotalOccurrences != 10 || marker.InputSHA256 != strings.Repeat("a", 64) || marker.ConfigPath != "/fixture/run/paramsConfig.json" || marker.DatasetFile != "/fixture/input.csv" || marker.NativeCommitsComplete {
		t.Fatalf("invalid EOP binding/completion claim: %+v", marker)
	}
	if !g.serial.loaded || g.serial.done || g.serial.next != 2 || g.retired || !g.active || g.stage != 4 {
		t.Fatal("EOP changed the pending migration state")
	}
}

func TestInputEOPRejectsMissingBindingOrInvalidWatermark(t *testing.T) {
	for _, kind := range []string{"input_hash", "config_path", "dataset_file", "excess_admission"} {
		t.Run(kind, func(t *testing.T) {
			g, path := inputEOPFixture(t)
			atomic.StoreInt64(&g.inputCount, 10)
			switch kind {
			case "input_hash":
				t.Setenv("SAA_FOREST_INPUT_SHA256", "")
			case "config_path":
				t.Setenv("BLOCK_EMULATOR_CONFIG_PATH", "")
			case "dataset_file":
				params.DatasetFile = ""
			case "excess_admission":
				atomic.StoreInt64(&g.inputCount, 11)
			}
			if err := g.publishNativeInputEOP(); err == nil {
				t.Fatal("invalid EOP was accepted")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("invalid EOP was published: %v", err)
			}
		})
	}
}
