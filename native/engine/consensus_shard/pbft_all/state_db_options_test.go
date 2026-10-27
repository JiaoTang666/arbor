package pbft_all

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestStateDBOptions(t *testing.T) {
	t.Setenv("BLOCK_EMULATOR_STATE_DB_CACHE_MB", "")
	t.Setenv("BLOCK_EMULATOR_STATE_DB_HANDLES", "")
	a, b := stateDBOptions()
	if a != 0 || b != 1 {
		t.Fatal(a, b)
	}
	t.Setenv("BLOCK_EMULATOR_STATE_DB_CACHE_MB", "128")
	t.Setenv("BLOCK_EMULATOR_STATE_DB_HANDLES", "256")
	a, b = stateDBOptions()
	if a != 128 || b != 256 {
		t.Fatal(a, b)
	}
}

func TestStateDBTableLayoutPreservesDurabilityAndBackpressure(t *testing.T) {
	t.Setenv("BLOCK_EMULATOR_STATE_DB_TABLE_MB", "")
	if size, err := stateDBTableMB(); err != nil || size != 0 {
		t.Fatalf("unset must retain upstream opening: %d %v", size, err)
	}
	t.Setenv("BLOCK_EMULATOR_STATE_DB_TABLE_MB", "16")
	size, err := stateDBTableMB()
	if err != nil || size != 16 {
		t.Fatal(size, err)
	}
	o := &opt.Options{}
	configureStateDBTables(o, 128, 256, size)
	if o.GetCompactionTableSize(0) != 16*opt.MiB || o.GetWriteBuffer() != 32*opt.MiB || o.GetBlockCacheCapacity() != 64*opt.MiB || o.GetOpenFilesCacheCapacity() != 256 {
		t.Fatalf("unexpected physical layout: %+v", o)
	}
	base := &opt.Options{}
	if o.NoSync || o.DisableCompactionBackoff || o.GetWriteL0PauseTrigger() != base.GetWriteL0PauseTrigger() || o.GetWriteL0SlowdownTrigger() != base.GetWriteL0SlowdownTrigger() || o.GetCompactionL0Trigger() != base.GetCompactionL0Trigger() || o.GetCompactionTotalSize(1) != base.GetCompactionTotalSize(1) {
		t.Fatal("physical table sizing must not relax sync or compaction backpressure")
	}
	for _, invalid := range []string{"0", "-1", "oops", "999999999999999999999"} {
		t.Setenv("BLOCK_EMULATOR_STATE_DB_TABLE_MB", invalid)
		if _, err := stateDBTableMB(); err == nil {
			t.Fatalf("accepted invalid size %q", invalid)
		}
	}
}

func TestStateDBTableLayoutReopensExistingData(t *testing.T) {
	t.Setenv("BLOCK_EMULATOR_STATE_DB_CACHE_MB", "16")
	t.Setenv("BLOCK_EMULATOR_STATE_DB_HANDLES", "16")
	path := filepath.Join(t.TempDir(), "state")
	key, value := []byte("durable-account-state"), []byte("same-account-state-and-effect")
	// Both directions must retain existing keys. This is close/reopen format
	// compatibility, not a power-loss recovery or performance claim.
	for i, layout := range []string{"", "16", ""} {
		t.Setenv("BLOCK_EMULATOR_STATE_DB_TABLE_MB", layout)
		db, err := openStateDB(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			err = db.Put(key, value)
		} else {
			var got []byte
			got, err = db.Get(key)
			if err == nil && !bytes.Equal(got, value) {
				t.Error("existing state changed")
			}
		}
		closeErr := db.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
	}
}
