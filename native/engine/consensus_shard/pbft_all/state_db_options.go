package pbft_all

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	gethleveldb "github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func stateDBOptions() (int, int) {
	read := func(key string, fallback int) int {
		v := os.Getenv(key)
		if v == "" {
			return fallback
		}
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 {
			panic(fmt.Sprintf("invalid %s=%q", key, v))
		}
		return n
	}
	return read("BLOCK_EMULATOR_STATE_DB_CACHE_MB", 0), read("BLOCK_EMULATOR_STATE_DB_HANDLES", 1)
}

// Keep the upstream database path unchanged unless a physical SST layout is
// explicitly selected. This does not change transaction admission, consensus,
// write-pause thresholds, WAL policy, or migration confirmation durability.
func openStateDB(path string) (ethdb.Database, error) {
	cache, handles := stateDBOptions()
	tableMB, err := stateDBTableMB()
	if err != nil {
		return nil, err
	}
	if tableMB == 0 {
		return rawdb.NewLevelDBDatabase(path, cache, handles, "accountState", false)
	}
	db, err := gethleveldb.NewCustom(path, "accountState", func(o *opt.Options) {
		configureStateDBTables(o, cache, handles, tableMB)
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("state_db_layout cacheMB=%d handles=%d tableMB=%d slowdown=%d pause=%d syncPolicy=upstream_unchanged\n", maxStateDBOption(cache, 16), maxStateDBOption(handles, 16), tableMB, opt.DefaultWriteL0SlowdownTrigger, opt.DefaultWriteL0PauseTrigger)
	return rawdb.NewDatabase(db), nil
}

func stateDBTableMB() (int, error) {
	raw := os.Getenv("BLOCK_EMULATOR_STATE_DB_TABLE_MB")
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > int(^uint(0)>>1)/opt.MiB {
		return 0, fmt.Errorf("invalid BLOCK_EMULATOR_STATE_DB_TABLE_MB=%q", raw)
	}
	return n, nil
}

func maxStateDBOption(n, minimum int) int {
	if n < minimum {
		return minimum
	}
	return n
}

func configureStateDBTables(o *opt.Options, cache, handles, tableMB int) {
	// Match go-ethereum v1.11.6 leveldb.New: NewCustom itself supplies the
	// Bloom filter and DisableSeeksCompaction. Only SST target size differs.
	cache = maxStateDBOption(cache, 16)
	o.OpenFilesCacheCapacity = maxStateDBOption(handles, 16)
	o.BlockCacheCapacity = cache / 2 * opt.MiB
	o.WriteBuffer = cache / 4 * opt.MiB
	o.CompactionTableSize = tableMB * opt.MiB
}
