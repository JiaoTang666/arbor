package storage

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

func atomicStorageFixture(t *testing.T) *Storage {
	t.Helper()
	dir := t.TempDir()
	previous := params.DatabaseWrite_path
	params.DatabaseWrite_path = dir + string(os.PathSeparator)
	s := NewStorage(filepath.Join(dir, "chain.bolt"), &params.ChainConfig{})
	t.Cleanup(func() {
		s.DataBase.Close()
		params.DatabaseWrite_path = previous
	})
	return s
}

func atomicBlockFixture(number uint64) *core.Block {
	at := time.Unix(int64(number), 0).UTC()
	header := &core.BlockHeader{Number: number, Time: at, StateRoot: []byte{byte(number)}, TxRoot: []byte{1}}
	tx := core.NewTransaction("sender", "recipient", big.NewInt(3), number, at)
	return &core.Block{Header: header, Body: []*core.Transaction{tx}, Hash: header.Hash()}
}

func storageCommitID(t *testing.T, s *Storage) int {
	t.Helper()
	var id int
	if err := s.DataBase.View(func(tx *bolt.Tx) error { id = tx.ID(); return nil }); err != nil {
		t.Fatal(err)
	}
	return id
}

func assertStoredBlock(t *testing.T, s *Storage, b *core.Block) {
	t.Helper()
	got, err := s.GetBlock(b.Hash)
	if err != nil || !bytes.Equal(got.Encode(), b.Encode()) {
		t.Fatalf("legacy block reader mismatch: %v", err)
	}
	header, err := s.GetBlockHeader(b.Hash)
	if err != nil || !bytes.Equal(header.Encode(), b.Header.Encode()) {
		t.Fatalf("legacy header reader mismatch: %v", err)
	}
}

func TestAddBlockSingleSynchronousAtomicCommitAndReopen(t *testing.T) {
	s := atomicStorageFixture(t)
	if s.DataBase.NoSync {
		t.Fatal("synchronous durability must remain enabled")
	}
	b := atomicBlockFixture(1)
	before := storageCommitID(t, s)
	s.AddBlock(b)
	if storageCommitID(t, s) != before+1 {
		t.Fatal("one AddBlock must commit exactly one write transaction")
	}
	assertStoredBlock(t, s, b)
	if err := s.DataBase.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(s.dbFilePath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.DataBase = db
	assertStoredBlock(t, s, b)
	head, err := s.GetNewestBlockHash()
	if err != nil || !bytes.Equal(head, b.Hash) {
		t.Fatalf("atomic newest pointer did not survive reopen: %v", err)
	}
}

func TestAddBlockLaterPutFailureRollsBackBlockAndNewestPointer(t *testing.T) {
	s := atomicStorageFixture(t)
	old, next := atomicBlockFixture(1), atomicBlockFixture(2)
	s.AddBlock(old)
	// The block Put succeeds first; a nested bucket makes the later header Put
	// fail with ErrIncompatibleValue. No production fault hook is required.
	if err := s.DataBase.Update(func(tx *bolt.Tx) error {
		_, err := tx.Bucket([]byte(s.blockHeaderBucket)).CreateBucket(next.Hash)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := storageCommitID(t, s)
	if err := s.addBlockAtomic(next); err == nil {
		t.Fatal("header Put conflict should fail the whole transaction")
	}
	if storageCommitID(t, s) != before {
		t.Fatal("failed transaction advanced the commit ID")
	}
	if _, err := s.GetBlock(next.Hash); err == nil {
		t.Fatal("failed later Put left a partially stored new block")
	}
	assertStoredBlock(t, s, old)
	head, err := s.GetNewestBlockHash()
	if err != nil || !bytes.Equal(head, old.Hash) {
		t.Fatalf("failed transaction changed latest pointer: %v", err)
	}
}

func TestAddBlockValidationAndEncodingFailBeforeWrites(t *testing.T) {
	s := atomicStorageFixture(t)
	old := atomicBlockFixture(1)
	s.AddBlock(old)
	for name, block := range map[string]*core.Block{
		"nil block":                   nil,
		"nil header":                  {Hash: []byte{2}},
		"empty key":                   {Header: &core.BlockHeader{}},
		"unencodable nil transaction": {Header: &core.BlockHeader{}, Hash: []byte{2}, Body: []*core.Transaction{nil}},
	} {
		t.Run(name, func(t *testing.T) {
			before := storageCommitID(t, s)
			func() {
				defer func() {
					if recover() == nil {
						t.Error("public AddBlock should preserve fail-fast behavior")
					}
				}()
				s.AddBlock(block)
			}()
			if storageCommitID(t, s) != before {
				t.Fatal("invalid encoding/input wrote a transaction")
			}
			head, err := s.GetNewestBlockHash()
			if err != nil || !bytes.Equal(head, old.Hash) {
				t.Fatalf("failed pre-encoding changed latest pointer: %v", err)
			}
		})
	}
}

func TestAddBlockPreservesExistingLayoutAndStandaloneWriterAPIs(t *testing.T) {
	s := atomicStorageFixture(t)
	old, next := atomicBlockFixture(1), atomicBlockFixture(2)
	if err := s.DataBase.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(s.blockBucket)).Put(old.Hash, old.Encode())
	}); err != nil {
		t.Fatal(err)
	}
	s.AddBlockHeader(old.Hash, old.Header)
	s.UpdateNewestBlock(old.Hash)
	assertStoredBlock(t, s, old)
	s.AddBlock(next)
	assertStoredBlock(t, s, old)
	assertStoredBlock(t, s, next)
	head, err := s.GetNewestBlockHash()
	if err != nil || !bytes.Equal(head, next.Hash) {
		t.Fatalf("legacy newest-pointer API mismatch: %v", err)
	}
}
