// storage is a key-value database and its interfaces indeed
// the information of block will be saved in storage

package storage

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

type Storage struct {
	dbFilePath            string // path to the database
	blockBucket           string // bucket in bolt database
	blockHeaderBucket     string // bucket in bolt database
	newestBlockHashBucket string // bucket in bolt database
	DataBase              *bolt.DB
}

// new a storage, build a bolt datase
func NewStorage(dbFp string, cc *params.ChainConfig) *Storage {
	dir := params.DatabaseWrite_path + "chainDB"
	errMkdir := os.MkdirAll(dir, os.ModePerm)
	if errMkdir != nil {
		log.Panic(errMkdir)
	}

	s := &Storage{
		dbFilePath:            dbFp,
		blockBucket:           "block",
		blockHeaderBucket:     "blockHeader",
		newestBlockHashBucket: "newestBlockHash",
	}

	db, err := bolt.Open(s.dbFilePath, 0600, nil)
	if err != nil {
		log.Panic(err)
	}

	// create buckets
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(s.blockBucket))
		if err != nil {
			log.Panic("create blocksBucket failed")
		}

		_, err = tx.CreateBucketIfNotExists([]byte(s.blockHeaderBucket))
		if err != nil {
			log.Panic("create blockHeaderBucket failed")
		}

		_, err = tx.CreateBucketIfNotExists([]byte(s.newestBlockHashBucket))
		if err != nil {
			log.Panic("create newestBlockHashBucket failed")
		}

		return nil
	})
	s.DataBase = db
	return s
}

// update the newest block in the database
func (s *Storage) UpdateNewestBlock(newestbhash []byte) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		nbhBucket := tx.Bucket([]byte(s.newestBlockHashBucket))
		// the bucket has the only key "OnlyNewestBlock"
		err := nbhBucket.Put([]byte("OnlyNewestBlock"), newestbhash)
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
	fmt.Println("The newest block is updated")
}

// add a blockheader into the database
func (s *Storage) AddBlockHeader(blockhash []byte, bh *core.BlockHeader) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.blockHeaderBucket))
		err := bhbucket.Put(blockhash, bh.Encode())
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
}

// add a block into the database
func (s *Storage) AddBlock(b *core.Block) {
	if err := s.StoreBlock(b); err != nil {
		log.Panic(err)
	}
}

// StoreBlock lets consensus application stop without advancing the in-memory
// head when the existing atomic block transaction fails.
func (s *Storage) StoreBlock(b *core.Block) error {
	if err := s.addBlockAtomic(b); err != nil {
		return err
	}
	fmt.Println("The newest block is updated")
	fmt.Println("Block is added")
	return nil
}

// Preserve the existing bucket/key/encoding layout, but commit the block,
// header and newest pointer together. Both encodings finish before any write;
// a failed Put rolls back every change. Bolt's default synchronous commit is
// retained: this removes redundant transactions, not durability barriers.
func (s *Storage) addBlockAtomic(b *core.Block) error {
	if b == nil || b.Header == nil || len(b.Hash) == 0 {
		return errors.New("block, header and nonempty hash are required")
	}
	hash := append([]byte(nil), b.Hash...)
	blockRaw, headerRaw := b.Encode(), b.Header.Encode()
	return s.DataBase.Update(func(tx *bolt.Tx) error {
		writes := []struct {
			bucket string
			key    []byte
			value  []byte
		}{
			{s.blockBucket, hash, blockRaw},
			{s.blockHeaderBucket, hash, headerRaw},
			{s.newestBlockHashBucket, []byte("OnlyNewestBlock"), hash},
		}
		for _, write := range writes {
			bucket := tx.Bucket([]byte(write.bucket))
			if bucket == nil {
				return fmt.Errorf("missing block storage bucket %q", write.bucket)
			}
			if err := bucket.Put(write.key, write.value); err != nil {
				return fmt.Errorf("write block storage bucket %q: %w", write.bucket, err)
			}
		}
		return nil
	})
}

// read a blockheader from the database
func (s *Storage) GetBlockHeader(bhash []byte) (*core.BlockHeader, error) {
	var res *core.BlockHeader
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.blockHeaderBucket))
		bh_encoded := bhbucket.Get(bhash)
		if bh_encoded == nil {
			return errors.New("the block is not existed")
		}
		res = core.DecodeBH(bh_encoded)
		return nil
	})
	return res, err
}

// read a block from the database
func (s *Storage) GetBlock(bhash []byte) (*core.Block, error) {
	var res *core.Block
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bbucket := tx.Bucket([]byte(s.blockBucket))
		b_encoded := bbucket.Get(bhash)
		if b_encoded == nil {
			return errors.New("the block is not existed")
		}
		res = core.DecodeB(b_encoded)
		return nil
	})
	return res, err
}

// read the Newest block hash
func (s *Storage) GetNewestBlockHash() ([]byte, error) {
	var nhb []byte
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.newestBlockHashBucket))
		// the bucket has the only key "OnlyNewestBlock"
		nhb = bhbucket.Get([]byte("OnlyNewestBlock"))
		if nhb == nil {
			return errors.New("cannot find the newest block hash")
		}
		return nil
	})
	return nhb, err
}
