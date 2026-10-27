// Here the blockchain structrue is defined
// each node in this system will maintain a blockchain object.

package chain

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"blockEmulator/storage"
	"blockEmulator/utils"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type BlockChain struct {
	initialLedger            *initialledger.Ledger
	ownership                *ownership.View     // immutable preauthorized first-owner view
	db                       ethdb.Database      // the leveldb database to store in the disk, for status trie
	triedb                   *trie.Database      // the trie database which helps to store the status trie
	ChainConfig              *params.ChainConfig // the chain configuration, which can help to identify the chain
	CurrentBlock             *core.Block         // the top block in this blockchain
	Storage                  *storage.Storage    // Storage is the bolt-db to store the blocks
	Txpool                   *core.TxPool        // the transaction pool
	PartitionMap             map[string]uint64   // the partition map which is defined by some algorithm can help account parition
	pmlock                   sync.RWMutex
	partitionSeedLock        sync.Mutex
	partitionSeeded          bool
	proposalIngressLock      sync.Mutex
	processedProposalIngress map[string]bool
	mappingUpdateCacheLock   sync.Mutex
	mappingUpdateCaches      map[string]*externalObjectMappingUpdateCache
	sourcePreparationMu      sync.RWMutex
	sourcePreparation        *core.SourcePreparation
	groupSourcePreparation   *core.GroupPreparation
	groupSourceHistory       []*core.GroupPreparation
	pendingExecution         *blockExecution          // one candidate; serialized by native consensus
	committedExecution       *committedBlockExecution // one committed report, not historical state
}

// Get the transaction root, this root can be used to check the transactions
func GetTxTreeRoot(txs []*core.Transaction) []byte {
	// use a memory trie database to do this, instead of disk database
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txs {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	return transactionTree.Hash().Bytes()
}

// Get bloom filter
func GetBloomFilter(txs []*core.Transaction) *bitset.BitSet {
	bs := bitset.New(2048)
	for _, tx := range txs {
		bs.Set(utils.ModBytes(tx.TxHash, 2048))
	}
	return bs
}

// Write Partition Map
func (bc *BlockChain) Update_PartitionMap(key string, val uint64) {
	if bc.ownership != nil {
		if err := bc.ownership.Check(key, val); err != nil {
			panic(err)
		}
	}
	if bc.groupPreparedAccount(key) && val != bc.ChainConfig.ShardID {
		panic("prepared group mapping cannot change before group activation")
	}
	if p := bc.SourcePreparation(); p != nil && normalizePartitionMapKey(key) == p.Command.Account && val != p.Command.SourceShard {
		panic("prepared account mapping cannot activate before handoff protocol")
	}
	if bc.ownership != nil {
		return // Already bound: do not materialize a second authorization table.
	}
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	bc.PartitionMap[normalizePartitionMapKey(key)] = val
}

// Get parition (if not exist, return default)
func (bc *BlockChain) Get_PartitionMap(key string) uint64 {
	if bc.ownership != nil {
		owner, exists := bc.ownership.Owner(key)
		if !exists {
			panic("first owner not authorized")
		}
		return owner
	}
	normalizedKey := normalizePartitionMapKey(key)
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if _, ok := bc.PartitionMap[normalizedKey]; !ok {
		return uint64(utils.Addr2Shard(normalizedKey))
	}
	return bc.PartitionMap[normalizedKey]
}

// ResolveTxSenderShard and ResolveTxRecipientShard preserve the route selected
// when a transaction entered the system. Legacy/baseline transactions that do
// not carry routing metadata continue to use the current PartitionMap.
func (bc *BlockChain) ResolveTxSenderShard(tx *core.Transaction) uint64 {
	if tx != nil && tx.RoutingFrozen {
		return tx.RoutedSenderShard
	}
	if tx == nil {
		return 0
	}
	return bc.Get_PartitionMap(tx.Sender)
}

func (bc *BlockChain) ResolveTxRecipientShard(tx *core.Transaction) uint64 {
	if tx != nil && tx.RoutingFrozen {
		return tx.RoutedRecipientShard
	}
	if tx == nil {
		return 0
	}
	return bc.Get_PartitionMap(tx.Recipient)
}

func normalizePartitionMapKey(key string) string {
	normalized := strings.TrimSpace(strings.ToLower(key))
	if strings.HasPrefix(normalized, "0x") {
		normalized = strings.TrimPrefix(normalized, "0x")
	}
	return normalized
}

// Send a transaction to the pool (need to decide which pool should be sended)
func (bc *BlockChain) SendTx2Pool(txs []*core.Transaction) {
	if bc.ownership != nil {
		if err := bc.TrySendAuthorizedTxs(txs); err != nil {
			panic(err)
		}
		return
	}
	bc.Txpool.AddTxs2Pool(txs)
}

// Compatibility entry for auxiliary callers. Ordinary block proposal,
// validation and commit use the same error-returning private execution path.
func (bc *BlockChain) GetUpdateStatusTrie(txs []*core.Transaction) common.Hash {
	return bc.getUpdateStatusTrieObserved(txs, nil)
}

func (bc *BlockChain) getUpdateStatusTrieObserved(txs []*core.Transaction, timing *blockBuildTiming) common.Hash {
	execution, err := bc.executeBlockTransactions(txs, timing)
	if err == nil {
		err = bc.persistBlockExecution(execution, timing)
	}
	if err != nil {
		panic(fmt.Sprintf("forecast state application failed: %v", err))
	}
	return execution.root
}

// generate (mine) a block, this function return a block
func (bc *BlockChain) GenerateBlock(miner int32) *core.Block {
	timing := newBlockBuildTiming()
	defer timing.report(bc.ChainConfig.ShardID, bc.CurrentBlock.Header.Number+1)
	// Reject a bad journal prefix before the packer consumes pending work.
	if err := bc.RefreshOwnershipAuthorization(); err != nil {
		panic(err)
	}
	var txs []*core.Transaction
	// pack the transactions from the txpool
	if params.UseBlocksizeInBytes == 1 {
		txs = bc.Txpool.PackTxsWithBytes(params.BlocksizeInBytes)
	} else {
		txs = bc.Txpool.PackTxs(bc.ChainConfig.BlockSize)
	}
	timing.transactions = len(txs)
	timing.mark(buildPack)

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Now(),
	}
	// handle transactions to build root
	execution, err := bc.executeBlockTransactions(txs, timing)
	if err != nil {
		// Packing has already consumed the queue prefix. Fail-stop rather than
		// pretending this was an empty proposal and losing pending work.
		panic(fmt.Sprintf("native proposal execution failed: %v", err))
	}

	bh.StateRoot = execution.root.Bytes()
	bh.TxRoot = GetTxTreeRoot(txs)
	bh.Bloom = *GetBloomFilter(txs)
	bh.Miner = miner
	b := core.NewBlock(bh, txs)
	b.ExecutionResults = append([]bool{}, execution.applied...)

	b.Hash = b.Header.Hash()
	bc.reportCompletionScheduling(b)
	timing.mark(buildFinalize)
	timing.complete = true
	return b
}

// new a genisis block, this func will be invoked only once for a blockchain object
func (bc *BlockChain) NewGenisisBlock() *core.Block {
	body := make([]*core.Transaction, 0)
	bh := &core.BlockHeader{
		Number: 0,
	}
	// build a new trie database by db
	triedb, err := newStateTrieDatabase(bc.db)
	if err != nil {
		panic(err)
	}
	bc.triedb = triedb
	statusTrie := trie.NewEmpty(triedb)
	if bc.initialLedger != nil {
		for _, account := range bc.initialLedger.Accounts {
			if account.Shard == bc.ChainConfig.ShardID {
				if err := statusTrie.Update([]byte(account.Address), account.State().Encode()); err != nil {
					panic(err)
				}
			}
		}
	}
	if err := InitializeTransactionEffectIndex(statusTrie, 0); err != nil {
		panic(err)
	}
	indexed, configErr := NativeEffectIndexConfiguration()
	if configErr != nil {
		panic(configErr)
	}
	bh.StateRoot = statusTrie.Hash().Bytes()
	if bc.initialLedger != nil || indexed {
		root, nodes := statusTrie.Commit(false)
		if nodes != nil {
			if err := triedb.Update(trie.NewWithNodeSet(nodes)); err != nil {
				panic(err)
			}
			if err := triedb.Commit(root, false); err != nil {
				panic(err)
			}
		}
		if bc.initialLedger != nil {
			if err := bc.db.Put([]byte(initialledger.Marker), []byte(bc.initialLedger.FileHash)); err != nil {
				panic(err)
			}
		}
	}
	bh.TxRoot = GetTxTreeRoot(body)
	bh.Bloom = *GetBloomFilter(body)
	b := core.NewBlock(bh, body)
	b.Hash = b.Header.Hash()
	return b
}

// add the genisis block in a blockchain
func (bc *BlockChain) AddGenisisBlock(gb *core.Block) {
	bc.Storage.AddBlock(gb)
	newestHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic()
	}
	curb, err := bc.Storage.GetBlock(newestHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = curb
}

// add a block
func (bc *BlockChain) AddBlock(b *core.Block) error {
	started := time.Now()
	var validateUs, persistUs, storeUs int64
	complete := false
	defer func() {
		fmt.Printf("block_commit_detail shard=%d validateUs=%d persistUs=%d storeUs=%d totalUs=%d complete=%t\n",
			bc.ChainConfig.ShardID, validateUs, persistUs, storeUs, time.Since(started).Microseconds(), complete)
	}()
	execution, err := bc.validateBlockExecution(b)
	validateUs = time.Since(started).Microseconds()
	if err != nil {
		return err
	}
	identity, err := committedExecutionIdentity(b, execution.body)
	if err != nil {
		return err
	}
	phase := time.Now()
	err = bc.persistBlockExecution(execution, nil)
	persistUs = time.Since(phase).Microseconds()
	if err != nil {
		return err
	}
	if err := bc.checkPreparedRoot(b.Header.StateRoot); err != nil {
		return err
	}
	// Never trust network-supplied outcome bits. Store the locally computed
	// result together with the block in the existing atomic Bolt transaction.
	b.ExecutionResults = append([]bool{}, execution.applied...)
	phase = time.Now()
	err = bc.Storage.StoreBlock(b)
	storeUs = time.Since(phase).Microseconds()
	if err != nil {
		return err
	}
	bc.CurrentBlock = b
	bc.committedExecution = &committedBlockExecution{identity: identity, applied: append([]bool{}, execution.applied...)}
	bc.pendingExecution = nil
	complete = true
	return nil
}

// new a blockchain.
// the ChainConfig is pre-defined to identify the blockchain; the db is the status trie database in disk
func NewBlockChain(cc *params.ChainConfig, db ethdb.Database) (*BlockChain, error) {
	ledger, loadErr := initialledger.FromEnv(cc.ShardNums)
	if loadErr != nil {
		return nil, loadErr
	}
	marked, markerErr := db.Has([]byte(initialledger.Marker))
	if markerErr != nil {
		return nil, markerErr
	}
	if marked {
		return nil, errors.New("initial ledger v1 requires a fresh database; restart is not supported")
	}
	fmt.Println("Generating a new blockchain", db)
	chainDBfp := params.DatabaseWrite_path + fmt.Sprintf("chainDB/S%d_N%d", cc.ShardID, cc.NodeID)
	bc := &BlockChain{
		initialLedger:            ledger,
		db:                       db,
		ChainConfig:              cc,
		Txpool:                   core.NewTxPool(),
		PartitionMap:             make(map[string]uint64),
		processedProposalIngress: make(map[string]bool),
	}
	if err := bc.configureOwnershipSecurity(); err != nil {
		return nil, err // No chain database or genesis has been written.
	}
	bc.Storage = storage.NewStorage(chainDBfp, cc)
	if err := bc.configureCompletionScheduling(); err != nil {
		bc.Storage.DataBase.Close()
		return nil, err
	}
	indexedAdmission, admissionConfigErr := NativeEffectIndexConfiguration()
	if admissionConfigErr != nil {
		bc.Storage.DataBase.Close()
		return nil, admissionConfigErr
	}
	if indexedAdmission {
		bc.Txpool.EnableIndexedMigrationAdmission()
	}
	curHash, err := bc.Storage.GetNewestBlockHash()
	if ledger != nil {
		if err == nil {
			bc.Storage.DataBase.Close()
			return nil, errors.New("initial ledger cannot overwrite existing chain")
		}
		if bc.ownership == nil {
			for account, shard := range ledger.Routes {
				bc.PartitionMap[account] = shard
			}
		}
	}
	if err != nil {
		fmt.Println("There is no existed blockchain in the database. ")
		// if the Storage bolt database cannot find the newest blockhash,
		// it means the blockchain should be built in height = 0
		if err.Error() == "cannot find the newest block hash" {
			genisisBlock := bc.NewGenisisBlock()
			bc.AddGenisisBlock(genisisBlock)
			fmt.Println("New genisis block")
			return bc, nil
		}
		log.Panic()
	}

	// there is a blockchain in the storage
	fmt.Println("Existing blockchain found")
	curb, err := bc.Storage.GetBlock(curHash)
	if err != nil {
		log.Panic()
	}

	bc.CurrentBlock = curb
	triedb, err := newStateTrieDatabase(db)
	if err != nil {
		bc.Storage.DataBase.Close()
		return nil, err
	}
	bc.triedb = triedb
	// check the existence of the trie database
	_, err = trie.New(trie.TrieID(common.BytesToHash(curb.Header.StateRoot)), triedb)
	if err != nil {
		log.Panic()
	}
	fmt.Println("The status trie can be built")
	status, err := bc.stateTrie()
	if err != nil {
		bc.Storage.DataBase.Close()
		return nil, err
	}
	indexed, configErr := NativeEffectIndexConfiguration()
	coverage, coverageErr := transactionEffectCoverage(status)
	if configErr != nil || coverageErr != nil || (indexed && coverage == nil) {
		bc.Storage.DataBase.Close()
		if configErr != nil {
			return nil, configErr
		}
		if coverageErr != nil {
			return nil, coverageErr
		}
		return nil, errors.New("cannot enable effect index on an existing ledger without verified backfill")
	}
	if err = InitializeTransactionEffectIndex(status, curb.Header.Number); err != nil {
		bc.Storage.DataBase.Close()
		return nil, err
	}
	fmt.Println("Generated a new blockchain successfully")
	if err := bc.restoreSourcePreparation(); err != nil {
		return nil, err
	}
	if err := bc.restoreGroupSourcePreparation(); err != nil {
		return nil, err
	}
	if err := bc.restoreTargetHolds(); err != nil {
		return nil, err
	}
	return bc, nil
}

// check a block is valid or not in this blockchain config
func (bc *BlockChain) IsValidBlock(b *core.Block) error {
	_, err := bc.validateBlockExecution(b)
	return err
}

// add accounts
func (bc *BlockChain) AddAccounts(ac []string, as []*core.AccountState, miner int32) {
	if bc.ownership != nil {
		panic("ownership security v1 forbids out-of-band state creation")
	}
	for _, account := range ac {
		if err := bc.rejectPreparedWrite(account); err != nil {
			panic(err)
		}
	}
	fmt.Printf("The len of accounts is %d, now adding the accounts\n", len(ac))

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Time{},
	}
	// handle transactions to build root
	rt := bc.CurrentBlock.Header.StateRoot
	if len(ac) != 0 {
		st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
		if err != nil {
			log.Panic(err)
		}
		for i, addr := range ac {
			if bc.Get_PartitionMap(addr) == bc.ChainConfig.ShardID {
				ib := new(big.Int)
				ib.Add(ib, as[i].Balance)
				new_state := &core.AccountState{
					Balance: ib,
					Nonce:   as[i].Nonce,
				}
				st.Update([]byte(addr), new_state.Encode())
			}
		}

		rrt, ns := st.Commit(false)

		// if `ns` is nil, the `err = bc.triedb.Update(trie.NewWithNodeSet(ns))` will report an error.
		if ns != nil {
			err = bc.triedb.Update(trie.NewWithNodeSet(ns))
			if err != nil {
				log.Panic(err)
			}
			err = bc.triedb.Commit(rrt, false)
			if err != nil {
				log.Panic(err)
			}
			rt = rrt.Bytes()
		}
	}

	emptyTxs := make([]*core.Transaction, 0)
	bh.StateRoot = rt
	bh.TxRoot = GetTxTreeRoot(emptyTxs)
	bh.Bloom = *GetBloomFilter(emptyTxs)
	bh.Miner = 0
	b := core.NewBlock(bh, emptyTxs)
	b.Hash = b.Header.Hash()

	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

// fetch accounts
func (bc *BlockChain) FetchAccounts(addrs []string) []*core.AccountState {
	res := make([]*core.AccountState, 0)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for _, addr := range addrs {
		asenc, _ := st.Get([]byte(addr))
		var state_a *core.AccountState
		if asenc == nil {
			ib := new(big.Int)
			ib.Add(ib, params.Init_Balance)
			state_a = &core.AccountState{
				Nonce:   uint64(0),
				Balance: ib,
			}
		} else {
			state_a = core.DecodeAS(asenc)
		}
		res = append(res, state_a)
	}
	return res
}

// close a blockChain, close the database inferfaces
func (bc *BlockChain) CloseBlockChain() {
	bc.Storage.DataBase.Close()
	bc.triedb.CommitPreimages()
	bc.db.Close()
}

// print the details of a blockchain
func (bc *BlockChain) PrintBlockChain() string {
	vals := []interface{}{
		bc.CurrentBlock.Header.Number,
		bc.CurrentBlock.Hash,
		bc.CurrentBlock.Header.StateRoot,
		bc.CurrentBlock.Header.Time,
		bc.triedb,
		// len(bc.Txpool.RelayPool[1]),
	}
	res := fmt.Sprintf("%v\n", vals)
	fmt.Println(res)
	return res
}
