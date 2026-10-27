// the define and some operation of txpool

package core

import (
	"blockEmulator/utils"
	"log"
	"sync"
	"time"
	"unsafe"
)

type TxPool struct {
	transactionValidation func(*Transaction) error // immutable, optional startup authorization
	completionScheduling *completionScheduler // optional bounded-prefix selector; default nil/FIFO
	migrationJournal     *migrationQueueJournal
	lanes                *migrationLanes // non-nil only after durable journal enable
	// journalGate serializes durable admissions and drain/ACK generations.
	// Admission/ACK/drain I/O runs outside the queue lock after journal enable.
	// Initial enable still holds the queue lock to capture its durable boundary.
	journalGate               journalGate
	completedReceipts         *completedReceiptIndex             // journalGate-owned; disposable validated history
	migrationJournalPublish   func(string, []byte) error         // test-only fault/latency injection
	indexedMigrationAdmission bool                               // startup-only: pending recovery work has one admission/effect
	migrationAdmissionCache   map[string]cachedMigrationIdentity // disposable; journalGate-owned, keyed by immutable original bytes
	heldMigrationAccounts     map[string]bool
	TxQueue                   []*Transaction            // legacy queue; journal mode uses lanes
	RelayPool                 map[uint64][]*Transaction //designed for sharded blockchain, from Monoxide
	lock                      sync.Mutex
	sourcePreparedAccount     string
	// The pending list is ignored
}

func NewTxPool() *TxPool {
	return &TxPool{
		TxQueue:   make([]*Transaction, 0),
		RelayPool: make(map[uint64][]*Transaction),
	}
}

// Add a transaction to the pool (consider the queue only)
func (txpool *TxPool) AddTx2Pool(tx *Transaction) {
	txpool.AddTxs2Pool([]*Transaction{tx})
}

// Add a list of transactions to the pool
func (txpool *TxPool) AddTxs2Pool(txs []*Transaction) {
	txpool.addTransactions(txs, false)
}

// addTransactions uses the same classification boundary for head and tail.
// Pool locking binds membership lookup to immediate ordinary admission; a
// concurrent journal enable/extension therefore cannot escape its snapshot.
func (txpool *TxPool) addTransactions(txs []*Transaction, head bool) {
	for _, tx := range txs {
		if tx == nil {
			panic("nil transaction in admission batch")
		}
	}
	// Classification and ordinary admission share the same short lock as
	// journal enable. If admission wins, enable snapshots these entries; if
	// enable wins, we observe its installed journal. Never read its sequence
	// here: that field is owned by journalGate, unlike immutable membership.
	txpool.lock.Lock()
	if err := txpool.validateTransactionsLocked(txs); err != nil {
		txpool.lock.Unlock()
		panic(err)
	}
	j := txpool.migrationJournal
	touches := false
	for _, tx := range txs {
		if j != nil {
			touches = touches || journalRequiresRecovery(tx, j.account, j.accounts)
		}
		if !head && tx.Time.IsZero() {
			tx.Time = time.Now()
		}
	}
	if !touches {
		// Ordinary execution (including valid new-route inbound debit) does not
		// own a migration recovery obligation. Queue order
		// is admission order, not the start order of concurrent function calls.
		txpool.appendPendingLocked(txs, head)
		txpool.lock.Unlock()
		return
	}
	txpool.lock.Unlock()
	// Mixed batches remain indivisible. Do not hold the pool lock while
	// waiting for the journal gate (enable/ACK acquire them in reverse order).
	// Encode immutable contact originals before owning the durable writer.
	// Membership is reclassified under the gate below: a newly extended member
	// falls back to encoding there, never to an unjournaled admission.
	prepareStart := time.Now()
	encoded := make(map[*Transaction][]byte)
	for _, tx := range txs {
		if journalTouches(tx, j.account, j.accounts) {
			encoded[tx] = tx.Encode()
		}
	}
	timing := journalAdmissionTiming{Prepare: time.Since(prepareStart)}
	release := txpool.lockJournalAdmission(len(txs), &timing)
	defer release()
	stage := time.Now()
	filtered, err := txpool.filterIndexedMigrationAdmission(txs)
	timing.Filter = time.Since(stage)
	if err != nil {
		panic(err)
	}
	if len(filtered) == 0 {
		return
	}
	txs = filtered
	stage = time.Now()
	if err := txpool.persistMigrationQueueAdditionEncodedLocked(txs, head, encoded, &timing); err != nil {
		timing.Persist = time.Since(stage)
		panic(err)
	}
	timing.Persist = time.Since(stage)
	stage = time.Now()
	txpool.lock.Lock()
	timing.PoolWait = time.Since(stage)
	stage = time.Now()
	txpool.appendPendingEncodedLocked(txs, head, encoded)
	timing.Queue = time.Since(stage)
	txpool.lock.Unlock()
}

// add transactions into the pool head
func (txpool *TxPool) AddTxs2Pool_Head(tx []*Transaction) {
	txpool.addTransactions(tx, true)
}

type journalAdmissionTiming struct {
	Prepare, Filter, Persist, Publish, PoolWait, Queue time.Duration
	RecoveryItems                                      int
}

// Diagnostics separate time queued from time owning the durable writer.
func (p *TxPool) lockJournalAdmission(items int, timing *journalAdmissionTiming) func() {
	start := time.Now()
	p.journalGate.LockAdmission()
	wait := time.Since(start)
	start = time.Now()
	return func() {
		hold := time.Since(start)
		p.journalGate.Unlock()
		if wait > time.Second || hold > time.Second {
			log.Printf("migration_journal_admission items=%d recovery_items=%d gateWaitMs=%d holdMs=%d prepareUs=%d filterUs=%d persistUs=%d publishUs=%d poolWaitUs=%d enqueueUs=%d", items, timing.RecoveryItems, wait.Milliseconds(), hold.Milliseconds(), timing.Prepare.Microseconds(), timing.Filter.Microseconds(), timing.Persist.Microseconds(), timing.Publish.Microseconds(), timing.PoolWait.Microseconds(), timing.Queue.Microseconds())
		}
	}
}

// Pack transactions for a proposal
func (txpool *TxPool) PackTxs(max_txs uint64) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if txpool.transactionValidation != nil {
		limit := max_txs
		if limit > uint64(len(txpool.TxQueue)) {
			limit = uint64(len(txpool.TxQueue))
		}
		if err := txpool.validateTransactionsLocked(txpool.TxQueue[:limit]); err != nil {
			panic(err)
		}
	}
	if txpool.lanes != nil {
		txpool.completionBypassLocked("migration_lanes")
		return txpool.packLanesLocked(max_txs)
	}
	if txpool.sourcePreparedAccount != "" {
		txpool.completionBypassLocked("source_hold")
		return txpool.packWithSourceHold(max_txs, -1, 0)
	}
	if txpool.completionScheduling != nil {
		return txpool.packCompletionLocked(max_txs)
	}
	txNum := max_txs
	if uint64(len(txpool.TxQueue)) < txNum {
		txNum = uint64(len(txpool.TxQueue))
	}
	txs_Packed := txpool.TxQueue[:txNum]
	txpool.TxQueue = txpool.TxQueue[txNum:]
	return txs_Packed
}

// Pack transaction for a proposal (use 'BlocksizeInBytes' to control)
func (txpool *TxPool) PackTxsWithBytes(max_bytes int) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if txpool.transactionValidation != nil {
		limit := 0
		if max_bytes > 0 {
			limit = max_bytes / int(unsafe.Sizeof(Transaction{}))
		}
		if limit > len(txpool.TxQueue) {
			limit = len(txpool.TxQueue)
		}
		if err := txpool.validateTransactionsLocked(txpool.TxQueue[:limit]); err != nil {
			panic(err)
		}
	}
	if txpool.lanes != nil {
		txpool.completionBypassLocked("migration_lanes")
		if max_bytes < 0 {
			return nil
		}
		return txpool.packLanesLocked(uint64(max_bytes / int(unsafe.Sizeof(Transaction{}))))
	}
	if txpool.sourcePreparedAccount != "" {
		txpool.completionBypassLocked("source_hold")
		if max_bytes < 0 {
			return nil
		}
		return txpool.packWithSourceHold(^uint64(0), max_bytes, int(unsafe.Sizeof(Transaction{})))
	}
	if txpool.completionScheduling != nil {
		limit := uint64(0)
		if max_bytes > 0 {
			limit = uint64(max_bytes / int(unsafe.Sizeof(Transaction{})))
		}
		return txpool.packCompletionLocked(limit)
	}

	txNum := len(txpool.TxQueue)
	currentSize := 0
	for tx_idx, tx := range txpool.TxQueue {
		currentSize += int(unsafe.Sizeof(*tx))
		if currentSize > max_bytes {
			txNum = tx_idx
			break
		}
	}

	txs_Packed := txpool.TxQueue[:txNum]
	txpool.TxQueue = txpool.TxQueue[txNum:]
	return txs_Packed
}

// Relay transactions
func (txpool *TxPool) AddRelayTx(tx *Transaction, shardID uint64) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	_, ok := txpool.RelayPool[shardID]
	if !ok {
		txpool.RelayPool[shardID] = make([]*Transaction, 0)
	}
	txpool.RelayPool[shardID] = append(txpool.RelayPool[shardID], tx)
}

// ReplaceRelayPool publishes one committed block's already classified relay
// groups with a single pool-lock acquisition. The caller relinquishes the map
// after this call. One acquisition avoids repeated contention with other
// queue operations (including ACK/full-snapshot publication).
func (txpool *TxPool) ReplaceRelayPool(groups map[uint64][]*Transaction) {
	if groups == nil {
		groups = make(map[uint64][]*Transaction)
	}
	txpool.lock.Lock()
	txpool.RelayPool = groups
	txpool.lock.Unlock()
}

// txpool get locked
func (txpool *TxPool) GetLocked() {
	txpool.lock.Lock()
}

// txpool get unlocked
func (txpool *TxPool) GetUnlocked() {
	txpool.lock.Unlock()
}

// get the length of tx queue
func (txpool *TxPool) GetTxQueueLen() int {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	return txpool.PendingCountLocked()
}

// get the total pending workload size, including both direct txs and relay txs
func (txpool *TxPool) GetPendingWorkloadCount() int {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	count := txpool.PendingCountLocked()
	for _, shardPool := range txpool.RelayPool {
		count += len(shardPool)
	}
	return count
}

// get the length of ClearRelayPool
func (txpool *TxPool) ClearRelayPool() {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txpool.RelayPool = nil
}

// abort ! Pack relay transactions from relay pool
func (txpool *TxPool) PackRelayTxs(shardID, minRelaySize, maxRelaySize uint64) ([]*Transaction, bool) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if _, ok := txpool.RelayPool[shardID]; !ok {
		return nil, false
	}
	if len(txpool.RelayPool[shardID]) < int(minRelaySize) {
		return nil, false
	}
	txNum := maxRelaySize
	if uint64(len(txpool.RelayPool[shardID])) < txNum {
		txNum = uint64(len(txpool.RelayPool[shardID]))
	}
	relayTxPacked := txpool.RelayPool[shardID][:txNum]
	txpool.RelayPool[shardID] = txpool.RelayPool[shardID][txNum:]
	return relayTxPacked, true
}

// abort ! Transfer transactions when re-sharding
func (txpool *TxPool) TransferTxs(addr utils.Address) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txpool.RequireLegacyQueueLocked()
	txTransfered := make([]*Transaction, 0)
	newTxQueue := make([]*Transaction, 0)
	for _, tx := range txpool.TxQueue {
		if tx.Sender == addr {
			txTransfered = append(txTransfered, tx)
		} else {
			newTxQueue = append(newTxQueue, tx)
		}
	}
	newRelayPool := make(map[uint64][]*Transaction)
	for shardID, shardPool := range txpool.RelayPool {
		for _, tx := range shardPool {
			if tx.Sender == addr {
				txTransfered = append(txTransfered, tx)
			} else {
				if _, ok := newRelayPool[shardID]; !ok {
					newRelayPool[shardID] = make([]*Transaction, 0)
				}
				newRelayPool[shardID] = append(newRelayPool[shardID], tx)
			}
		}
	}
	txpool.TxQueue = newTxQueue
	txpool.RelayPool = newRelayPool
	return txTransfered
}
