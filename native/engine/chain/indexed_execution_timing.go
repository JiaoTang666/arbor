package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"fmt"
	"math/big"
	"time"
)

const (
	execIdentity = iota
	execStateRead
	execStateDecode
	execStateWrite
	execEffectLookup
	execEffectRecord
	execFirstOwner
	execPhaseCount
)

// Block-local diagnostics, not a state cache. The effect I/O subtotals are
// INCLUDED in effectLookup/effectRecord; they must not be added a second time.
type indexedExecutionTiming struct {
	phases                                        [execPhaseCount]time.Duration
	effectRead, effectWrite                       time.Duration
	transactions, applied, stateReads             int
	effectReads, effectWrites                     int
	stateBytes, effectReadBytes, effectWriteBytes int64
}

func executionTick(t *indexedExecutionTiming) time.Time {
	if t == nil {
		return time.Time{}
	}
	return time.Now()
}

func (t *indexedExecutionTiming) add(phase int, started time.Time) {
	if t != nil {
		t.phases[phase] += time.Since(started)
	}
}

// One immediate read supplies BOTH account existence and decoded state. There
// is no reuse across effects: a self-transfer credit reads the updated debit.
func readExecutionAccount(st EffectIndexTrie, address string, mustExist bool, t *indexedExecutionTiming) (*core.AccountState, bool, error) {
	started := executionTick(t)
	raw, err := st.Get([]byte(address))
	t.add(execStateRead, started)
	if t != nil {
		t.stateReads++
		t.stateBytes += int64(len(raw))
	}
	if err != nil {
		return nil, false, err
	}
	if len(raw) == 0 && mustExist {
		return nil, false, fmt.Errorf("required batch account absent: %s", address)
	}
	started = executionTick(t)
	if len(raw) > 0 {
		state := core.DecodeAS(raw)
		t.add(execStateDecode, started)
		return state, true, nil
	}
	state := &core.AccountState{Balance: new(big.Int).Set(params.Init_Balance)}
	t.add(execStateDecode, started)
	return state, false, nil
}

type observedEffectTrie struct {
	EffectIndexTrie
	timing *indexedExecutionTiming
}

func (st observedEffectTrie) Get(key []byte) ([]byte, error) {
	started := time.Now()
	raw, err := st.EffectIndexTrie.Get(key)
	st.timing.effectRead += time.Since(started)
	st.timing.effectReads++
	st.timing.effectReadBytes += int64(len(raw))
	return raw, err
}

func (st observedEffectTrie) Update(key, raw []byte) error {
	started := time.Now()
	err := st.EffectIndexTrie.Update(key, raw)
	st.timing.effectWrite += time.Since(started)
	st.timing.effectWrites++
	st.timing.effectWriteBytes += int64(len(raw))
	return err
}

func (t *indexedExecutionTiming) report(shard, height uint64, execution time.Duration) {
	if t.transactions == 0 {
		return
	}
	p := t.phases
	accounted := time.Duration(0)
	for _, elapsed := range p {
		accounted += elapsed
	}
	fmt.Printf("block_execute_detail shard=%d height=%d txs=%d effects=%d identityUs=%d stateReadUs=%d stateDecodeUs=%d stateWriteUs=%d effectLookupUs=%d effectRecordUs=%d firstOwnerUs=%d effectReadUs=%d effectWriteUs=%d stateReads=%d stateBytes=%d effectReads=%d effectReadBytes=%d effectWrites=%d effectWriteBytes=%d otherUs=%d executeUs=%d\n",
		shard, height, t.transactions, t.applied, p[execIdentity].Microseconds(),
		p[execStateRead].Microseconds(), p[execStateDecode].Microseconds(), p[execStateWrite].Microseconds(),
		p[execEffectLookup].Microseconds(), p[execEffectRecord].Microseconds(), p[execFirstOwner].Microseconds(),
		t.effectRead.Microseconds(), t.effectWrite.Microseconds(), t.stateReads, t.stateBytes,
		t.effectReads, t.effectReadBytes, t.effectWrites, t.effectWriteBytes,
		(execution - accounted).Microseconds(), execution.Microseconds())
}
