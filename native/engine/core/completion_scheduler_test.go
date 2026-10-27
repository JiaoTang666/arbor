package core

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestCompletionCreatorBindingFallsBackWithoutMutatingWire(t *testing.T) {
	first, second := completionTestTx(0, 1, 0, 1), completionTestTx(1, 2, 2, 3)
	second.FirstPlacement = &FirstPlacementBinding{RequestID: "bound-creator"}
	before := second.Encode()
	p := completionTestPool(t, "completion_frontier", 4, 100, first, second)
	completionTestOrder(t, p.PackTxs(1), first)
	if a := p.CompletionSchedulingAudit(); a.FallbackReason != "first_placement_binding" || a.EffectiveMode != "fifo" {
		t.Fatalf("creation binding was reordered: %+v", a)
	}
	if !bytes.Equal(before, second.Encode()) {
		t.Fatal("scheduler changed transaction transport")
	}
	completionTestOrder(t, p.TxQueue, second)
}

func TestCompletionConfigurationFromEnv(t *testing.T) {
	t.Setenv("BLOCK_EMULATOR_COMPLETION_LOOKAHEAD_BLOCKS", "")
	t.Setenv("BLOCK_EMULATOR_COMPLETION_FAIR_EVERY", "")
	for _, mode := range []string{"", "off"} {
		t.Setenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER", mode)
		cfg, err := CompletionSchedulingFromEnv()
		if err != nil || cfg != nil {
			t.Fatalf("disabled env produced scheduler: %+v %v", cfg, err)
		}
	}
	t.Setenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER", "completion_frontier")
	cfg, err := CompletionSchedulingFromEnv()
	if err != nil || cfg.LookaheadBlocks != 4 || cfg.FairEvery != 4 {
		t.Fatalf("bad defaults: %+v %v", cfg, err)
	}
	t.Setenv("BLOCK_EMULATOR_COMPLETION_LOOKAHEAD_BLOCKS", "invalid")
	if _, err := CompletionSchedulingFromEnv(); err == nil {
		t.Fatal("invalid env lookahead accepted")
	}
	t.Setenv("BLOCK_EMULATOR_COMPLETION_LOOKAHEAD_BLOCKS", "4")
	t.Setenv("BLOCK_EMULATOR_COMPLETION_FAIR_EVERY", "0")
	if _, err := CompletionSchedulingFromEnv(); err == nil {
		t.Fatal("zero fairness period accepted")
	}
}

func TestCompletionConcurrentAdmissionPackingAndAudit(t *testing.T) {
	p := completionTestPool(t, "completion_frontier", 4, 4)
	var writers sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		writers.Add(1)
		go func(base int) {
			defer writers.Done()
			for i := 0; i < 100; i++ {
				id := base*100 + i
				p.AddTx2Pool(completionTestTx(id, id%3, 2*id, 2*id+1))
			}
		}(writer)
	}
	done := make(chan struct{})
	go func() { writers.Wait(); close(done) }()
	seen := map[*Transaction]bool{}
	consume := func(out []*Transaction) {
		for _, tx := range out {
			if seen[tx] {
				t.Fatal("concurrent packing repeated an occurrence")
			}
			seen[tx] = true
		}
	}
	for {
		consume(p.PackTxs(7))
		_ = p.CompletionSchedulingAudit()
		select {
		case <-done:
			for p.GetTxQueueLen() > 0 {
				consume(p.PackTxs(7))
			}
			if len(seen) != 400 {
				t.Fatalf("concurrent packing lost work: %d/400", len(seen))
			}
			return
		default:
		}
	}
}

// These contracts are about queue occurrences and legal local order, not
// successful state execution, remote completion, or a global nonce sequence.
func completionTestAddress(i int) string { return fmt.Sprintf("%040x", i+1) }

func completionTestTx(i, stage, sender, recipient int) *Transaction {
	tx := NewTransaction(completionTestAddress(sender), completionTestAddress(recipient), big.NewInt(1), uint64(i), time.Unix(1, 0))
	tx.AdmissionID = fmt.Sprintf("occurrence-%d", i)
	tx.IngressSequence = uint64(1000000 + 13*i)
	tx.IngressSequenceAssigned = true
	tx.RoutingFrozen = true
	switch stage {
	case 0:
		tx.RoutedSenderShard, tx.RoutedRecipientShard = 0, 0
	case 1:
		tx.RoutedSenderShard, tx.RoutedRecipientShard = 0, 1
	case 2:
		tx.RoutedSenderShard, tx.RoutedRecipientShard = 1, 0
		tx.Relayed = true
	default:
		panic("invalid test stage")
	}
	return tx
}

func completionTestPool(t *testing.T, mode string, lookahead int, fairEvery uint64, txs ...*Transaction) *TxPool {
	t.Helper()
	p := NewTxPool()
	if err := p.EnableCompletionScheduling(CompletionScheduleConfig{Mode: mode, LookaheadBlocks: lookahead, FairEvery: fairEvery}, 0, 2); err != nil {
		t.Fatal(err)
	}
	p.AddTxs2Pool(txs)
	return p
}

func completionTestOrder(t *testing.T, actual []*Transaction, expected ...*Transaction) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("length = %d, want %d", len(actual), len(expected))
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Fatalf("position %d = %p, want occurrence %p", i, actual[i], expected[i])
		}
	}
}

func TestCompletionDefaultAndExplicitFIFOCompatibility(t *testing.T) {
	first, inner, second := completionTestTx(0, 1, 0, 1), completionTestTx(1, 0, 2, 3), completionTestTx(2, 2, 4, 5)
	for _, configured := range []bool{false, true} {
		p := NewTxPool()
		if configured {
			if err := p.EnableCompletionScheduling(CompletionScheduleConfig{"fifo", 4, 4}, 0, 2); err != nil {
				t.Fatal(err)
			}
		}
		p.AddTxs2Pool([]*Transaction{first, inner, second})
		completionTestOrder(t, p.PackTxs(2), first, inner)
		completionTestOrder(t, p.TxQueue, second)
		if !configured && p.CompletionSchedulingAudit().Mode != "off" {
			t.Fatal("unconfigured scheduler is not reported off")
		}
	}
}

func TestCompletionPhasePrefersIndependentFinishingStages(t *testing.T) {
	first, inner, second := completionTestTx(0, 1, 0, 1), completionTestTx(1, 0, 2, 3), completionTestTx(2, 2, 4, 5)
	p := completionTestPool(t, "phase_priority", 4, 1000, first, inner, second)
	completionTestOrder(t, p.PackTxs(2), inner, second)
	completionTestOrder(t, p.TxQueue, first)
	a := p.CompletionSchedulingAudit()
	if a.Selected.Inner != 1 || a.Selected.Relay2 != 1 || a.Selected.Relay1 != 0 || a.SelectedCount != 2 {
		t.Fatalf("incorrect selected-stage audit: %+v", a)
	}
}

func TestCompletionCannotBypassAccountPredecessor(t *testing.T) {
	first, second := completionTestTx(0, 1, 0, 1), completionTestTx(1, 2, 2, 0)
	for _, mode := range []string{"phase_priority", "completion_frontier"} {
		p := completionTestPool(t, mode, 4, 1000, first, second)
		completionTestOrder(t, p.PackTxs(1), first)
		if a := p.CompletionSchedulingAudit(); a.InitiallyBlocked != 1 || a.InitiallyReady.Relay2 != 0 {
			t.Fatalf("blocked completion was reported ready: %+v", a)
		}
		completionTestOrder(t, p.PackTxs(1), second)
	}
}

func TestCompletionUnlocksSuccessorWithinSameBlock(t *testing.T) {
	first, second, unrelated := completionTestTx(0, 1, 0, 1), completionTestTx(1, 2, 2, 0), completionTestTx(2, 1, 3, 4)
	p := completionTestPool(t, "phase_priority", 4, 1000, first, second, unrelated)
	completionTestOrder(t, p.PackTxs(2), first, second)
	completionTestOrder(t, p.TxQueue, unrelated)
}

func TestCompletionFrontierChoosesLegalUnlockInsteadOfPhaseTie(t *testing.T) {
	f0, f1, c2 := completionTestTx(0, 1, 0, 1), completionTestTx(1, 1, 2, 3), completionTestTx(2, 2, 4, 2)
	phase := completionTestPool(t, "phase_priority", 4, 1000, f0, f1, c2)
	frontier := completionTestPool(t, "completion_frontier", 4, 1000, f0, f1, c2)
	completionTestOrder(t, phase.PackTxs(1), f0)
	completionTestOrder(t, frontier.PackTxs(1), f1)
}

func TestCompletionThreePolicyReplayFinishesOneLocalTerminalEarlier(t *testing.T) {
	// This finite selector replay is not native performance evidence. F0 and
	// F1 create remote work; only C2 is a terminal stage in this local fixture.
	f0, f1, c2 := completionTestTx(0, 1, 0, 1), completionTestTx(1, 1, 2, 3), completionTestTx(2, 2, 4, 2)
	for _, tc := range []struct {
		mode         string
		order        []*Transaction
		terminalPack int
	}{
		{"fifo", []*Transaction{f0, f1, c2}, 3},
		{"phase_priority", []*Transaction{f0, f1, c2}, 3},
		{"completion_frontier", []*Transaction{f1, c2, f0}, 2},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			p := completionTestPool(t, tc.mode, 4, 100, f0, f1, c2)
			terminalPack := 0
			for i, expected := range tc.order {
				out := p.PackTxs(1)
				completionTestOrder(t, out, expected)
				if out[0] == c2 {
					terminalPack = i + 1
				}
			}
			if terminalPack != tc.terminalPack {
				t.Fatalf("terminal selected at pack %d, want %d", terminalPack, tc.terminalPack)
			}
			t.Logf("mode=%s local_terminal_pack=%d", tc.mode, terminalPack)
		})
	}
}

func TestCompletionFrontierReweightsRemainingJoinPrerequisite(t *testing.T) {
	// Joint completion needs both A and X. Choosing A's earlier inner stage
	// increases X's unlock share from 1/2 to 1, ahead of the later independent
	// inner stage. A's B-successor makes the first selection unambiguous.
	a := completionTestTx(0, 0, 0, 1)
	x := completionTestTx(1, 1, 2, 3)
	independent := completionTestTx(2, 0, 4, 5)
	b := completionTestTx(3, 2, 6, 1)
	join := completionTestTx(4, 0, 0, 2)
	p := completionTestPool(t, "completion_frontier", 4, 1000, a, x, independent, b, join)
	completionTestOrder(t, p.PackTxs(2), a, x)
	if p.CompletionSchedulingAudit().InitiallyBlocked != 2 {
		t.Fatal("join/local successor dependencies were not counted")
	}
}

func TestCompletionStableResidualPrefixAndUntouchedSuffix(t *testing.T) {
	f0, c1, f2, c3, f4, c5 := completionTestTx(0, 1, 0, 1), completionTestTx(1, 0, 2, 3), completionTestTx(2, 1, 4, 5), completionTestTx(3, 2, 6, 7), completionTestTx(4, 1, 8, 9), completionTestTx(5, 0, 10, 11)
	p := completionTestPool(t, "phase_priority", 2, 1000, f0, c1, f2, c3, f4, c5)
	completionTestOrder(t, p.PackTxs(2), c1, c3)
	completionTestOrder(t, p.TxQueue, f0, f2, f4, c5)
	if p.CompletionSchedulingAudit().Examined != 4 {
		t.Fatal("selector examined beyond bounded prefix")
	}
}

func TestCompletionDoesNotSearchOutsideLookahead(t *testing.T) {
	f0, f1, c2 := completionTestTx(0, 1, 0, 1), completionTestTx(1, 1, 2, 3), completionTestTx(2, 0, 4, 5)
	p := completionTestPool(t, "phase_priority", 2, 1000, f0, f1, c2)
	completionTestOrder(t, p.PackTxs(1), f0)
	completionTestOrder(t, p.TxQueue, f1, c2)
	if p.CompletionSchedulingAudit().Examined != 2 {
		t.Fatal("bounded prefix changed")
	}
}

func TestCompletionCanonicalAliasesShareDependency(t *testing.T) {
	first, second := completionTestTx(0, 1, 10, 1), completionTestTx(1, 2, 2, 10)
	second.Recipient = " 0X" + strings.ToUpper(second.Recipient) + " "
	p := completionTestPool(t, "phase_priority", 4, 1000, first, second)
	completionTestOrder(t, p.PackTxs(1), first)
	if p.CompletionSchedulingAudit().InitiallyBlocked != 1 {
		t.Fatal("canonical aliases were treated as independent accounts")
	}
}

func TestCompletionFootprintsExcludeNonlocalEffects(t *testing.T) {
	t.Run("relay2-does-not-write-sender", func(t *testing.T) {
		first := completionTestTx(0, 1, 0, 1)
		second := completionTestTx(1, 2, 0, 2)
		p := completionTestPool(t, "phase_priority", 4, 1000, first, second)
		completionTestOrder(t, p.PackTxs(1), second)
	})
	t.Run("relay1-does-not-write-remote-recipient", func(t *testing.T) {
		first := completionTestTx(0, 1, 0, 1)
		inner := completionTestTx(1, 0, 1, 2)
		p := completionTestPool(t, "phase_priority", 4, 1000, first, inner)
		completionTestOrder(t, p.PackTxs(1), inner)
	})
}

func TestCompletionSameAddressDoesNotDoubleDependency(t *testing.T) {
	first, self := completionTestTx(0, 1, 0, 1), completionTestTx(1, 0, 0, 0)
	p := completionTestPool(t, "completion_frontier", 4, 1000, first, self)
	completionTestOrder(t, p.PackTxs(2), first, self)
	if p.GetTxQueueLen() != 0 {
		t.Fatal("same-address transaction left phantom work")
	}
}

func TestCompletionUnsupportedPrefixFallsBackWithoutLoss(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Transaction)
	}{
		{"unfrozen", func(tx *Transaction) { tx.RoutingFrozen = false }},
		{"broker", func(tx *Transaction) { tx.HasBroker = true }},
		{"broker-sender", func(tx *Transaction) { tx.SenderIsBroker = true }},
		{"original-sender", func(tx *Transaction) { tx.OriginalSender = completionTestAddress(9) }},
		{"final-recipient", func(tx *Transaction) { tx.FinalRecipient = completionTestAddress(9) }},
		{"invalid-route", func(tx *Transaction) { tx.RoutedRecipientShard = 2 }},
		{"nonlocal", func(tx *Transaction) { tx.RoutedSenderShard = 1 }},
		{"invalid-relay-stage", func(tx *Transaction) { tx.Relayed = true }},
		{"missing-account", func(tx *Transaction) { tx.Sender = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, invalid, complete := completionTestTx(0, 1, 0, 1), completionTestTx(1, 1, 2, 3), completionTestTx(2, 0, 4, 5)
			tc.modify(invalid)
			p := completionTestPool(t, "completion_frontier", 4, 1000, first, invalid, complete)
			completionTestOrder(t, p.PackTxs(1), first)
			completionTestOrder(t, p.TxQueue, invalid, complete)
			a := p.CompletionSchedulingAudit()
			if a.EffectiveMode != "fifo" || a.FallbackReason == "" || a.Prefix.Unsupported != 1 {
				t.Fatalf("unsupported input was not auditable FIFO fallback: %+v", a)
			}
		})
	}
	t.Run("nil-corrupt-entry", func(t *testing.T) {
		first, complete := completionTestTx(0, 1, 0, 1), completionTestTx(2, 0, 4, 5)
		p := completionTestPool(t, "completion_frontier", 4, 1000)
		// Ordinary admission already rejects nil; model an invalid restored
		// internal queue here without weakening that admission boundary.
		p.TxQueue = []*Transaction{first, nil, complete}
		completionTestOrder(t, p.PackTxs(1), first)
		completionTestOrder(t, p.TxQueue, nil, complete)
		if a := p.CompletionSchedulingAudit(); a.EffectiveMode != "fifo" || a.FallbackReason != "nil_transaction" {
			t.Fatalf("nil restored queue entry was not explicit fallback: %+v", a)
		}
	})
}

func TestCompletionCapacityAndBytesBoundaries(t *testing.T) {
	first, inner := completionTestTx(0, 1, 0, 1), completionTestTx(1, 0, 2, 3)
	t.Run("empty", func(t *testing.T) {
		p := completionTestPool(t, "completion_frontier", 64, 1000)
		completionTestOrder(t, p.PackTxs(^uint64(0)))
	})
	t.Run("zero-and-huge-count", func(t *testing.T) {
		p := completionTestPool(t, "phase_priority", 64, 1000, first, inner)
		completionTestOrder(t, p.PackTxs(0))
		completionTestOrder(t, p.TxQueue, first, inner)
		completionTestOrder(t, p.PackTxs(^uint64(0)), inner, first)
	})
	t.Run("bytes", func(t *testing.T) {
		p := completionTestPool(t, "phase_priority", 4, 1000, first, inner)
		size := int(unsafe.Sizeof(Transaction{}))
		for _, limit := range []int{-1, 0, size - 1} {
			completionTestOrder(t, p.PackTxsWithBytes(limit))
			completionTestOrder(t, p.TxQueue, first, inner)
		}
		completionTestOrder(t, p.PackTxsWithBytes(size), inner)
		completionTestOrder(t, p.TxQueue, first)
	})
}

func TestCompletionConfigurationValidation(t *testing.T) {
	for _, cfg := range []CompletionScheduleConfig{
		{"unknown", 4, 4}, {"phase_priority", 0, 4}, {"completion_frontier", -1, 4},
		{"phase_priority", 65, 4}, {"completion_frontier", 4, 0},
	} {
		p := NewTxPool()
		if err := p.EnableCompletionScheduling(cfg, 0, 2); err == nil {
			t.Fatalf("accepted invalid config: %+v", cfg)
		}
	}
	for _, topology := range [][2]uint64{{0, 0}, {2, 2}} {
		if err := NewTxPool().EnableCompletionScheduling(CompletionScheduleConfig{"phase_priority", 4, 4}, topology[0], topology[1]); err == nil {
			t.Fatal("accepted invalid shard topology")
		}
	}
	p := completionTestPool(t, "phase_priority", 4, 4)
	if err := p.EnableCompletionScheduling(CompletionScheduleConfig{"fifo", 4, 4}, 0, 2); err == nil {
		t.Fatal("runtime replacement silently changed startup-only scheduler")
	}
}

func TestCompletionFairnessRoundUsesWholeBlockFIFO(t *testing.T) {
	p := completionTestPool(t, "completion_frontier", 4, 3)
	for call := 1; call <= 3; call++ {
		first := completionTestTx(call*10, 1, call*4, call*4+1)
		inner := completionTestTx(call*10+1, 0, call*4+2, call*4+3)
		p.AddTxs2Pool([]*Transaction{first, inner})
		if call < 3 {
			completionTestOrder(t, p.PackTxs(2), inner, first)
		} else {
			completionTestOrder(t, p.PackTxs(2), first, inner)
			if a := p.CompletionSchedulingAudit(); !a.FairFIFO || a.EffectiveMode != "fifo" || a.PackCall != 3 {
				t.Fatalf("fair FIFO round not recorded: %+v", a)
			}
		}
	}
}

func TestCompletionFairnessAdvancesOldFirstLegDespiteNewCompletions(t *testing.T) {
	old := completionTestTx(0, 1, 0, 1)
	p := completionTestPool(t, "completion_frontier", 4, 4, old)
	for call := 1; call <= 4; call++ {
		fresh := completionTestTx(call, 0, 2*call+10, 2*call+11)
		p.AddTx2Pool(fresh)
		out := p.PackTxs(1)
		if call < 4 {
			completionTestOrder(t, out, fresh)
		} else {
			completionTestOrder(t, out, old)
			if !p.CompletionSchedulingAudit().FairFIFO {
				t.Fatal("old first leg advanced without auditable FIFO fairness round")
			}
		}
	}
}

func TestCompletionHeadInsertionUsesCurrentQueueOrderNotGlobalSequence(t *testing.T) {
	old := completionTestTx(0, 2, 2, 0)
	head := completionTestTx(1, 1, 0, 1)
	head.IngressSequence, old.IngressSequence = 99999999, 1
	head.Nonce, old.Nonce = 700, 0
	p := completionTestPool(t, "phase_priority", 4, 1000, old)
	p.AddTxs2Pool_Head([]*Transaction{head})
	completionTestOrder(t, p.PackTxs(1), head)
	completionTestOrder(t, p.PackTxs(1), old)
}

func TestCompletionMigrationAndSourceHoldKeepLegacySelection(t *testing.T) {
	t.Run("source-hold", func(t *testing.T) {
		held, first, complete := completionTestTx(0, 1, 0, 1), completionTestTx(1, 1, 2, 3), completionTestTx(2, 0, 4, 5)
		p := completionTestPool(t, "phase_priority", 4, 1000, held, first, complete)
		p.HoldSourceAccount(held.Sender)
		completionTestOrder(t, p.PackTxs(1), first)
		if a := p.CompletionSchedulingAudit(); a.EffectiveMode != "legacy_migration" || a.FallbackReason == "" {
			t.Fatalf("source hold bypass was not explicit: %+v", a)
		}
	})
	t.Run("migration-lanes", func(t *testing.T) {
		p := NewTxPool()
		if err := p.EnableMigrationQueueJournal(t.TempDir(), completionTestAddress(99)); err != nil {
			t.Fatal(err)
		}
		if err := p.EnableCompletionScheduling(CompletionScheduleConfig{"completion_frontier", 4, 1000}, 0, 2); err != nil {
			t.Fatal(err)
		}
		first, complete := completionTestTx(0, 1, 0, 1), completionTestTx(1, 0, 2, 3)
		p.AddTxs2Pool([]*Transaction{first, complete})
		completionTestOrder(t, p.PackTxs(1), first)
		if a := p.CompletionSchedulingAudit(); a.EffectiveMode != "legacy_migration" || a.FallbackReason == "" {
			t.Fatalf("journal lane bypass was not explicit: %+v", a)
		}
	})
}

func TestCompletionRandomQueuesPreserveLocalOrderAndOccurrences(t *testing.T) {
	rng := rand.New(rand.NewSource(43))
	for sample := 0; sample < 500; sample++ {
		mode := "phase_priority"
		if sample%2 == 0 {
			mode = "completion_frontier"
		}
		p := completionTestPool(t, mode, 1+rng.Intn(8), uint64(2+rng.Intn(7)))
		n := 1 + rng.Intn(80)
		original := make([]*Transaction, n)
		perAccount := make(map[string][]*Transaction)
		for i := range original {
			tx := completionTestTx(sample*100+i, rng.Intn(3), rng.Intn(12), rng.Intn(12))
			// Equal hashes are deliberately distinct admission occurrences.
			tx.TxHash = []byte("same-hash-does-not-mean-same-queue-occurrence")
			original[i] = tx
			accounts := []string{tx.Sender}
			if tx.Relayed {
				accounts = []string{tx.Recipient}
			} else if tx.RoutedRecipientShard == 0 && tx.Recipient != tx.Sender {
				accounts = append(accounts, tx.Recipient)
			}
			for _, account := range accounts {
				perAccount[account] = append(perAccount[account], tx)
			}
		}
		p.AddTxs2Pool(original)
		seen := make(map[*Transaction]bool, n)
		position := make(map[*Transaction]int, n)
		for p.GetTxQueueLen() > 0 {
			out := p.PackTxs(uint64(1 + rng.Intn(10)))
			if len(out) == 0 {
				t.Fatalf("sample %d failed to advance legal nonempty queue", sample)
			}
			for _, tx := range out {
				if seen[tx] {
					t.Fatalf("sample %d repeated occurrence", sample)
				}
				position[tx] = len(seen)
				seen[tx] = true
			}
		}
		if len(seen) != len(original) {
			t.Fatalf("sample %d lost occurrences: %d/%d", sample, len(seen), len(original))
		}
		for account, ordered := range perAccount {
			for i := 1; i < len(ordered); i++ {
				if position[ordered[i-1]] >= position[ordered[i]] {
					t.Fatalf("sample %d reversed local dependency for %s", sample, account)
				}
			}
		}
	}
}
