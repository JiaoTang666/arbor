package signal

import (
	"testing"
	"time"
)

func TestUntouchedShardBlocksLegacyCloseout(t *testing.T) {
	s := NewStopSignal(48, 16)
	for i := 0; i < 16; i++ {
		if i != 11 {
			s.ObserveBlockInfo(uint64(i), 1)
			s.ObserveBlockInfo(uint64(i), 0)
		}
	}
	s.lastMeaningfulActivity = time.Now().Add(-time.Hour)
	if s.GapEnough() {
		t.Fatal("legacy path must still require all shards")
	}
	if len(s.shardSeen) != 15 {
		t.Fatal("fixture must reproduce missing shard 11")
	}
}

func TestUntouchedProofKeepsQuietAndDoesNotFakeBlock(t *testing.T) {
	s := NewStopSignal(48, 16)
	for i := 0; i < 16; i++ {
		if i != 11 {
			s.ObserveBlockInfo(uint64(i), 1)
			s.ObserveBlockInfo(uint64(i), 0)
		}
	}
	if e := s.ConfirmUntouchedAfterDrainedInput([]uint64{11}); e != nil {
		t.Fatal(e)
	}
	if s.GapEnough() {
		t.Fatal("quiet period bypassed")
	}
	if s.shardSeen[11] || s.shardEmptyStreak[11] != 0 {
		t.Fatal("fabricated block")
	}
	s.lastMeaningfulActivity = time.Now().Add(-time.Hour)
	if !s.GapEnough() {
		t.Fatal("fully drained run cannot close")
	}
	s.ObserveBlockInfo(11, 1)
	if s.untouchedAfterDrain[11] || s.GapEnough() {
		t.Fatal("late work must revoke and reset")
	}
}
func TestUntouchedProofRejectsPendingAndBadSetAtomically(t *testing.T) {
	s := NewStopSignal(48, 16)
	s.ObserveSecondLegProgress(1, 0)
	if s.ConfirmUntouchedAfterDrainedInput([]uint64{11}) == nil {
		t.Fatal("pending second leg")
	}
	s.ObserveSecondLegProgress(0, 1)
	for _, bad := range [][]uint64{{11, 16}, {11, 11}} {
		if s.ConfirmUntouchedAfterDrainedInput(bad) == nil {
			t.Fatal("invalid set")
		}
		if len(s.untouchedAfterDrain) != 0 {
			t.Fatal("partial proof write")
		}
	}
}
