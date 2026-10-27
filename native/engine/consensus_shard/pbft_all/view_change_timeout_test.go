package pbft_all

import (
	"blockEmulator/params"
	"testing"
	"time"
)

func TestViewChangeDropsTimeoutAfterCommit(t *testing.T) {
	old := params.PbftViewChangeTimeOut
	params.PbftViewChangeTimeOut = 20000
	defer func() { params.PbftViewChangeTimeOut = old }()
	p := &PbftConsensusNode{node_nums: 4}
	p.hasCommittedBlock.Store(true)
	p.pbftStage.Store(1)
	p.lastCommitTime.Store(time.Now().Add(-30 * time.Second).UnixMilli())
	if !p.viewChangeTimeoutDue(time.Now()) {
		t.Fatal("expired progress must qualify")
	}
	p.pbftLock.Lock()
	started, done := make(chan struct{}), make(chan struct{})
	go func() { close(started); p.viewChangePropose(); close(done) }()
	<-started
	// Model successful commit while the timeout request cannot acquire its lock.
	p.sequenceID = 85
	p.lastCommitTime.Store(time.Now().UnixMilli())
	p.pbftLock.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout request did not return")
	}
	if p.pbftStage.Load() != 1 || p.sequenceID != 85 {
		t.Fatal("stale timeout altered fresh committed state")
	}
}

func TestViewChangeTimeoutBoundaries(t *testing.T) {
	old := params.PbftViewChangeTimeOut
	params.PbftViewChangeTimeOut = 20000
	defer func() { params.PbftViewChangeTimeOut = old }()
	now := time.Unix(1000, 0)
	p := &PbftConsensusNode{node_nums: 4}
	p.hasCommittedBlock.Store(true)
	for _, c := range []struct {
		age  int64
		want bool
	}{{0, false}, {19999, false}, {20000, false}, {20001, true}, {30000, true}} {
		p.lastCommitTime.Store(now.UnixMilli() - c.age)
		if p.viewChangeTimeoutDue(now) != c.want {
			t.Fatalf("age %d", c.age)
		}
	}
	p.hasCommittedBlock.Store(false)
	if p.viewChangeTimeoutDue(now) {
		t.Fatal("startup cannot time out")
	}
	p.hasCommittedBlock.Store(true)
	p.node_nums = 1
	if p.viewChangeTimeoutDue(now) {
		t.Fatal("single node cannot fail over")
	}
}
