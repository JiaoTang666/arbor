package pbft_all

import (
	"blockEmulator/core"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func awaitRetirementAudit(t *testing.T, lane *retirementAuditLane) *retirementAuditResult {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if _, result := lane.poll(); result != nil {
			return result
		}
		select {
		case <-deadline.C:
			t.Fatal("audit worker did not deliver its result")
		case <-tick.C:
		}
	}
}

func TestRetirementAuditLaneSingleFlightAndBinding(t *testing.T) {
	var lane retirementAuditLane
	started, release := make(chan struct{}), make(chan struct{})
	defer lane.close()
	if !lane.start("original", func() retirementAuditResult {
		close(started)
		<-release
		return retirementAuditResult{binding: "worker-must-not-rebind"}
	}) {
		t.Fatal("zero-value lane did not start")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("accepted audit worker did not start")
	}
	if lane.start("second", func() retirementAuditResult { return retirementAuditResult{} }) {
		t.Fatal("admitted a second file audit")
	}
	if busy, result := lane.poll(); !busy || result != nil {
		t.Fatal("unfinished audit was not a nonblocking busy result")
	}
	close(release)
	r := awaitRetirementAudit(t, &lane)
	if r.binding != "original" || r.binding == "current" || r.elapsed <= 0 {
		t.Fatal("result did not preserve its original request binding")
	}
	if busy, result := lane.poll(); busy || result != nil {
		t.Fatal("result delivered more than once")
	}
}

func TestRetirementAuditCloseDoesNotWaitForLateError(t *testing.T) {
	var lane retirementAuditLane
	release := make(chan struct{})
	expected := errors.New("late filesystem failure")
	lane.start("old", func() retirementAuditResult {
		<-release
		return retirementAuditResult{err: expected}
	})
	lane.mu.Lock()
	done := lane.done
	lane.mu.Unlock()
	closed := make(chan struct{})
	go func() { lane.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close waited for filesystem work")
	}
	if lane.start("new", func() retirementAuditResult { return retirementAuditResult{} }) {
		t.Fatal("closed lane accepted new work")
	}
	close(release)
	select {
	case result := <-done:
		if result.err != expected {
			t.Fatal("lost late error")
		}
	case <-time.After(time.Second):
		t.Fatal("late worker blocked sending to abandoned result slot")
	}
	if busy, result := lane.poll(); busy || result != nil {
		t.Fatal("closed lane exposed a late result to consensus")
	}
}

func TestRetirementAuditOwnsIndependentIndex(t *testing.T) {
	n := &PbftConsensusNode{}
	dir := t.TempDir()
	ordinary := n.handoffIndex(dir)
	retirement := n.retirementAudit.indexFor(dir)
	if ordinary == retirement || retirement != n.retirementAudit.indexFor(dir) {
		t.Fatal("audit shares ordinary index lock or recreates cache each poll")
	}
}

func TestRetirementAuditBindingIncludesJournalDirectory(t *testing.T) {
	first, err := retirementAuditBinding(nil, []byte("activation"), "outbox", "replica-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := retirementAuditBinding(nil, []byte("activation"), "outbox", "replica-2")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("audit result can be reused across local replica journals")
	}
}

func TestRetirementAuditFinalFileFreshness(t *testing.T) {
	dir := t.TempDir()
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := func() *retirementAuditFiles {
		t.Helper()
		s, err := readRetirementAuditFiles(dir)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	initial := read()
	write(".temporary-handoff", "incomplete")
	if !initial.same(read()) {
		t.Fatal("temporary publication invalidated final artifacts")
	}
	for _, name := range []string{"a.handoff.json", "b.handoff.json.bundle", "batch-confirmed-c.json", "d.confirmed.json", "e.relay-confirmed.json"} {
		before := read()
		write(name, "x")
		if before.same(read()) {
			t.Fatal("ignored added final artifact", name)
		}
	}
	before := read()
	write("a.handoff.json", "changed-size")
	if before.same(read()) {
		t.Fatal("ignored changed final artifact")
	}
	before = read()
	path := filepath.Join(dir, "a.handoff.json")
	if err := os.Rename(path, filepath.Join(dir, "old.tmp")); err != nil {
		t.Fatal(err)
	}
	write("a.handoff.json", "changed-size")
	if before.same(read()) {
		t.Fatal("ignored inode replacement with equal contents")
	}
	before = read()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if before.same(read()) {
		t.Fatal("ignored removed final artifact")
	}
}

func TestReadRetirementAuditPendingAndInvalidProof(t *testing.T) {
	dir := t.TempDir()
	account := "0000000000000000000000000000000000000010"
	tx := core.NewTransaction(account, account, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen, tx.IngressSequence = true, 1
	h, err := core.BuildMigrationHandoff(tx, "audit", account, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = core.PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	index := core.NewHandoffIndex(dir, 1<<20)
	members := map[string]string{account: "audit"}
	r := readRetirementAudit(index, dir, members)
	if r.err != nil || r.filesChanged || len(r.audits[account].Pending) != 1 || r.audits[account].Confirmed != 0 {
		t.Fatal("persisted work without completion proof was not pending", r)
	}
	if err = os.WriteFile(filepath.Join(dir, h.ID+".confirmed.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	r = readRetirementAudit(index, dir, members)
	if r.filesChanged || r.err == nil {
		t.Fatal("stable but invalid completion proof was accepted", r)
	}
}

func TestRetirementAuditJointConfirmationFreshness(t *testing.T) {
	outbox, journal := t.TempDir(), t.TempDir()
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(journal, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := func() *retirementAuditFiles {
		t.Helper()
		s, err := readRetirementAuditFiles(outbox, journal)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	initial := read()
	write("00000000000000000001.queue.json", `{"Sequence":1,"Kind":"full"}`)
	write(".sef-prepare-incomplete", "partial")
	if !initial.same(read()) {
		t.Fatal("ordinary generations or temporary publication invalidated confirmation facts")
	}
	name := "00000000000000000002.queue.json"
	write(name, `{"Sequence":2,"Kind":"confirm-remove","ConfirmationSchema":"saa_confirm_remove_v2"}`)
	confirmed := read()
	if initial.same(confirmed) {
		t.Fatal("ignored joint confirmation in the separate source journal")
	}
	write("00000000000000000003.queue.json", `{"Sequence":3,"Kind":"full"}`)
	if !confirmed.same(read()) {
		t.Fatal("drain normalization invalidated an unchanged completion audit")
	}
	write(name, `{"Sequence":2,"Kind":"confirm-remove","ConfirmationSchema":"saa_confirm_remove_v2","padding":"changed-size"}`)
	if confirmed.same(read()) {
		t.Fatal("ignored changed joint confirmation")
	}
	before := read()
	path := filepath.Join(journal, name)
	if err := os.Rename(path, filepath.Join(journal, "old.tmp")); err != nil {
		t.Fatal(err)
	}
	write(name, `{"Sequence":2,"Kind":"confirm-remove","ConfirmationSchema":"saa_confirm_remove_v2","padding":"changed-size"}`)
	if before.same(read()) {
		t.Fatal("ignored replacement of a joint confirmation")
	}
	before = read()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if before.same(read()) {
		t.Fatal("ignored removed joint confirmation")
	}
}

func TestReadRetirementAuditReadsSeparateJointConfirmation(t *testing.T) {
	outbox, journal := t.TempDir(), t.TempDir()
	a, b := confirmationMemberA, confirmationMemberB
	tx := core.NewTransaction(a, b, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen, tx.IngressSequence, tx.IngressSequenceAssigned = true, 1, true
	tx.AdmissionID = "native-input/audit/1"
	pool := core.NewTxPool()
	pool.AddTx2Pool(tx)
	if err := pool.EnableGroupMigrationQueueJournal(journal, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	h, err := core.BuildGroupMigrationHandoff(tx, "audit-joint", []string{a, b}, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.PersistMigrationHandoff(outbox, h); err != nil {
		t.Fatal(err)
	}
	index := core.NewHandoffIndex(outbox, 1<<20)
	members := map[string]string{h.Account: h.ProposalID}
	before := readRetirementAudit(index, outbox, members, journal)
	if before.err != nil || len(before.audits[h.Account].Pending) != 1 {
		t.Fatal("unconfirmed work was not pending", before.err)
	}
	if err := pool.ConfirmCompletedHandoffBatch(outbox, []core.CompletedHandoffConfirmation{{H: h, Ack: confirmationAck(h)}}); err != nil {
		t.Fatal(err)
	}
	after := readRetirementAudit(index, outbox, members, journal)
	if after.err != nil || after.filesChanged || len(after.audits[h.Account].Pending) != 0 || after.audits[h.Account].Confirmed != 1 {
		t.Fatal("cached outbox audit omitted the separate durable confirmation", after.err)
	}
	if before.files.same(after.files) {
		t.Fatal("joint confirmation did not invalidate the original file set")
	}
	withoutJournal := readRetirementAudit(index, outbox, members)
	if withoutJournal.err != nil || len(withoutJournal.audits[h.Account].Pending) != 1 {
		t.Fatal("fixture did not require explicit source journal provenance", withoutJournal.err)
	}
}
