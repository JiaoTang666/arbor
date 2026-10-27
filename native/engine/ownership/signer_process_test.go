package ownership

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// These are real CLI processes, but trusted local IPC, one host and test keys
// are NOT authenticated networking, independent fault domains or PBFT.
type processCommittee struct {
	t                                   *testing.T
	fixture                             *committeeFixture
	dir, binary, ledgerHash, configHash string
	args                                [4][]string
	members                             [4]*memberProcess
	evidence                            map[string]interface{}
	transcript                          []map[string]interface{}
}

type memberProcess struct {
	command *exec.Cmd
	input   io.WriteCloser
	encoder *json.Encoder
	decoder *json.Decoder
}

func (p *memberProcess) stop() {
	if p == nil {
		return
	}
	p.input.Close()
	done := make(chan error, 1)
	go func() { done <- p.command.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		p.command.Process.Kill()
		<-done
	}
}

// Kill AFTER a published durable vote, without running the member's Close.
// This is a process crash, not a physical power-loss simulation.
func (p *memberProcess) crash(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal("member crash injection failed")
	}
	p.command.Wait()
	p.input.Close()
}

func newProcessCommittee(t *testing.T, binary string) *processCommittee {
	t.Helper()
	c := &processCommittee{t: t, fixture: newCommitteeFixture(t), dir: t.TempDir(), binary: binary}
	ledger := map[string]interface{}{"schema": "synthetic_initial_ledger_v1", "shards": 2,
		"accounts": []map[string]interface{}{{"account": testAccount(1000), "balance": "100", "nonce": 0, "storage_root": "", "code_hash": "", "shard": 0}}}
	c.ledgerHash = writePinnedFixture(t, filepath.Join(c.dir, "ledger.json"), ledger)
	c.fixture.config.Context.GenesisDigest = c.ledgerHash
	var err error
	c.fixture.verifier, err = NewVerifier(c.fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	c.configHash = writePinnedFixture(t, filepath.Join(c.dir, "committee.json"), c.fixture.config)
	c.evidence = map[string]interface{}{"schema": "coolchain/committee-process-acceptance-v1", "config": c.fixture.config,
		"ledger_sha256": c.ledgerHash, "config_sha256": c.configHash, "shards": 2,
		"transport": "trusted-local-IPC", "pbft_commit": "NOT_IMPLEMENTED", "native_workload": "NOT_RUN", "private_keys_in_report": false}
	for i, member := range c.fixture.config.Members {
		keyPath := filepath.Join(c.dir, member.ID+".key")
		if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(c.fixture.private[member.ID])), 0600); err != nil {
			t.Fatal("cannot write temporary member key")
		}
		c.args[i] = []string{"-genesis", filepath.Join(c.dir, "ledger.json"), "-genesis-sha256", c.ledgerHash,
			"-config", filepath.Join(c.dir, "committee.json"), "-config-sha256", c.configHash, "-shards", "2",
			"-member", member.ID, "-key", keyPath, "-store", filepath.Join(c.dir, member.ID+".db")}
	}
	t.Cleanup(func() {
		for _, member := range c.members {
			member.stop()
		}
	})
	return c
}

func (c *processCommittee) start(i int) {
	c.t.Helper()
	command := exec.Command(c.binary, c.args[i]...)
	input, err := command.StdinPipe()
	if err != nil {
		c.t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		input.Close()
		c.t.Fatal(err)
	}
	// Do not copy subprocess errors or command arguments into public evidence.
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		input.Close()
		c.t.Fatal("member process could not start")
	}
	c.members[i] = &memberProcess{command: command, input: input, encoder: json.NewEncoder(input), decoder: json.NewDecoder(output)}
	c.ask(i, MemberRequest{Action: "status"}, true)
}

func (c *processCommittee) ask(i int, request MemberRequest, pass bool) MemberResponse {
	c.t.Helper()
	p := c.members[i]
	if err := p.encoder.Encode(request); err != nil {
		c.t.Fatal("member request write failed")
	}
	var response MemberResponse
	done := make(chan error, 1)
	go func() { done <- p.decoder.Decode(&response) }()
	select {
	case err := <-done:
		if err != nil {
			c.t.Fatal("member response unavailable")
		}
	case <-time.After(5 * time.Second):
		p.command.Process.Kill()
		<-done
		c.t.Fatal("member response timed out")
	}
	c.transcript = append(c.transcript, map[string]interface{}{"member": i, "pid": p.command.Process.Pid, "request": request, "response": response})
	if (response.Status == "PASS") != pass {
		c.t.Fatalf("member %d action %s: expected pass=%v, got %s (%s)", i, request.Action, pass, response.Status, response.Error)
	}
	if !pass && response.Signature != nil {
		c.t.Fatal("rejected request published a signature")
	}
	return response
}

func (c *processCommittee) vote(i int, batch Batch, pass bool) Signature {
	response := c.ask(i, MemberRequest{Action: "vote", Batch: &batch}, pass)
	if !pass {
		return Signature{}
	}
	if response.Signature == nil {
		c.t.Fatal("successful vote has no signature")
	}
	return *response.Signature
}

func (c *processCommittee) batch(base uint64, id string, account, target uint64) Batch {
	return Batch{Schema: Schema, Context: c.fixture.config.Context, CommitteeDigest: c.fixture.verifier.CommitteeDigest(),
		BaseVersion: base, ProposalID: id, Bindings: []Binding{{testAccount(account), target}}}
}

// The intended payload is durably fixed BEFORE asking any member to vote.
// Closing and re-reading ensures no later request is built from a mutated plan.
func (c *processCommittee) plan(batches ...Batch) []Batch {
	c.t.Helper()
	path := filepath.Join(c.dir, "plan.json")
	raw, err := json.Marshal(batches)
	if err != nil {
		c.t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		c.t.Fatal(err)
	}
	_, writeErr := file.Write(raw)
	syncErr, closeErr := file.Sync(), file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		c.t.Fatal("plan persistence failed")
	}
	directory, err := os.Open(c.dir)
	if err != nil {
		c.t.Fatal(err)
	}
	syncErr, closeErr = directory.Sync(), directory.Close()
	if syncErr != nil || closeErr != nil {
		c.t.Fatal("plan directory persistence failed")
	}
	readBack, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(raw, readBack) {
		c.t.Fatal("persisted plan changed")
	}
	var reopened []Batch
	if err := json.Unmarshal(readBack, &reopened); err != nil {
		c.t.Fatal(err)
	}
	c.evidence["plan"] = reopened
	c.evidence["plan_sha256"] = digestBytes(readBack)
	c.evidence["plan_synced_before_vote"] = true
	return reopened
}

func (c *processCommittee) report(name string) {
	c.evidence["status"] = "PASS"
	if c.t.Failed() {
		c.evidence["status"] = "FAIL"
	}
	c.evidence["transcript"] = c.transcript
	directory := os.Getenv("OWNERSHIP_PROCESS_REPORT_DIR")
	if directory == "" {
		return
	}
	if !filepath.IsAbs(directory) {
		c.t.Error("process report destination must be absolute")
		return
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		c.t.Error(err)
		return
	}
	file, err := os.OpenFile(filepath.Join(directory, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		c.t.Error(err)
		return
	}
	encodeErr, closeErr := json.NewEncoder(file).Encode(c.evidence), file.Close()
	if encodeErr != nil || closeErr != nil {
		c.t.Error("cannot write public acceptance report")
	}
}

func TestSignerFourProcessAcceptance(t *testing.T) {
	binary := os.Getenv("OWNERSHIP_MEMBER_BINARY")
	if binary == "" {
		t.Skip("OWNERSHIP_MEMBER_BINARY not configured; no process evidence claimed")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("member binary path must be absolute")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("configured member binary unavailable")
	}
	t.Run("quorum_restart_and_ordered_prefix", func(t *testing.T) {
		c := newProcessCommittee(t, binary)
		defer c.report("quorum_restart_and_ordered_prefix")
		plan := c.plan(c.batch(0, "A", 1, 1), c.batch(0, "B", 1, 0), c.batch(1, "next", 2, 0),
			c.batch(2, "after-next", 3, 1), c.batch(1, "old-account", 1, 0), c.batch(1, "A", 4, 1))
		for i := range c.members {
			c.start(i)
		}
		cert := Certificate{Batch: plan[0]}
		for i := 0; i < 3; i++ {
			cert.Signatures = append(cert.Signatures, c.vote(i, plan[0], true))
		}
		minority := Certificate{Batch: plan[1], Signatures: []Signature{c.vote(3, plan[1], true)}}
		if _, _, err := c.fixture.verifier.Verify(cert); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.fixture.verifier.Verify(minority); err == nil {
			t.Fatal("minority certificate accepted")
		}
		c.evidence["certificates"] = []Certificate{cert, minority}
		c.members[0].crash(t)
		c.members[0] = nil
		c.start(0)
		c.evidence["process_kill_after_published_vote"] = true
		c.vote(0, plan[1], false)
		if replay := c.vote(0, plan[0], true); !bytes.Equal(replay.Value, cert.Signatures[0].Value) {
			t.Fatal("restart changed the original signature")
		}
		registry, err := Open(filepath.Join(c.dir, "authorized.db"), c.fixture.verifier, c.fixture.genesis)
		if err != nil {
			t.Fatal(err)
		}
		defer registry.Close()
		if _, err := registry.Apply(minority); err == nil {
			t.Fatal("minority published authorization")
		}
		if changed, err := registry.Apply(cert); err != nil || !changed {
			t.Fatal("quorum authorization did not persist")
		}
		view, err := registry.Snapshot(c.fixture.genesis)
		if err != nil || view.Version() != 1 || view.Check(testAccount(1), 1) != nil {
			t.Fatal("persisted authorization mismatch")
		}
		c.evidence["authorized_after_registry_apply"] = true
		for i := range c.members {
			response := c.ask(i, MemberRequest{Action: "commit", Certificate: &cert}, true)
			if response.Version != 1 {
				t.Fatal("member did not commit version 1")
			}
			c.vote(i, plan[4], false)
			c.vote(i, plan[5], false)
			c.vote(i, plan[3], false)
		}
		next := Certificate{Batch: plan[2]}
		for i := 0; i < 3; i++ {
			next.Signatures = append(next.Signatures, c.vote(i, plan[2], true))
		}
		for i := 0; i < 3; i++ {
			if c.ask(i, MemberRequest{Action: "commit", Certificate: &next}, true).Version != 2 {
				t.Fatal("next prefix not committed")
			}
		}
		c.vote(3, plan[3], false) // Member 3 lacks the predecessor certificate.
		if c.ask(3, MemberRequest{Action: "commit", Certificate: &next}, true).Version != 2 {
			t.Fatal("minority did not catch up")
		}
		c.vote(3, plan[3], true)
		for i := 0; i < 3; i++ {
			c.vote(i, plan[3], true)
		}
		if changed, err := registry.Apply(next); err != nil || !changed {
			t.Fatal("next authorization did not persist")
		}
		view, err = registry.Snapshot(c.fixture.genesis)
		if err != nil || view.Version() != 2 || view.Check(testAccount(2), 0) != nil {
			t.Fatal("next authorization mismatch")
		}
		c.evidence["next_certificate"] = next
		c.evidence["authorized_registry_version"] = view.Version()
	})
	t.Run("split_vote_remains_locked_after_restart", func(t *testing.T) {
		c := newProcessCommittee(t, binary)
		defer c.report("split_vote_remains_locked_after_restart")
		plan := c.plan(c.batch(0, "split-A", 1, 1), c.batch(0, "split-B", 1, 0))
		certs := []Certificate{{Batch: plan[0]}, {Batch: plan[1]}}
		for i := range c.members {
			c.start(i)
			which := i / 2
			certs[which].Signatures = append(certs[which].Signatures, c.vote(i, plan[which], true))
		}
		registry, err := Open(filepath.Join(c.dir, "authorized.db"), c.fixture.verifier, c.fixture.genesis)
		if err != nil {
			t.Fatal(err)
		}
		defer registry.Close()
		for _, cert := range certs {
			if _, _, err := c.fixture.verifier.Verify(cert); err == nil {
				t.Fatal("split certificate verified")
			}
			if _, err := registry.Apply(cert); err == nil {
				t.Fatal("split certificate authorized")
			}
			for i := range c.members {
				c.ask(i, MemberRequest{Action: "commit", Certificate: &cert}, false)
			}
		}
		for i := range c.members {
			c.members[i].stop()
			c.members[i] = nil
			c.start(i)
			which := i / 2
			old := certs[which].Signatures[i%2]
			if replay := c.vote(i, plan[which], true); !bytes.Equal(old.Value, replay.Value) {
				t.Fatal("restart changed locked split vote")
			}
			c.vote(i, plan[1-which], false)
			if c.ask(i, MemberRequest{Action: "status"}, true).Version != 0 {
				t.Fatal("split vote advanced committed version")
			}
		}
		view, err := registry.Snapshot(c.fixture.genesis)
		if err != nil || view.Version() != 0 || view.Check(testAccount(1), 1) == nil {
			t.Fatal("split authorized an account")
		}
		c.evidence["certificates"] = certs
		c.evidence["authorized_registry_version"] = view.Version()
		c.evidence["liveness"] = "BLOCKED_WITHOUT_QUORUM; NO_TIMEOUT_UNLOCK"
	})
}
