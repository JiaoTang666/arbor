package ownership

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestSignerIPCRejectsMalformedAndReportsNonzeroHead(t *testing.T) {
	f := newCommitteeFixture(t)
	signer, err := OpenSigner(filepath.Join(t.TempDir(), "member.db"), f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	defer signer.Close()
	cert := f.certificate(t, 0, "ipc-first", Binding{testAccount(1), 0})
	if _, err := signer.Commit(cert); err != nil {
		t.Fatal(err)
	}
	next := f.certificate(t, 1, "ipc-next", Binding{testAccount(2), 1}).Batch
	var input bytes.Buffer
	input.WriteString("not-json\n{\"action\":\"status\",\"unknown\":true}\n{\"action\":\"status\"} {}\n{\"action\":\"vote\"}\n")
	encoder := json.NewEncoder(&input)
	encoder.Encode(MemberRequest{Action: "status"})
	encoder.Encode(MemberRequest{Action: "vote", Batch: &next})
	var output bytes.Buffer
	if err := ServeMember(signer, &input, &output); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&output)
	for index := 0; index < 6; index++ {
		var response MemberResponse
		if err := decoder.Decode(&response); err != nil {
			t.Fatal(err)
		}
		if index < 4 {
			if response.Status != "FAIL" || response.Signature != nil {
				t.Fatalf("malformed request %d accepted", index)
			}
		} else {
			if response.Status != "PASS" || response.Version != 1 {
				t.Fatalf("request %d reported wrong committed head: %+v", index, response)
			}
			if index == 5 && (response.Signature == nil || response.Signature.MemberID != "member-0") {
				t.Fatal("valid vote has no signature")
			}
		}
	}
}

func TestSignerIPCAdvanceVoteReportsAppliedParentAndPinnedPolicy(t *testing.T) {
	f := newCommitteeFixture(t)
	s, err := OpenSignerWithPolicy(filepath.Join(t.TempDir(), "member.db"), f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := witnessedBatch(t, f, 0, "one", 1, []PlacementTransaction{
		{"first", 1, testAccount(1000), testAccount(1), 0, 0},
	}, Binding{testAccount(1), 0})
	parent := f.sign(t, first)
	next := witnessedBatch(t, f, 1, "two", 2, []PlacementTransaction{
		{"second", 2, testAccount(1), testAccount(2), 0, 0},
	}, Binding{testAccount(2), 0})
	var input, output bytes.Buffer
	json.NewEncoder(&input).Encode(MemberRequest{Action: "advance_vote", Batch: &next, Certificates: []Certificate{parent}})
	if err := ServeMember(s, &input, &output); err != nil {
		t.Fatal(err)
	}
	var response MemberResponse
	if err := json.NewDecoder(&output).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Status != "PASS" || response.Version != 1 || response.Signature == nil || response.PlacementPolicy != PlacementWitnessSchema {
		t.Fatalf("unexpected fused policy response: %+v", response)
	}
}
