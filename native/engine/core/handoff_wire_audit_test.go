package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSavedHandoffWireAudit(t *testing.T) {
	root := os.Getenv("HANDOFF_WIRE_AUDIT_DIR")
	if root == "" {
		t.Skip("saved artifact only")
	}
	files, e := filepath.Glob(filepath.Join(root, "handoff", "*.handoff.json"))
	if e != nil || len(files) == 0 {
		t.Fatal("no handoff artifacts")
	}
	h := MigrationHandoff{}
	raw, e := os.ReadFile(files[0])
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &h); e != nil {
		t.Fatal(e)
	}
	original := DecodeTx(h.Original)
	expected, e := BuildMigrationHandoff(original, h.ProposalID, h.Account, h.SourceShard, h.TargetShard, h.MappingVersion)
	if e != nil {
		t.Fatal(e)
	}
	origSame := reflect.DeepEqual(original, DecodeTx(expected.Original))
	executionSame := reflect.DeepEqual(DecodeTx(h.Execution), DecodeTx(expected.Execution))
	different := 0
	for different < len(h.Original) && different < len(expected.Original) && h.Original[different] == expected.Original[different] {
		different++
	}
	out := map[string]interface{}{"file": files[0], "validation_error": fmt.Sprint(ValidateMigrationHandoff(&h)), "original_bytes_equal": bytes.Equal(h.Original, expected.Original), "execution_bytes_equal": bytes.Equal(h.Execution, expected.Execution), "original_fields_equal": origSame, "execution_fields_equal": executionSame, "first_original_difference": different, "original_prefix": fmt.Sprintf("%x", h.Original[:16]), "rebuilt_prefix": fmt.Sprintf("%x", expected.Original[:16])}
	data, _ := json.MarshalIndent(out, "", "  ")
	os.WriteFile(filepath.Join(root, "wire_audit.json"), data, 0600)
	t.Log(string(data))
	if !origSame || !executionSame {
		t.Fatal("semantic difference, not only encoding")
	}
	if e := ValidateMigrationHandoff(&h); e != nil {
		t.Fatal("saved cross-process handoff still rejected", e)
	}
	for _, file := range files {
		raw, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		var item MigrationHandoff
		if e = json.Unmarshal(raw, &item); e != nil {
			t.Fatal(e)
		}
		if e = ValidateMigrationHandoff(&item); e != nil {
			t.Fatal(file, e)
		}
	}
	ackFiles, _ := filepath.Glob(filepath.Join(root, "ack", "*.ack.json"))
	for _, file := range ackFiles {
		raw, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		var ack HandoffAck
		if e = json.Unmarshal(raw, &ack); e != nil {
			t.Fatal(e)
		}
		raw, e = os.ReadFile(filepath.Join(root, "handoff", ack.ID+".handoff.json"))
		if e != nil {
			t.Fatal(e)
		}
		var item MigrationHandoff
		if e = json.Unmarshal(raw, &item); e != nil {
			t.Fatal(e)
		}
		if e = ValidateHandoffAck(&item, ack); e != nil {
			t.Fatal(file, e)
		}
	}
	t.Logf("all saved records PASS: handoffs=%d acknowledgements=%d", len(files), len(ackFiles))
	bad := h
	tx := DecodeTx(h.Execution)
	tx.RoutedSenderShard = h.SourceShard
	bad.Execution = tx.Encode()
	if ValidateMigrationHandoff(&bad) == nil {
		t.Fatal("altered execution route accepted")
	}
	bad = h
	bad.ID = "tampered"
	if ValidateMigrationHandoff(&bad) == nil {
		t.Fatal("altered identity accepted")
	}
}
