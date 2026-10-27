package core

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// PublishSourceHandoffBatch prevalidates the entire owned batch before writing.
// Existing legacy records and bundles keep the same immutable-ID contract.
// Publication is transport only: it never acknowledges execution or dequeues.
func PublishSourceHandoffBatch(dir string, hs []*MigrationHandoff) error {
	return publishSourceHandoffBatch(dir, hs, PublishMigrationArtifacts)
}

func publishSourceHandoffBatch(dir string, hs []*MigrationHandoff, publish func(string, string, MigrationArtifacts) error) error {
	if len(hs) == 0 {
		return fmt.Errorf("empty source handoff batch")
	}
	items := MigrationArtifacts{}
	for _, h := range hs {
		if err := ValidateMigrationHandoff(h); err != nil {
			return err
		}
		raw, err := json.Marshal(h)
		if err != nil {
			return err
		}
		if _, found := items[h.ID]; found {
			return fmt.Errorf("duplicate source handoff %s", h.ID)
		}
		if err := items.add(h.ID, raw, ".handoff.json"); err != nil {
			return err
		}
	}
	existing, err := LoadMigrationArtifacts(dir, ".handoff.json", false)
	if err != nil {
		return err
	}
	for id, raw := range items {
		if old, found := existing[id]; found && !bytes.Equal(old, raw) {
			return fmt.Errorf("immutable source handoff conflict %s", id)
		}
	}
	// Do not skip equal visible files after an uncertain sync: retry the same
	// content-addressed bundle and its durability barriers, including restart.
	return publish(dir, ".handoff.json", items)
}
