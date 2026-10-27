package committee

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimePublishesTargetCommandBeforeAnyActivation(t *testing.T) {
	m, p := sourceResultFixture(t)
	s := m.integratedStateLocked()
	path := filepath.Join(t.TempDir(), "target.json")
	t.Setenv("SAA_NATIVE_TARGET_INSTALL_PATH", path)
	s.nativeSourcePreparedPath = writeSourceResult(t, p)
	var first []byte
	for _, epoch := range []int{2, 2, 3} {
		m.lock.Lock()
		m.applySAAIntegratedDueLocked(epoch)
		m.lock.Unlock()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var delivered core.SourcePreparation
		if err = json.Unmarshal(data, &delivered); err != nil {
			t.Fatal(err)
		}
		if delivered.Command != p.Command || !bytes.Equal(delivered.State, p.State) {
			t.Fatal("runtime changed source preparation")
		}
		if first == nil {
			first = data
		} else if !bytes.Equal(first, data) {
			t.Fatal("poll changed immutable command")
		}
		if s.authority.routeVersion != 0 || s.authority.decisionByID["p"].Status == mappingApplied {
			t.Fatal("publication activated route without receipt")
		}
	}
}
