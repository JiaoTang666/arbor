package committee

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAppendJSONLRecordConcurrentIntegrity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	const writers, perWriter = 16, 50
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for sequence := 0; sequence < perWriter; sequence++ {
				if err := appendJSONLRecord(path, map[string]int{"writer": id, "sequence": sequence}); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(writer)
	}
	wg.Wait()
	handle, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	scanner := bufio.NewScanner(handle)
	count := 0
	for scanner.Scan() {
		var record map[string]int
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("malformed line %d: %v", count+1, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != writers*perWriter {
		t.Fatalf("records=%d want=%d", count, writers*perWriter)
	}
}

func TestAppendJSONLRecordRejectsEmptyPath(t *testing.T) {
	if err := appendJSONLRecord("", map[string]int{"x": 1}); err == nil {
		t.Fatal("empty path must fail")
	}
}
