package chain

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func TestAppendJSONLRecordConcurrentLinesRemainValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.jsonl")
	const writers = 32
	const recordsPerWriter = 100
	var wait sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		writer := writer
		wait.Add(1)
		go func() {
			defer wait.Done()
			for sequence := 0; sequence < recordsPerWriter; sequence++ {
				appendJSONLRecord(path, map[string]int{
					"writer":   writer,
					"sequence": sequence,
				})
			}
		}()
	}
	wait.Wait()

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
			t.Fatalf("malformed JSONL line %d: %v", count+1, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != writers*recordsPerWriter {
		t.Fatalf("got %d records, want %d", count, writers*recordsPerWriter)
	}
}

func TestAppendJSONLRecordConcurrentProcessesRemainValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "multiprocess.jsonl")
	const writers = 12
	const recordsPerWriter = 80
	commands := make([]*exec.Cmd, 0, writers)
	for writer := 0; writer < writers; writer++ {
		command := exec.Command(os.Args[0], "-test.run=TestAppendJSONLRecordProcessHelper")
		command.Env = append(os.Environ(),
			"GO_JSONL_HELPER=1",
			"GO_JSONL_PATH="+path,
			"GO_JSONL_WRITER="+strconv.Itoa(writer),
			"GO_JSONL_RECORDS="+strconv.Itoa(recordsPerWriter),
		)
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
	}
	for _, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	assertValidJSONLRecordCount(t, path, writers*recordsPerWriter)
}

func TestAppendJSONLRecordProcessHelper(t *testing.T) {
	if os.Getenv("GO_JSONL_HELPER") != "1" {
		return
	}
	writer, err := strconv.Atoi(os.Getenv("GO_JSONL_WRITER"))
	if err != nil {
		t.Fatal(err)
	}
	records, err := strconv.Atoi(os.Getenv("GO_JSONL_RECORDS"))
	if err != nil {
		t.Fatal(err)
	}
	for sequence := 0; sequence < records; sequence++ {
		appendJSONLRecord(os.Getenv("GO_JSONL_PATH"), map[string]int{
			"writer":   writer,
			"sequence": sequence,
		})
	}
}

func assertValidJSONLRecordCount(t *testing.T, path string, expected int) {
	t.Helper()
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
			t.Fatalf("malformed JSONL line %d: %v", count+1, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("got %d records, want %d", count, expected)
	}
}
