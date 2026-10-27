package committee

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// appendJSONLRecord performs one O_APPEND write for one complete JSON record.
// A short write is an error and is never retried as a second append, because a
// concurrent writer could otherwise interleave bytes between the two calls.
func appendJSONLRecord(path string, record interface{}) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("JSONL path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	n, writeErr := handle.Write(payload)
	closeErr := handle.Close()
	if writeErr != nil {
		return writeErr
	}
	if n != len(payload) {
		return io.ErrShortWrite
	}
	return closeErr
}

func appendAdmissionUpdateJSONL(path string, update runtimeUnknownAdmissionUpdate) error {
	return appendJSONLRecord(path, update)
}

func mustAppendAdmissionUpdateJSONL(path string, update runtimeUnknownAdmissionUpdate) {
	if err := appendAdmissionUpdateJSONL(path, update); err != nil {
		panic(fmt.Sprintf("append runtime mapping update: %v", err))
	}
}
