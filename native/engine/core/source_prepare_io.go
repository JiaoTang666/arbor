package core

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
)

// PublishSourcePrepareFile publishes an immutable single-command/result file.
// Temporary and final files share a directory; Link never overwrites a different
// proposal. This is a trusted local experiment transport, not authentication.
func PublishSourcePrepareFile(path string, data []byte) error {
	if path == "" {
		return fmt.Errorf("source preparation path required")
	}
	if old, err := os.ReadFile(path); err == nil {
		if bytes.Equal(old, data) {
			return syncPublishedSourcePrepare(path)
		}
		return fmt.Errorf("source preparation file conflict")
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".sef-prepare-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Link(f.Name(), path); err != nil {
		if old, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(old, data) {
			return syncPublishedSourcePrepare(path)
		}
		return err
	}
	return syncPublishedSourcePrepare(path)
}

// An existing identical link is not proof that the previous directory sync
// succeeded. Retry both barriers before reporting publication as durable.
func syncPublishedSourcePrepare(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
