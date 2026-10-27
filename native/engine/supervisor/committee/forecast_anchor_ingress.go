package committee

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Consume only newline-terminated events. Offset advances after installation;
// partial writes wait, malformed complete events fail instead of disappearing.
func (m *RuntimeUnknownAdmissionManager) consumeForecastAnchorsLocked() error {
	state := m.integratedStateLocked()
	if state.controllerMode != forecastControllerMode || state.forecastAnchorPath == "" {
		return nil
	}
	file, err := os.Open(state.forecastAnchorPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < state.forecastAnchorOffset {
		return fmt.Errorf("Anchor stream truncated")
	}
	if _, err = file.Seek(state.forecastAnchorOffset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		var next forecastAnchorState
		if err = json.Unmarshal(line, &next); err != nil {
			return err
		}
		if err = m.installForecastAnchorLocked(next, state.forecastObservedSequence); err != nil {
			return err
		}
		state.forecastAnchorOffset += int64(len(line))
	}
}
