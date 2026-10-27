package networks

import (
	"blockEmulator/params"
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"
)

// Dedicated bounded connection for history recovery; does not hold or enter
// the shared connection pool. Delay and upload-rate simulation remain active.
func SendHistoryBounded(payload []byte, address string, budget time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	delay := params.Delay
	if params.JitterRange > 0 {
		delay += rand.Intn(params.JitterRange) - params.JitterRange/2
	}
	if delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err = conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	data := append(append([]byte(nil), payload...), '\n')
	for len(data) > 0 {
		size := len(data)
		if rateLimiterUpload != nil {
			burst := rateLimiterUpload.Burst()
			if burst <= 0 {
				return fmt.Errorf("history transport upload burst must be positive")
			}
			if size > burst {
				size = burst
			}
			if err = rateLimiterUpload.WaitN(ctx, size); err != nil {
				return err
			}
		}
		written, writeErr := conn.Write(data[:size])
		if writeErr != nil {
			return writeErr
		}
		if written == 0 {
			return fmt.Errorf("history transport made no progress")
		}
		data = data[written:]
	}
	return nil
}
