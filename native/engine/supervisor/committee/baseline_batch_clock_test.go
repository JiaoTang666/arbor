package committee

import (
	"blockEmulator/params"
	"testing"
	"time"
)

func TestBaselineCLPABatchClockMatchesNativeRelease(t *testing.T) {
	t.Setenv("BLOCK_EMULATOR_COMMON_BATCH_CLOCK_V1", "1")
	previous := params.InjectSpeed
	params.InjectSpeed = 2000
	t.Cleanup(func() { params.InjectSpeed = previous })
	start := time.Now()
	e := &clpaEpochController{start: start, ccm: &CLPACommitteeModule{dataTotalNum: 2572, batchDataNum: 1000}}
	for _, test := range []struct {
		at   time.Duration
		want int
	}{
		{-time.Nanosecond, 0}, {0, 1000}, {499 * time.Millisecond, 1000},
		{500 * time.Millisecond, 2000}, {time.Second, 2572},
	} {
		if got := e.offeredDue(start.Add(test.at)); got != test.want {
			t.Fatalf("at %v: got %d, want %d", test.at, got, test.want)
		}
	}
}
