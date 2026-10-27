package signal

import "testing"

func TestDrainReportOrder(t *testing.T) {
	for name, reports := range map[string][][2]int{
		"source first": {{3, 0}, {0, 3}},
		"target first": {{0, 3}, {3, 0}},
		"interleaved": {{1, 0}, {0, 3}, {2, 0}},
	} {
		t.Run(name, func(t *testing.T) {
			s := NewStopSignal(12, 4)
			for i, report := range reports {
				s.ObserveSecondLegProgress(report[0], report[1])
				if got := s.Snapshot().SecondLegDrainReady; got != (i == len(reports)-1) {
					t.Fatalf("report %d: drain ready=%v, pending=%d", i, got, s.Snapshot().PendingSecondLegTx)
				}
			}
			if got := s.Snapshot().PendingSecondLegTx; got != 0 {
				t.Fatalf("all reports received, pending=%d", got)
			}
		})
	}
}
