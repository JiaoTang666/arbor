package message

// PlacementTelemetryV2 is a leader-local observation. It never confirms work.
// Queue excludes the proposed block and anticipated, not-yet-generated relay2.
const CPlacementTelemetryV2 MessageType = "PlacementTelemetryV2"

type PlacementTelemetryV2 struct {
	Schema string `json:"schema"`
	Shard uint64 `json:"shard"`
	Node uint64 `json:"node"`
	View uint64 `json:"view"`
	ClockID string `json:"clock_id"`
	Sequence uint64 `json:"sequence"`
	ReplacedReports uint64 `json:"replaced_reports"`
	CapturedMonoNS int64 `json:"captured_mono_ns"`
	Queue int `json:"queue"`
	LastNonemptyCommitID string `json:"last_nonempty_commit_id"`
	LastNonemptyFill float64 `json:"last_nonempty_fill"`
	MeanNonemptyDurationNS float64 `json:"mean_nonempty_duration_ns"`
	ServiceMissing bool `json:"service_missing"`
}
