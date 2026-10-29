package committee

import (
	"encoding/json"
	"testing"
)

func TestSelfEvaluationRejectsFullAsGP(t *testing.T) {
	t.Setenv("PAPER_SELF_EVAL_ARM", "GP")
	t.Setenv("SAA_FOREST_FACTS_V2", "false")
	m := testCostModel()
	if validateSelfEvaluationModel(m, selfEvaluationFullModelSHA) == nil { t.Fatal("Full model deployed as GP") }
	off := false
	m.ForestEnabled, m.AblationRole = &off, "GP"
	data, _ := json.Marshal(m)
	parsed, err := parseCostRankerModel(data)
	if err != nil { t.Fatal(err) }
	if err = validateSelfEvaluationModel(parsed, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != nil { t.Fatal(err) }
	t.Setenv("SAA_FOREST_FACTS_V2", "true")
	if validateSelfEvaluationModel(parsed, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") == nil { t.Fatal("GP enabled forest") }
}

func TestLightFeaturesAbsentForest(t *testing.T) {
	rows := make([]placementSignal, 16)
	observed := make([]lightTelemetryObservationV2, 16)
	previous := make([]float64, 16)
	for i := range previous { previous[i] = 1.0/16 }
	at := placementClockReading{ClockID:"test", MonoNS:100}
	raw, err := lightFeaturesOptionalForest(rows, observed, at, nil, nil,
		lightGraphSnapshotV2{Cutoff:10, SourceTime:20, ReceivedMonoNS:90, DayRates:make([]float64,16)}, previous, 0, 10, 20)
	if err != nil { t.Fatal(err) }
	for _, x := range raw {
		for _, i := range []int{14,15,16,17} { if x[i] != 0 { t.Fatal("forest value manufactured") } }
		if x[18] != 1 { t.Fatal("forest absence not marked missing") }
	}
}
