package committee

import (
	"fmt"
	"os"
)

const selfEvaluationFullModelSHA = "f03d32a81f77f805d5fe0c3c018112ea246abc36c563c5eb7856b81f9b59859b"

func validateSelfEvaluationModel(model *costRankerModel, digest string) error {
	arm := os.Getenv("PAPER_SELF_EVAL_ARM")
	gp := model.ForestEnabled != nil && !*model.ForestEnabled && model.AblationRole == "GP"
	if gp {
		if arm != "GP" || os.Getenv("SAA_FOREST_FACTS_V2") != "false" || digest == selfEvaluationFullModelSHA {
			return fmt.Errorf("GP model requires explicit GP deployment with forest disabled")
		}
		return nil
	}
	if arm == "GP" { return fmt.Errorf("Full weights cannot be deployed as GP") }
	if arm != "" && (arm != "Full" && arm != "Full-no-prune" || digest != selfEvaluationFullModelSHA || os.Getenv("SAA_FOREST_FACTS_V2") != "true") {
		return fmt.Errorf("self evaluation Full requires unchanged frozen model and forest")
	}
	return nil
}

func (m *RelayCommitteeModule) graphOnlyCostSnapshot() lightGraphSnapshotV2 {
	p := m.newNewPolicy
	p.mu.RLock()
	defer p.mu.RUnlock()
	prior := p.prior
	if prior.ForestEnabled == nil || *prior.ForestEnabled || !prior.ForestChannelsDisabled { panic("unmarked graph-only prior") }
	rates := make([]float64, len(prior.Rates))
	for i := range rates { rates[i] = prior.Rates[i][1] }
	return lightGraphSnapshotV2{Version: int64(prior.Version), Cutoff: prior.Cutoff, SourceTime: prior.SourceTime, SHA256: prior.Digest, JointSHA256: "", ReceivedMonoNS: p.priorReceivedMonoNS, DayRates: rates}
}
