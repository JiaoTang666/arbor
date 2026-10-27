package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func (n *PbftConsensusNode) targetInstallInput() ([]byte, bool) {
	path := os.Getenv("SAA_NATIVE_TARGET_INSTALL_PATH")
	if path == "" {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var p core.SourcePreparation
	if json.Unmarshal(data, &p) != nil || p.Command.TargetShard != n.ShardID {
		return nil, false
	}
	return data, true
}
func (n *PbftConsensusNode) hasTargetInstallInput() bool {
	data, ok := n.targetInstallInput()
	if !ok {
		return false
	}
	value, seen := n.sourcePrepareSeen.Load(sha256.Sum256(data))
	return !seen || value != true
}
func (n *PbftConsensusNode) targetInstallProposal() (*message.Request, error) {
	data, ok := n.targetInstallInput()
	if !ok {
		return nil, nil
	}
	key := sha256.Sum256(data)
	if value, seen := n.sourcePrepareSeen.Load(key); seen && value == true {
		return nil, nil
	}
	r := &message.Request{RequestType: targetInstallRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}
	p, err := decodeTargetInstall(r)
	if err != nil {
		return nil, err
	}
	old, err := n.CurChain.TargetInstallation(p.Command.Account)
	if err != nil {
		return nil, err
	}
	if old != nil {
		if err = n.commitTargetInstall(r, old.InstallSequence, getDigest(r)); err != nil {
			return nil, err
		}
		n.sourcePrepareSeen.Store(key, true)
		return nil, nil
	}
	if n.sequenceID == 0 {
		return nil, fmt.Errorf("target installation sequence is uninitialized")
	}
	if err = n.CurChain.ValidatePreparedTarget(p, n.sequenceID, getDigest(r)); err != nil {
		n.sourcePrepareSeen.Store(key, true)
		return nil, err
	}
	return r, nil
}
