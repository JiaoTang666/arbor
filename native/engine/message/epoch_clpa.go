package message

// Epoch CLPA is a fresh-run, trusted-transport emulator protocol. These hashes
// bind batches to a plan; they are not signatures or Byzantine certificates.
import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const (
	CCLPAEpochInput    MessageType = "CLPAEpochInput"
	CCLPAEpochPlan     MessageType = "CLPAEpochPlan"
	CCLPAEpochReady    MessageType = "CLPAEpochReady"
	CCLPAEpochRelay    MessageType = "CLPAEpochRelay"
	CCLPAEpochTransfer MessageType = "CLPAEpochTransfer"
	CCLPAEpochApplied  MessageType = "CLPAEpochApplied"
	CCLPAEpochResume   MessageType = "CLPAEpochResume"
	CCLPAEpochDrain    MessageType = "CLPAEpochDrain"
	CLPAEpochInstall   RequestType = "CLPAEpochInstall"
)

type CLPAEpochInput struct {
	Epoch   uint64
	Batch   uint64 // One-based, contiguous per destination and epoch.
	ToShard uint64
	Txs     []*core.Transaction
}

type CLPAEpochPlan struct {
	Epoch     uint64 // New epoch; exactly BaseEpoch + 1.
	BaseEpoch uint64
	Mapping   map[string]uint64 // Changed owners only; unchanged routes are retained.
	InputCuts map[uint64]uint64 // Final input batch of BaseEpoch, including zero.
	PlanHash  string
}

func (p CLPAEpochPlan) Digest() string {
	p.PlanHash = ""
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	} // Struct contains only JSON-supported values.
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (p CLPAEpochPlan) Validate(shards uint64) error {
	if shards == 0 || p.BaseEpoch == ^uint64(0) || p.Epoch != p.BaseEpoch+1 || p.PlanHash != p.Digest() || len(p.InputCuts) != int(shards) {
		return fmt.Errorf("invalid CLPA epoch plan identity or input cuts")
	}
	for s := uint64(0); s < shards; s++ {
		if _, ok := p.InputCuts[s]; !ok {
			return fmt.Errorf("missing input cut for shard %d", s)
		}
	}
	for account, shard := range p.Mapping {
		decoded, err := hex.DecodeString(account)
		if err != nil || len(decoded) != 20 || hex.EncodeToString(decoded) != account || shard >= shards {
			return fmt.Errorf("invalid canonical account or destination")
		}
	}
	return nil
}

type CLPAEpochReady struct {
	Epoch     uint64
	PlanHash  string
	FromShard uint64
	RelayCut  uint64 // Last emitted ordinary-block relay batch in BaseEpoch.
}

type CLPAEpochRelay struct {
	Epoch     uint64
	Sequence  uint64 // One-based; an empty batch is sent for every ordinary block.
	FromShard uint64
	Txs       []*core.Transaction
}

type CLPAEpochTransfer struct {
	Epoch           uint64
	PlanHash        string
	FromShard       uint64
	ToShard         uint64
	Addrs           []string
	AccountState    []*core.AccountState
	Txs             []*core.Transaction   // Pending first legs or pending relay second legs only.
	SourceRounds    uint64                `json:",omitempty"` // Actual ordinary commit cut in async mode.
	PendingManifest []CLPAEpochPendingLeg `json:",omitempty"`
}

// The async source's local bundle describes its complete pending queue without
// copying retained transaction payload. A remote bundle describes only its
// moved payload. Hashes bind the immutable admission leg to its routing owner.
type CLPAEpochPendingLeg struct {
	AdmissionID  string
	Relayed      bool
	PayloadHash  string
	OwnerAccount string // Sender for ordinary work, recipient for relay credit.
	Destination  uint64
}

type CLPAEpochApplied struct {
	Epoch                uint64
	PlanHash             string
	Shard                uint64
	Node                 uint64
	Height               uint64
	StateRoot            string
	Pending              int
	PendingOrdinary      int
	PendingRelayed       int
	PendingPayloadBytes  uint64
	MovedIn              int
	MovedOut             int
	AsyncPipeline        bool   `json:",omitempty"`
	ReplicaRole          string `json:",omitempty"`
	LocalRounds          uint64 `json:",omitempty"`
	InputCut             uint64 `json:",omitempty"`
	RetainedPending      int    `json:",omitempty"`
	RetainedAvailable    int    `json:",omitempty"`
	RetainedManifestHash string `json:",omitempty"`
	IncomingPending      int    `json:",omitempty"`
	IncomingManifestHash string `json:",omitempty"`
	SourceManifestHash   string `json:",omitempty"`
	PendingLedgerHash    string `json:",omitempty"`
	InstallPayloadHash   string `json:",omitempty"`
	InstallPayloadBytes  uint64 `json:",omitempty"`
	PendingManifestBytes uint64 `json:",omitempty"`
}

const (
	CLPAEpochFixedLeaderRole = "fixed_leader_complete_pending"
	CLPAEpochFollowerRole    = "follower_state_and_incoming_subset"
)

type CLPAEpochResume struct {
	Epoch    uint64
	PlanHash string
}

type CLPAEpochDrain struct {
	Epoch uint64
}
