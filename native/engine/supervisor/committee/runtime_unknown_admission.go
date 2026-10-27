package committee

import (
	"blockEmulator/core"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
)

const (
	runtimeUnknownAdmissionEnabledEnv       = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED"
	runtimeMappingBoundaryEnabledEnv        = "BLOCK_EMULATOR_MAPPING_BOUNDARY_ENABLED"
	runtimeUnknownDynamicEnabledEnv         = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_DYNAMIC_ENABLED"
	runtimeUnknownGlobalFallbackEnabledEnv  = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_GLOBAL_FALLBACK_ENABLED"
	runtimeUnknownMinAnchorTxEnv            = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_MIN_ANCHOR_TX"
	runtimeUnknownMinScoreShareEnv          = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_MIN_SCORE_SHARE"
	runtimeUnknownLocalityWeightEnv         = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_LOCALITY_WEIGHT"
	runtimeUnknownAnchorWeightEnv           = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ANCHOR_WEIGHT"
	runtimeUnknownCrossShardWeightEnv       = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_CROSS_SHARD_WEIGHT"
	runtimeUnknownLoadWeightEnv             = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_LOAD_WEIGHT"
	runtimeUnknownDecisionModeEnv           = "BLOCK_EMULATOR_SAA_DECISION_MODE"
	runtimeUnknownObservationTauEnv         = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_OBSERVATION_TAU"
	runtimeUnknownAnchorTauEnv              = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ANCHOR_TAU"
	runtimeUnknownPersistenceTauEnv         = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_PERSISTENCE_TAU"
	runtimeUnknownLoadTauEnv                = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_LOAD_TAU"
	runtimeUnknownSoftAnchorWeightEnv       = "BLOCK_EMULATOR_SAA_ANCHOR_WEIGHT"
	runtimeUnknownSoftLocalityWeightEnv     = "BLOCK_EMULATOR_SAA_LOCALITY_WEIGHT"
	runtimeUnknownPersistenceWeightEnv      = "BLOCK_EMULATOR_SAA_PERSISTENCE_WEIGHT"
	runtimeUnknownSoftCrossWeightEnv        = "BLOCK_EMULATOR_SAA_CROSS_SHARD_WEIGHT"
	runtimeUnknownSoftLoadWeightEnv         = "BLOCK_EMULATOR_SAA_LOAD_WEIGHT"
	runtimeUnknownRiskWeightEnv             = "BLOCK_EMULATOR_SAA_RISK_WEIGHT"
	runtimeUnknownMigrationTemperatureEnv   = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_MIGRATION_TEMPERATURE"
	runtimeUnknownMigrationMarginEnv        = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_MIGRATION_MARGIN"
	runtimeUnknownSoftAuditPathEnv          = "BLOCK_EMULATOR_SAA_SOFT_DECISION_AUDIT_PATH"
	runtimeUnknownSoftAuditFullEnv          = "BLOCK_EMULATOR_SAA_SOFT_AUDIT_FULL"
	runtimeUnknownOverflowDiversionEnv      = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_OVERFLOW_DIVERSION_ENABLED"
	runtimeUnknownOverflowRatioEnv          = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_OVERFLOW_RATIO"
	runtimeUnknownOverflowMinAdmissionsEnv  = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_OVERFLOW_MIN_ADMISSIONS"
	runtimeUnknownProtectAnchorShareEnv     = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_STRONG_ANCHOR_PROTECT_SHARE"
	runtimeUnknownMinDwellEpochsEnv         = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_MIN_DWELL_EPOCHS"
	runtimeUnknownStrongAnchorDwellEnv      = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_STRONG_ANCHOR_DWELL_EPOCHS"
	runtimeUnknownRouterSafetyLagEnv        = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ROUTER_SAFETY_LAG_EPOCHS"
	runtimeUnknownRefreshMaxPerEpochEnv     = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_REFRESH_MAX_PER_EPOCH"
	runtimeUnknownPressureHintPathEnv       = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_PRESSURE_HINT_PATH"
	runtimeUnknownRoutePressureWindowEnv    = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ROUTE_PRESSURE_WINDOW_EPOCHS"
	runtimeUnknownRouterStatusPathEnv       = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ROUTER_STATUS_PATH"
	runtimeUnknownQPSAModeEnv               = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_MODE"
	runtimeUnknownQPSAQueueWeightEnv        = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_QUEUE_WEIGHT"
	runtimeUnknownQPSADemandCapEnv          = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_DEMAND_CAP"
	runtimeUnknownQPSAMinRelativeGainEnv    = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_MIN_RELATIVE_POTENTIAL_GAIN"
	runtimeUnknownQPSAAuditPathEnv          = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_AUDIT_PATH"
	runtimeUnknownQPSAV2ModeEnv             = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_V2_MODE"
	runtimeUnknownQPSAV2QueueStatePathEnv   = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_V2_QUEUE_STATE_PATH"
	runtimeUnknownQPSAV2AuditPathEnv        = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_V2_AUDIT_PATH"
	runtimeUnknownQPSAV2DriftWeightEnv      = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_QPSA_V2_DRIFT_WEIGHT"
	runtimeAnchorRealignmentModeEnv         = "BLOCK_EMULATOR_ANCHOR_REALIGNMENT_MODE"
	runtimeAnchorProposalPathEnv            = "BLOCK_EMULATOR_ANCHOR_PROPOSAL_PATH"
	runtimeExternalMappingUpdatePathEnv     = "BLOCK_EMULATOR_EXTERNAL_MAPPING_UPDATE_PATH"
	runtimeExternalMappingAuditPathEnv      = "BLOCK_EMULATOR_SUPERVISOR_MAPPING_AUDIT_PATH"
	runtimeMappingDecisionReadyPathEnv      = "BLOCK_EMULATOR_MAPPING_DECISION_READY_PATH"
	runtimeMappingDecisionRequestPathEnv    = "BLOCK_EMULATOR_MAPPING_DECISION_REQUEST_PATH"
	runtimeMappingDecisionBarrierAuditEnv   = "BLOCK_EMULATOR_MAPPING_DECISION_BARRIER_AUDIT_PATH"
	runtimeMappingDecisionBarrierTimeoutEnv = "BLOCK_EMULATOR_MAPPING_DECISION_BARRIER_TIMEOUT_MS"
	runtimeMappingDecisionModeEnv           = "BLOCK_EMULATOR_MAPPING_DECISION_MODE"
	runtimeExternalActionEnabledEnv         = "BLOCK_EMULATOR_EXTERNAL_ACTION_ENABLED"
	runtimeCausalLagEpochsEnv               = "BLOCK_EMULATOR_CAUSAL_LAG_EPOCHS"
	runtimeRouteBudgetPerEpochEnv           = "BLOCK_EMULATOR_ROUTE_BUDGET_PER_EPOCH"
	runtimeEffectiveConfigDirEnv            = "BLOCK_EMULATOR_RUNTIME_EFFECTIVE_CONFIG_DIR"
	runtimeDedicatedMappingUpdatePathEnv    = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_MAPPING_UPDATE_PATH"
	runtimeAccountClusterMapPathEnv         = "BLOCK_EMULATOR_ACCOUNT_CLUSTER_MAP_PATH"
	runtimeClusterShardMapPathEnv           = "BLOCK_EMULATOR_CLUSTER_SHARD_MAP_PATH"
)

type runtimeUnknownObservation struct {
	TotalTxCount          int
	FirstObservedRound    int
	LastObservedRound     int
	ShardEvidence         map[uint64]int
	StableShardEvidence   map[uint64]int
	StableClusterEvidence map[int]int
}

type runtimeUnknownAdmissionUpdate struct {
	ProposalID           string   `json:"proposal_id"`
	EpochID              int      `json:"epoch_id"`
	EffectiveEpoch       int      `json:"effective_epoch"`
	ObjectType           string   `json:"object_type"`
	ObjectID             string   `json:"object_id"`
	SourceShard          uint64   `json:"source_shard"`
	TargetShard          uint64   `json:"target_shard"`
	Addresses            []string `json:"addresses"`
	ResolvedAccountCount int      `json:"resolved_account_count"`
	ApplyMode            string   `json:"apply_mode"`
	PlacementMode        string   `json:"placement_mode"`
	PlacementReason      string   `json:"placement_reason"`
	PlacementConfidence  float64  `json:"placement_confidence"`
	ObservedTxCount      int      `json:"observed_tx_count"`
	AnchorTxCount        int      `json:"anchor_tx_count"`
	AnchorScoreShare     float64  `json:"anchor_score_share"`
	ParentStableObjectID string   `json:"parent_stable_object_id,omitempty"`
	ProjectionClusterID  *int     `json:"projection_target_cluster_id,omitempty"`
	AnchorClusterID      *int     `json:"anchor_cluster_id,omitempty"`
	StableClusterID      *int     `json:"stable_cluster_id,omitempty"`
	PressureHintEpoch    int      `json:"pressure_hint_epoch,omitempty"`
	TargetPressureScore  float64  `json:"target_pressure_score,omitempty"`
	PressureSignalSource string   `json:"pressure_signal_source,omitempty"`
	ControlChannel       string   `json:"control_channel,omitempty"`
	ProposalKind         string   `json:"proposal_kind,omitempty"`
}

type runtimeShardPressureHint struct {
	BoundaryEpoch int `json:"boundary_epoch"`
	Shards        []struct {
		ShardID       uint64  `json:"shard_id"`
		PressureScore float64 `json:"pressure_score"`
		QueueLength   float64 `json:"queue_length"`
		QueueRatio    float64 `json:"queue_ratio"`
	} `json:"shards"`
}

type runtimeRouterShardStatus struct {
	ShardID       uint64  `json:"shard_id"`
	PressureScore float64 `json:"pressure_score"`
	IsHot         bool    `json:"is_hot"`
	IsCold        bool    `json:"is_cold"`
}

type runtimeRouterPressureStatus struct {
	LogicalRound int                        `json:"logical_round"`
	SignalSource string                     `json:"signal_source"`
	HotShards    []uint64                   `json:"hot_shards"`
	ColdShards   []uint64                   `json:"cold_shards"`
	Shards       []runtimeRouterShardStatus `json:"shards"`
}

type RuntimeUnknownAdmissionManager struct {
	enabled                         bool
	mappingBoundaryEnabled          bool
	dynamicAdmissionEnabled         bool
	externalActionEnabled           bool
	globalFallbackEnabled           bool
	shardNum                        uint64
	mappingUpdatePath               string
	minAnchorTx                     int
	minScoreShare                   float64
	localityWeight                  float64
	anchorWeight                    float64
	crossWeight                     float64
	loadWeight                      float64
	softDecisionMode                string
	softDecay                       saaSoftDecayConfig
	saaSoftStateByAccount           map[string]*saaSoftAccountState
	saaSoftLoadByShard              map[uint64]saaSoftLoadState
	saaSoftLoadLastRound            int
	saaSoftAuditPath                string
	saaSoftAuditFull                bool
	saaSoftAuditLastRound           map[string]int
	overflowDiversionEnabled        bool
	overflowRatio                   float64
	overflowMinAdmissions           int
	strongAnchorProtectShare        float64
	minDwellEpochs                  int
	strongAnchorDwellEpochs         int
	routerSafetyLagEpochs           int
	refreshMaxPerEpoch              int
	anchorRealignmentMode           string
	anchorProposalPath              string
	stableShardByAccount            map[string]uint64
	stableClusterByAccount          map[string]int
	stableShardByCluster            map[int]uint64
	externalShardByAccount          map[string]uint64
	runtimeShardByAccount           map[string]uint64
	pendingRuntimeShard             map[string]uint64
	pendingRuntimeRouteRound        map[string]int
	pendingRuntimeUpdate            map[string]runtimeUnknownAdmissionUpdate
	appliedExternalUpdates          map[string]bool
	shardAdmissionCounts            map[uint64]int
	observations                    map[string]*runtimeUnknownObservation
	lastRuntimeRefreshRound         map[string]int
	runtimeRefreshCountRound        map[int]int
	lastExternalRefreshRound        int
	externalAppliedSequence         uint64
	routeVersion                    uint64
	externalRollingHash             [32]byte
	externalMappingAuditPath        string
	mappingDecisionReadyPath        string
	mappingDecisionRequestPath      string
	mappingDecisionBarrierAuditPath string
	mappingDecisionBarrierTimeout   time.Duration
	mappingDecisionMode             string
	mappingDecisionsClosed          bool
	lastMappingDecisionBarrierRound int
	acceptedMappingDecisionRounds   map[int]bool
	lastPressureHintRound           int
	pressureHintEpoch               int
	livePressureByShard             map[uint64]float64
	liveQueueByShard                map[uint64]float64
	routePressureWindowEpochs       int
	recentRoutePressure             map[int]map[uint64]int
	lastRouteRound                  int
	routerStatusPath                string
	qpsaMode                        string
	qpsaQueueWeight                 float64
	qpsaDemandCap                   float64
	qpsaMinRelativePotentialGain    float64
	qpsaAuditPath                   string
	qpsaProjectedDemand             map[int]map[uint64]float64
	qpsaV2Mode                      string
	qpsaV2QueueStatePath            string
	qpsaV2AuditPath                 string
	qpsaV2DriftWeight               float64
	qpsaV2LastRefreshRound          int
	qpsaV2StateByShard              map[uint64]qpsaV2QueueState
	qpsaV2ProjectedDemand           map[int]map[uint64]float64
	lock                            sync.Mutex
	sl                              *supervisor_log.SupervisorLog
}

func (m *RuntimeUnknownAdmissionManager) effectiveConfiguration() map[string]interface{} {
	if m == nil {
		return map[string]interface{}{}
	}
	return map[string]interface{}{
		"admission_enabled":                m.enabled,
		"dynamic_admission_enabled":        m.dynamicAdmissionEnabled,
		"mapping_boundary_enabled":         m.mappingBoundaryEnabled,
		"external_action_enabled":          m.externalActionEnabled,
		"mapping_decision_mode":            m.mappingDecisionMode,
		"causal_lag_epochs":                parseEnvInt(runtimeCausalLagEpochsEnv, 1),
		"route_budget_per_epoch":           parseEnvInt(runtimeRouteBudgetPerEpochEnv, 1),
		"epoch_aware_dispatch_enabled":     parseEnvBool("BLOCK_EMULATOR_EPOCH_AWARE_DISPATCH_ENABLED", false),
		"epoch_aware_dispatch_timeout_ms":  parseEnvInt("BLOCK_EMULATOR_EPOCH_AWARE_DISPATCH_TIMEOUT_MS", 60000),
		"route_version":                    m.routeVersion,
		"saa_decision_mode":                m.softDecisionMode,
		"saa_soft_decay_enabled":           m.softDecisionMode == "soft_decay",
		"saa_integrated_enabled":           m.softDecisionMode == saaIntegratedDecisionMode,
		"saa_mapping_authority":            "supervisor_single_writer",
		"saa_integrated_effect_lag_epochs": 1,
		"saa_observation_tau":              m.softDecay.ObservationTau,
		"saa_anchor_tau":                   m.softDecay.AnchorTau,
		"saa_persistence_tau":              m.softDecay.PersistenceTau,
		"saa_load_tau":                     m.softDecay.LoadTau,
		"saa_anchor_weight":                m.softDecay.AnchorWeight,
		"saa_locality_weight":              m.softDecay.LocalityWeight,
		"saa_persistence_weight":           m.softDecay.PersistenceWeight,
		"saa_cross_weight":                 m.softDecay.CrossWeight,
		"saa_load_weight":                  m.softDecay.LoadWeight,
		"saa_risk_weight":                  m.softDecay.RiskWeight,
		"saa_migration_temperature":        m.softDecay.Temperature,
		"saa_migration_margin":             m.softDecay.Margin,
		"saa_soft_audit_full":              m.saaSoftAuditFull,
	}
}

func writeRuntimeEffectiveConfiguration(role string, payload map[string]interface{}) {
	dir := strings.TrimSpace(os.Getenv(runtimeEffectiveConfigDirEnv))
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["role"] = role
	payload["pid"] = os.Getpid()
	payload["recorded_at_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	path := filepath.Join(dir, fmt.Sprintf("effective_configuration_%s_%d.json", role, os.Getpid()))
	_ = os.WriteFile(path, append(data, '\n'), 0o644)
}

func (m *RuntimeUnknownAdmissionManager) writeEffectiveConfiguration() {
	writeRuntimeEffectiveConfiguration("runtime_unknown_manager", m.effectiveConfiguration())
}

func NewRuntimeUnknownAdmissionManager(shardNum uint64, sl *supervisor_log.SupervisorLog) *RuntimeUnknownAdmissionManager {
	softConfig := defaultSAASoftDecayConfig()
	softConfig.ObservationTau = maxFloat(parseEnvFloat(runtimeUnknownObservationTauEnv, softConfig.ObservationTau), 0.001)
	softConfig.AnchorTau = maxFloat(parseEnvFloat(runtimeUnknownAnchorTauEnv, softConfig.AnchorTau), 0.001)
	softConfig.PersistenceTau = maxFloat(parseEnvFloat(runtimeUnknownPersistenceTauEnv, softConfig.PersistenceTau), 0.001)
	softConfig.LoadTau = maxFloat(parseEnvFloat(runtimeUnknownLoadTauEnv, softConfig.LoadTau), 0.001)
	softConfig.AnchorWeight = maxFloat(parseEnvFloat(runtimeUnknownSoftAnchorWeightEnv, softConfig.AnchorWeight), 0)
	softConfig.LocalityWeight = maxFloat(parseEnvFloat(runtimeUnknownSoftLocalityWeightEnv, softConfig.LocalityWeight), 0)
	softConfig.PersistenceWeight = maxFloat(parseEnvFloat(runtimeUnknownPersistenceWeightEnv, softConfig.PersistenceWeight), 0)
	softConfig.CrossWeight = maxFloat(parseEnvFloat(runtimeUnknownSoftCrossWeightEnv, softConfig.CrossWeight), 0)
	softConfig.LoadWeight = maxFloat(parseEnvFloat(runtimeUnknownSoftLoadWeightEnv, softConfig.LoadWeight), 0)
	softConfig.RiskWeight = maxFloat(parseEnvFloat(runtimeUnknownRiskWeightEnv, softConfig.RiskWeight), 0)
	softConfig.Temperature = maxFloat(parseEnvFloat(runtimeUnknownMigrationTemperatureEnv, softConfig.Temperature), 0.001)
	softConfig.Margin = parseEnvFloat(runtimeUnknownMigrationMarginEnv, softConfig.Margin)
	manager := &RuntimeUnknownAdmissionManager{
		enabled:                         parseEnvBool(runtimeUnknownAdmissionEnabledEnv, false),
		mappingBoundaryEnabled:          parseEnvBool(runtimeMappingBoundaryEnabledEnv, false),
		dynamicAdmissionEnabled:         parseEnvBool(runtimeUnknownDynamicEnabledEnv, true),
		externalActionEnabled:           parseEnvBool(runtimeExternalActionEnabledEnv, false),
		globalFallbackEnabled:           parseEnvBool(runtimeUnknownGlobalFallbackEnabledEnv, false),
		shardNum:                        shardNum,
		mappingUpdatePath:               resolveRuntimeMappingUpdatePath(),
		minAnchorTx:                     parseEnvInt(runtimeUnknownMinAnchorTxEnv, 1),
		minScoreShare:                   parseEnvFloat(runtimeUnknownMinScoreShareEnv, 0.6),
		localityWeight:                  parseEnvFloat(runtimeUnknownLocalityWeightEnv, 0.45),
		anchorWeight:                    parseEnvFloat(runtimeUnknownAnchorWeightEnv, 0.35),
		crossWeight:                     parseEnvFloat(runtimeUnknownCrossShardWeightEnv, 0.15),
		loadWeight:                      parseEnvFloat(runtimeUnknownLoadWeightEnv, 0.05),
		softDecisionMode:                parseSAADecisionMode(os.Getenv(runtimeUnknownDecisionModeEnv)),
		softDecay:                       softConfig,
		saaSoftStateByAccount:           make(map[string]*saaSoftAccountState),
		saaSoftLoadByShard:              make(map[uint64]saaSoftLoadState),
		saaSoftLoadLastRound:            -1,
		saaSoftAuditPath:                resolveSAASoftAuditPath(),
		saaSoftAuditFull:                parseEnvBool(runtimeUnknownSoftAuditFullEnv, false),
		saaSoftAuditLastRound:           make(map[string]int),
		overflowDiversionEnabled:        parseEnvBool(runtimeUnknownOverflowDiversionEnv, true),
		overflowRatio:                   maxFloat(parseEnvFloat(runtimeUnknownOverflowRatioEnv, 1.35), 1.05),
		overflowMinAdmissions:           maxInt(parseEnvInt(runtimeUnknownOverflowMinAdmissionsEnv, 1024), 1),
		strongAnchorProtectShare:        minFloat(maxFloat(parseEnvFloat(runtimeUnknownProtectAnchorShareEnv, 0.85), 0.0), 1.0),
		minDwellEpochs:                  maxInt(parseEnvInt(runtimeUnknownMinDwellEpochsEnv, 3), 1),
		strongAnchorDwellEpochs:         maxInt(parseEnvInt(runtimeUnknownStrongAnchorDwellEnv, 1), 1),
		routerSafetyLagEpochs:           maxInt(parseEnvInt(runtimeUnknownRouterSafetyLagEnv, 2), 0),
		refreshMaxPerEpoch:              maxInt(parseEnvInt(runtimeUnknownRefreshMaxPerEpochEnv, 128), 0),
		anchorRealignmentMode:           parseAnchorRealignmentMode(os.Getenv(runtimeAnchorRealignmentModeEnv)),
		anchorProposalPath:              strings.TrimSpace(os.Getenv(runtimeAnchorProposalPathEnv)),
		stableShardByAccount:            make(map[string]uint64),
		stableClusterByAccount:          make(map[string]int),
		stableShardByCluster:            make(map[int]uint64),
		externalShardByAccount:          make(map[string]uint64),
		runtimeShardByAccount:           make(map[string]uint64),
		pendingRuntimeShard:             make(map[string]uint64),
		pendingRuntimeRouteRound:        make(map[string]int),
		pendingRuntimeUpdate:            make(map[string]runtimeUnknownAdmissionUpdate),
		appliedExternalUpdates:          make(map[string]bool),
		shardAdmissionCounts:            make(map[uint64]int),
		observations:                    make(map[string]*runtimeUnknownObservation),
		lastRuntimeRefreshRound:         make(map[string]int),
		runtimeRefreshCountRound:        make(map[int]int),
		lastExternalRefreshRound:        -1,
		externalMappingAuditPath:        strings.TrimSpace(os.Getenv(runtimeExternalMappingAuditPathEnv)),
		mappingDecisionReadyPath:        strings.TrimSpace(os.Getenv(runtimeMappingDecisionReadyPathEnv)),
		mappingDecisionRequestPath:      strings.TrimSpace(os.Getenv(runtimeMappingDecisionRequestPathEnv)),
		mappingDecisionBarrierAuditPath: strings.TrimSpace(os.Getenv(runtimeMappingDecisionBarrierAuditEnv)),
		mappingDecisionBarrierTimeout:   time.Duration(maxInt(parseEnvInt(runtimeMappingDecisionBarrierTimeoutEnv, 30000), 1000)) * time.Millisecond,
		mappingDecisionMode:             parseMappingDecisionMode(os.Getenv(runtimeMappingDecisionModeEnv)),
		lastMappingDecisionBarrierRound: 1,
		acceptedMappingDecisionRounds:   make(map[int]bool),
		lastPressureHintRound:           -1,
		livePressureByShard:             make(map[uint64]float64),
		liveQueueByShard:                make(map[uint64]float64),
		routePressureWindowEpochs:       maxInt(parseEnvInt(runtimeUnknownRoutePressureWindowEnv, 4), 1),
		recentRoutePressure:             make(map[int]map[uint64]int),
		lastRouteRound:                  -1,
		routerStatusPath:                strings.TrimSpace(os.Getenv(runtimeUnknownRouterStatusPathEnv)),
		qpsaMode:                        parseQPSAMode(os.Getenv(runtimeUnknownQPSAModeEnv)),
		qpsaQueueWeight:                 maxFloat(parseEnvFloat(runtimeUnknownQPSAQueueWeightEnv, 0.25), 0.0),
		qpsaDemandCap:                   maxFloat(parseEnvFloat(runtimeUnknownQPSADemandCapEnv, 64.0), 1.0),
		qpsaMinRelativePotentialGain:    minFloat(maxFloat(parseEnvFloat(runtimeUnknownQPSAMinRelativeGainEnv, 0.05), 0.0), 1.0),
		qpsaAuditPath:                   strings.TrimSpace(os.Getenv(runtimeUnknownQPSAAuditPathEnv)),
		qpsaProjectedDemand:             make(map[int]map[uint64]float64),
		qpsaV2Mode:                      parseQPSAV2Mode(os.Getenv(runtimeUnknownQPSAV2ModeEnv)),
		qpsaV2QueueStatePath:            strings.TrimSpace(os.Getenv(runtimeUnknownQPSAV2QueueStatePathEnv)),
		qpsaV2AuditPath:                 strings.TrimSpace(os.Getenv(runtimeUnknownQPSAV2AuditPathEnv)),
		qpsaV2DriftWeight:               maxFloat(parseEnvFloat(runtimeUnknownQPSAV2DriftWeightEnv, 0.5), 0.0),
		qpsaV2LastRefreshRound:          -1,
		qpsaV2StateByShard:              make(map[uint64]qpsaV2QueueState),
		qpsaV2ProjectedDemand:           make(map[int]map[uint64]float64),
		sl:                              sl,
	}
	if !manager.enabled {
		// Dynamic admission is a subordinate capability. Keeping it false when
		// first-touch admission is disabled prevents an inherited shell value
		// from silently re-enabling unknown-account assignment.
		manager.dynamicAdmissionEnabled = false
	}
	accountClusterMapPath := strings.TrimSpace(os.Getenv(runtimeAccountClusterMapPathEnv))
	clusterShardMapPath := strings.TrimSpace(os.Getenv(runtimeClusterShardMapPathEnv))
	manager.stableShardByAccount, manager.stableClusterByAccount, manager.stableShardByCluster = loadStableAccountAnchorMaps(accountClusterMapPath, clusterShardMapPath)
	if !manager.enabled && !manager.mappingBoundaryEnabled && len(manager.stableShardByAccount) == 0 {
		manager.writeEffectiveConfiguration()
		return manager
	}
	if manager.enabled && len(manager.stableShardByAccount) == 0 {
		manager.enabled = false
	}
	if manager.dynamicAdmissionEnabled && manager.mappingUpdatePath == "" && manager.softDecisionMode != saaIntegratedDecisionMode {
		manager.dynamicAdmissionEnabled = false
	}
	if manager.mappingDecisionMode == "async_pipeline_lag1" {
		// Prime the first future-effective slot before epoch 1 starts. Later
		// slots are advanced atomically at each routing boundary.
		publishMappingDecisionRequest(manager.mappingDecisionRequestPath, 1, 2)
	}
	manager.writeEffectiveConfiguration()
	return manager
}

func resolveRuntimeMappingUpdatePath() string {
	dedicatedPath := strings.TrimSpace(os.Getenv(runtimeDedicatedMappingUpdatePathEnv))
	if dedicatedPath != "" {
		return dedicatedPath
	}
	return strings.TrimSpace(os.Getenv(runtimeExternalMappingUpdatePathEnv))
}

func (m *RuntimeUnknownAdmissionManager) Enabled() bool {
	return m != nil && (m.enabled || m.mappingBoundaryEnabled || len(m.stableShardByAccount) > 0)
}

// CurrentRouteVersion returns the version of the complete supervisor routing
// map used for subsequently frozen transactions. The version advances for
// every effective runtime admission or external mapping record.
func (m *RuntimeUnknownAdmissionManager) CurrentRouteVersion() uint64 {
	if m == nil || !m.Enabled() {
		return 0
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.softDecisionMode == saaIntegratedDecisionMode {
		return m.integratedStateLocked().authority.routeVersion
	}
	return m.routeVersion
}

// CloseMappingDecisions consumes the final injectable routing slot and closes
// the request channel under the same lock used by off-chain batch commits.
// The final batch can therefore use an already committed READY decision, while
// any later planner work deterministically loses commit rights.
func (m *RuntimeUnknownAdmissionManager) CloseMappingDecisions(finalLogicalRound int) {
	if m == nil || !m.mappingBoundaryEnabled || finalLogicalRound <= 1 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.mappingDecisionsClosed {
		return
	}
	started := time.Now()
	var ready *mappingDecisionReadyRecord
	accepted := false
	closedPublished := false
	alreadyAudited := finalLogicalRound <= m.lastMappingDecisionBarrierRound
	locked := withMappingDecisionCommitLock(m.mappingDecisionRequestPath, func() {
		if !alreadyAudited && m.mappingDecisionMode == "async_pipeline_lag1" {
			ready, accepted = readMappingDecisionReady(
				m.mappingDecisionReadyPath,
				finalLogicalRound-1,
				finalLogicalRound,
			)
		}
		closedPublished = publishMappingDecisionClosed(
			m.mappingDecisionRequestPath,
			finalLogicalRound,
		)
	})
	if !locked || !closedPublished {
		panic(fmt.Sprintf(
			"failed to close mapping decision channel at final logical round %d",
			finalLogicalRound,
		))
	}
	m.mappingDecisionsClosed = true
	if alreadyAudited {
		// Multiple injected batches may share the same completed-chain epoch.
		// Closing the decision channel in that epoch is a lifecycle event, not
		// a second routing boundary.
		return
	}
	m.acceptedMappingDecisionRounds[finalLogicalRound] = accepted
	m.lastMappingDecisionBarrierRound = maxInt(
		m.lastMappingDecisionBarrierRound,
		finalLogicalRound,
	)
	status := "NOOP_PIPELINE_FINAL_BATCH"
	if accepted {
		status = "ACCEPTED_PIPELINE_FINAL_BATCH"
	} else {
		// A terminal no-op must not carry a stale READY payload that could be
		// mistaken for an accepted decision by offline audit.
		ready = nil
	}
	appendMappingDecisionBarrierAudit(
		m.mappingDecisionBarrierAuditPath,
		finalLogicalRound,
		status,
		ready,
		time.Since(started),
	)
}

func (m *RuntimeUnknownAdmissionManager) RouteSender(tx *core.Transaction, logicalRound int) (uint64, bool) {
	if m == nil || !m.Enabled() || tx == nil {
		return 0, false
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.awaitMappingDecisionReadyLocked(logicalRound) {
		panic(fmt.Sprintf("mapping decision barrier timed out for logical round %d", logicalRound))
	}
	m.refreshExternalMappingUpdatesLocked(logicalRound)
	m.refreshRuntimePressureHintLocked(logicalRound)
	if m.softDecisionMode == saaIntegratedDecisionMode {
		m.applySAAIntegratedDueLocked(logicalRound)
	} else {
		m.promoteRuntimeAssignmentsLocked(logicalRound)
	}
	if !m.enabled || !m.dynamicAdmissionEnabled {
		if shard, ok := m.lookupAssignedShardLocked(tx.Sender); ok {
			return shard, true
		}
		return 0, false
	}

	m.observePairLocked(tx.Sender, tx.Recipient, logicalRound)
	m.observePairLocked(tx.Recipient, tx.Sender, logicalRound)

	m.maybeAssignLocked(tx.Sender, logicalRound)
	m.maybeAssignLocked(tx.Recipient, logicalRound)

	if shard, ok := m.lookupAssignedShardLocked(tx.Sender); ok {
		m.recordRoutedPressureLocked(shard, tx.Recipient, logicalRound)
		return shard, true
	}
	return 0, false
}

// CurrentShard returns the supervisor-authoritative recipient route at the
// same epoch boundary used for the sender. It does not add another locality
// observation, so each injected transaction updates admission evidence once.
func (m *RuntimeUnknownAdmissionManager) CurrentShard(account string, logicalRound int) (uint64, bool) {
	if m == nil || !m.Enabled() {
		return 0, false
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.awaitMappingDecisionReadyLocked(logicalRound) {
		panic(fmt.Sprintf("mapping decision barrier timed out for logical round %d", logicalRound))
	}
	m.refreshExternalMappingUpdatesLocked(logicalRound)
	m.refreshRuntimePressureHintLocked(logicalRound)
	if m.softDecisionMode == saaIntegratedDecisionMode {
		m.applySAAIntegratedDueLocked(logicalRound)
	} else {
		m.promoteRuntimeAssignmentsLocked(logicalRound)
	}
	return m.lookupAssignedShardLocked(account)
}

type mappingDecisionReadyRecord struct {
	Status             string `json:"status"`
	RequestID          string `json:"request_id"`
	DecisionEpoch      int    `json:"decision_epoch"`
	EffectiveEpoch     int    `json:"effective_epoch"`
	EmittedUpdateCount int    `json:"emitted_update_count"`
	MappingAuthority   string `json:"mapping_authority"`
}

func (m *RuntimeUnknownAdmissionManager) awaitMappingDecisionReadyLocked(logicalRound int) bool {
	if m == nil || !m.mappingBoundaryEnabled || logicalRound <= 1 || logicalRound <= m.lastMappingDecisionBarrierRound {
		return true
	}
	if m.mappingDecisionsClosed {
		m.lastMappingDecisionBarrierRound = logicalRound
		return true
	}
	if m.mappingDecisionReadyPath == "" {
		m.lastMappingDecisionBarrierRound = logicalRound
		return true
	}
	started := time.Now()
	if m.mappingDecisionMode == "async_pipeline_lag1" {
		ready, accepted := readMappingDecisionReady(
			m.mappingDecisionReadyPath,
			logicalRound-1,
			logicalRound,
		)
		// READY is normally published during the preceding epoch. Retain only
		// a short filesystem-race grace period; planner delay becomes an
		// explicit no-op and never stalls transaction routing.
		if !accepted && ready != nil && ready.EffectiveEpoch == logicalRound-1 {
			graceDeadline := time.Now().Add(20 * time.Millisecond)
			for !accepted && time.Now().Before(graceDeadline) {
				time.Sleep(time.Millisecond)
				ready, accepted = readMappingDecisionReady(
					m.mappingDecisionReadyPath,
					logicalRound-1,
					logicalRound,
				)
			}
		}
		// Serialize the routing boundary with the off-chain commit. The critical
		// section only appends the staged batch and atomically renames READY:
		// whichever side acquires the lock first owns this effective epoch.
		// This prevents an update from being committed after the boundary has
		// already recorded the same epoch as a no-op.
		locked := withMappingDecisionCommitLock(m.mappingDecisionRequestPath, func() {
			ready, accepted = readMappingDecisionReady(
				m.mappingDecisionReadyPath,
				logicalRound-1,
				logicalRound,
			)
			if !publishMappingDecisionRequest(
				m.mappingDecisionRequestPath,
				logicalRound,
				logicalRound+1,
			) && m.sl != nil {
				m.sl.Slog.Printf(
					"Failed to publish pipelined mapping request: logicalRound=%d requestPath=%s\n",
					logicalRound,
					m.mappingDecisionRequestPath,
				)
			}
		})
		if !locked {
			accepted = false
			if m.sl != nil {
				m.sl.Slog.Printf(
					"Failed to acquire pipelined mapping commit lock: logicalRound=%d requestPath=%s\n",
					logicalRound,
					m.mappingDecisionRequestPath,
				)
			}
		}
		m.acceptedMappingDecisionRounds[logicalRound] = accepted
		m.lastMappingDecisionBarrierRound = logicalRound
		status := "NOOP_PIPELINE_NOT_READY"
		if accepted {
			status = "ACCEPTED_PIPELINE"
		}
		appendMappingDecisionBarrierAudit(
			m.mappingDecisionBarrierAuditPath,
			logicalRound,
			status,
			ready,
			time.Since(started),
		)
		return true
	}
	if m.mappingDecisionMode == "async_double_buffer" {
		ready, accepted := readMappingDecisionReady(
			m.mappingDecisionReadyPath,
			logicalRound-2,
			logicalRound,
		)
		// A READY rename can race with the boundary's first read by a few
		// milliseconds. Wait only when the observed record is exactly one
		// round old; a genuinely stalled planner still falls through to no-op.
		if !accepted && ready != nil && ready.EffectiveEpoch == logicalRound-1 {
			graceDeadline := time.Now().Add(100 * time.Millisecond)
			for !accepted && time.Now().Before(graceDeadline) {
				time.Sleep(2 * time.Millisecond)
				ready, accepted = readMappingDecisionReady(
					m.mappingDecisionReadyPath,
					logicalRound-2,
					logicalRound,
				)
			}
		}
		// Serialize the final READY check with the controller's batch commit.
		// Either the controller publishes updates+READY first and this boundary
		// consumes them, or this boundary advances the request first and the
		// controller discards the now-stale batch.
		locked := withMappingDecisionCommitLock(m.mappingDecisionRequestPath, func() {
			ready, accepted = readMappingDecisionReady(
				m.mappingDecisionReadyPath,
				logicalRound-2,
				logicalRound,
			)
			if !publishMappingDecisionRequest(m.mappingDecisionRequestPath, logicalRound-1, logicalRound+1) && m.sl != nil {
				m.sl.Slog.Printf(
					"Failed to publish asynchronous mapping request: logicalRound=%d requestPath=%s\n",
					logicalRound,
					m.mappingDecisionRequestPath,
				)
			}
		})
		if !locked {
			accepted = false
			if m.sl != nil {
				m.sl.Slog.Printf(
					"Failed to acquire mapping decision commit lock: logicalRound=%d requestPath=%s\n",
					logicalRound,
					m.mappingDecisionRequestPath,
				)
			}
		}
		m.acceptedMappingDecisionRounds[logicalRound] = accepted
		m.lastMappingDecisionBarrierRound = logicalRound
		appendMappingDecisionBarrierAudit(
			m.mappingDecisionBarrierAuditPath,
			logicalRound,
			map[bool]string{true: "ACCEPTED_ASYNC", false: "NOOP_ASYNC_NOT_READY"}[accepted],
			ready,
			time.Since(started),
		)
		return true
	}
	deadline := started.Add(m.mappingDecisionBarrierTimeout)
	if !publishMappingDecisionRequest(m.mappingDecisionRequestPath, logicalRound-1, logicalRound) {
		if m.sl != nil {
			m.sl.Slog.Printf(
				"Failed to publish mapping decision request: logicalRound=%d requestPath=%s\n",
				logicalRound,
				m.mappingDecisionRequestPath,
			)
		}
		return false
	}
	for time.Now().Before(deadline) {
		ready, accepted := readMappingDecisionReady(
			m.mappingDecisionReadyPath,
			logicalRound-1,
			logicalRound,
		)
		if accepted {
			m.lastMappingDecisionBarrierRound = logicalRound
			m.acceptedMappingDecisionRounds[logicalRound] = true
			appendMappingDecisionBarrierAudit(
				m.mappingDecisionBarrierAuditPath,
				logicalRound,
				"ACCEPTED",
				ready,
				time.Since(started),
			)
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	if m.sl != nil {
		m.sl.Slog.Printf(
			"Mapping decision barrier timeout: logicalRound=%d readyPath=%s timeout=%s\n",
			logicalRound,
			m.mappingDecisionReadyPath,
			m.mappingDecisionBarrierTimeout,
		)
	}
	return false
}

func withMappingDecisionCommitLock(requestPath string, fn func()) bool {
	if requestPath == "" || fn == nil {
		return false
	}
	lockPath := filepath.Join(filepath.Dir(requestPath), "mapping_decision_commit.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return false
	}
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return false
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return false
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	fn()
	return true
}

func tryWithMappingDecisionCommitLock(requestPath string, fn func()) bool {
	if requestPath == "" || fn == nil {
		return false
	}
	lockPath := filepath.Join(filepath.Dir(requestPath), "mapping_decision_commit.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return false
	}
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return false
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return false
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	fn()
	return true
}

func readMappingDecisionReady(
	readyPath string,
	expectedDecisionEpoch int,
	expectedEffectiveEpoch int,
) (*mappingDecisionReadyRecord, bool) {
	data, err := os.ReadFile(readyPath)
	if err != nil || len(data) == 0 {
		return nil, false
	}
	ready := new(mappingDecisionReadyRecord)
	if json.Unmarshal(data, ready) != nil {
		return nil, false
	}
	accepted := strings.EqualFold(strings.TrimSpace(ready.Status), "READY") &&
		ready.RequestID == mappingDecisionRequestID(expectedDecisionEpoch, expectedEffectiveEpoch) &&
		ready.DecisionEpoch == expectedDecisionEpoch &&
		ready.EffectiveEpoch == expectedEffectiveEpoch &&
		ready.MappingAuthority == "supervisor_frozen_route"
	return ready, accepted
}

func publishMappingDecisionRequest(requestPath string, decisionEpoch int, effectiveEpoch int) bool {
	if requestPath == "" {
		return false
	}
	if err := os.MkdirAll(filepath.Dir(requestPath), 0o755); err != nil {
		return false
	}
	record := map[string]interface{}{
		"written_at_utc":    time.Now().UTC().Format(time.RFC3339Nano),
		"status":            "REQUESTED",
		"request_id":        mappingDecisionRequestID(decisionEpoch, effectiveEpoch),
		"decision_epoch":    decisionEpoch,
		"effective_epoch":   effectiveEpoch,
		"mapping_authority": "supervisor_frozen_route",
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return false
	}
	temporaryPath := fmt.Sprintf("%s.%d.tmp", requestPath, os.Getpid())
	if err := os.WriteFile(temporaryPath, payload, 0o644); err != nil {
		return false
	}
	if err := os.Rename(temporaryPath, requestPath); err != nil {
		_ = os.Remove(temporaryPath)
		return false
	}
	if !appendMappingDecisionRequestAudit(requestPath, payload) {
		return false
	}
	return true
}

func mappingDecisionRequestID(decisionEpoch int, effectiveEpoch int) string {
	return fmt.Sprintf("mapping-request-d%d-e%d", decisionEpoch, effectiveEpoch)
}

func appendMappingDecisionRequestAudit(requestPath string, payload []byte) bool {
	auditPath := filepath.Join(filepath.Dir(requestPath), "mapping_decision_request_audit.jsonl")
	handle, err := os.OpenFile(auditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return false
	}
	defer handle.Close()
	line := append(append([]byte(nil), payload...), '\n')
	written, err := handle.Write(line)
	return err == nil && written == len(line)
}

func publishMappingDecisionClosed(requestPath string, finalLogicalRound int) bool {
	if requestPath == "" {
		return false
	}
	if err := os.MkdirAll(filepath.Dir(requestPath), 0o755); err != nil {
		return false
	}
	record := map[string]interface{}{
		"written_at_utc":      time.Now().UTC().Format(time.RFC3339Nano),
		"status":              "CLOSED",
		"final_logical_round": finalLogicalRound,
		"mapping_authority":   "supervisor_frozen_route",
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return false
	}
	temporaryPath := fmt.Sprintf("%s.%d.tmp", requestPath, os.Getpid())
	if err := os.WriteFile(temporaryPath, payload, 0o644); err != nil {
		return false
	}
	if err := os.Rename(temporaryPath, requestPath); err != nil {
		_ = os.Remove(temporaryPath)
		return false
	}
	return true
}

func appendMappingDecisionBarrierAudit(
	auditPath string,
	logicalRound int,
	status string,
	ready *mappingDecisionReadyRecord,
	waited time.Duration,
) {
	if auditPath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(auditPath), 0o755); err != nil {
		return
	}
	record := map[string]interface{}{
		"recorded_at_utc":       time.Now().UTC().Format(time.RFC3339Nano),
		"status":                status,
		"logical_round":         logicalRound,
		"decision_epoch":        -1,
		"effective_epoch":       logicalRound,
		"emitted_update_count":  0,
		"mapping_authority":     "supervisor_frozen_route",
		"waited_ms":             float64(waited.Microseconds()) / 1000.0,
		"route_boundary_frozen": true,
	}
	if ready != nil {
		record["decision_epoch"] = ready.DecisionEpoch
		record["effective_epoch"] = ready.EffectiveEpoch
		record["emitted_update_count"] = ready.EmittedUpdateCount
		record["mapping_authority"] = ready.MappingAuthority
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return
	}
	file, err := os.OpenFile(auditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(payload, '\n'))
}

func (m *RuntimeUnknownAdmissionManager) observePairLocked(target string, peer string, logicalRound int) {
	targetAddr := normalizeAdmissionAddress(target)
	peerAddr := normalizeAdmissionAddress(peer)
	if targetAddr == "" || peerAddr == "" {
		return
	}
	targetRuntimeObserved := m.isRuntimeObservedAccountLocked(targetAddr)
	if !targetRuntimeObserved {
		if _, known := m.lookupAssignedShardLocked(targetAddr); known {
			return
		}
	} else if _, externallyOverridden := m.externalShardByAccount[targetAddr]; externallyOverridden {
		return
	} else if _, stableAccount := m.stableShardByAccount[targetAddr]; stableAccount {
		return
	}
	peerShard, peerKnown := m.lookupAssignedShardLocked(peerAddr)
	if !peerKnown {
		return
	}
	obs := m.observations[targetAddr]
	if obs == nil {
		obs = &runtimeUnknownObservation{
			FirstObservedRound:    logicalRound,
			LastObservedRound:     logicalRound,
			ShardEvidence:         make(map[uint64]int),
			StableShardEvidence:   make(map[uint64]int),
			StableClusterEvidence: make(map[int]int),
		}
		m.observations[targetAddr] = obs
	}
	obs.LastObservedRound = logicalRound
	obs.TotalTxCount++
	obs.ShardEvidence[peerShard]++
	if clusterID, stableKnown := m.stableClusterByAccount[peerAddr]; stableKnown {
		obs.StableShardEvidence[peerShard]++
		obs.StableClusterEvidence[clusterID]++
	}
	if targetRuntimeObserved {
		m.maybeEmitRuntimeObservationRefreshLocked(targetAddr, logicalRound, obs)
	}
}

func (m *RuntimeUnknownAdmissionManager) isRuntimeObservedAccountLocked(account string) bool {
	if m.softDecisionMode == saaIntegratedDecisionMode {
		return m.isSAAIntegratedObservedLocked(account)
	}
	if _, ok := m.pendingRuntimeShard[account]; ok {
		return true
	}
	if _, ok := m.runtimeShardByAccount[account]; ok {
		return true
	}
	return false
}

func (m *RuntimeUnknownAdmissionManager) maybeAssignLocked(account string, logicalRound int) {
	accountAddr := normalizeAdmissionAddress(account)
	if accountAddr == "" {
		return
	}
	if _, known := m.lookupAssignedShardLocked(accountAddr); known {
		return
	}
	obs := m.observations[accountAddr]
	if m.softDecisionMode == saaIntegratedDecisionMode {
		m.decideSAAIntegratedLocked(accountAddr, obs, logicalRound, true)
		return
	}
	if m.softDecisionMode == "soft_decay" {
		currentShard := uint64(utils.Addr2Shard(accountAddr))
		decision := m.decideSAASoftTargetLocked(accountAddr, obs, logicalRound, currentShard, true)
		if decision.Action != "assign" {
			return
		}
		m.assignLocked(accountAddr, decision.SelectedShard, logicalRound, "soft_decay_unified_policy", decision.Reason, decision.MigrationIntensity, obs)
		state := m.saaSoftStateByAccount[accountAddr]
		state.LastTarget = decision.SelectedShard
		state.HasTarget = true
		return
	}
	m.maybeAssignLegacyLocked(accountAddr, obs, logicalRound)
}

func (m *RuntimeUnknownAdmissionManager) assignLocked(account string, targetShard uint64, logicalRound int, mode string, reason string, confidence float64, obs *runtimeUnknownObservation) {
	if _, exists := m.runtimeShardByAccount[account]; exists {
		return
	}
	if _, exists := m.pendingRuntimeShard[account]; exists {
		return
	}
	sourceShard := uint64(utils.Addr2Shard(account))
	routeEligibleEpoch := logicalRound + m.minDwellEpochs
	if m.softDecisionMode == "soft_decay" {
		routeEligibleEpoch = logicalRound + 1
	}
	if mode == "runtime_strong_stable_anchor" {
		routeEligibleEpoch = logicalRound + minInt(m.strongAnchorDwellEpochs, m.minDwellEpochs)
	}
	observedTxCount := 0
	anchorTxCount := 0
	anchorScoreShare := 0.0
	if obs != nil {
		observedTxCount = obs.TotalTxCount
		anchorTxCount = obs.StableShardEvidence[targetShard]
		anchorScoreShare = share(anchorTxCount, obs.TotalTxCount)
	}
	parentStableObjectID := ""
	var stableClusterIDPtr *int
	if clusterID, ok := m.dominantStableClusterForShardLocked(obs, targetShard); ok {
		clusterIDCopy := clusterID
		stableClusterIDPtr = &clusterIDCopy
		parentStableObjectID = "stable:" + strconv.Itoa(clusterID)
	}
	m.pendingRuntimeShard[account] = targetShard
	m.pendingRuntimeRouteRound[account] = routeEligibleEpoch
	m.shardAdmissionCounts[targetShard]++
	m.lastRuntimeRefreshRound[account] = logicalRound
	activePressure, pressureSignalSource, pressureSignalEpoch := m.activePressureByShardLocked()
	update := runtimeUnknownAdmissionUpdate{
		ProposalID:           "runtime_unknown_admission:" + account + ":" + strconv.Itoa(logicalRound),
		EpochID:              logicalRound,
		EffectiveEpoch:       routeEligibleEpoch,
		ObjectType:           "runtime_unknown_account",
		ObjectID:             account,
		SourceShard:          sourceShard,
		TargetShard:          targetShard,
		Addresses:            []string{account},
		ResolvedAccountCount: 1,
		ApplyMode:            "runtime_unknown_admission_pending_observation",
		PlacementMode:        mode,
		PlacementReason:      reason,
		PlacementConfidence:  confidence,
		ObservedTxCount:      observedTxCount,
		AnchorTxCount:        anchorTxCount,
		AnchorScoreShare:     anchorScoreShare,
		ParentStableObjectID: parentStableObjectID,
		ProjectionClusterID:  stableClusterIDPtr,
		AnchorClusterID:      stableClusterIDPtr,
		StableClusterID:      stableClusterIDPtr,
		PressureHintEpoch:    pressureSignalEpoch,
		TargetPressureScore:  activePressure[targetShard],
		PressureSignalSource: pressureSignalSource,
		ControlChannel:       "anchor_initial_admission",
		ProposalKind:         "initial_placement",
	}
	m.pendingRuntimeUpdate[account] = update
	// Write the already-observed admission candidate immediately so the Python
	// runtime micro-cluster tracker can see it during the same native run. The
	// future EffectiveEpoch preserves dwell/sticky placement on-chain: chain
	// hooks will not apply this update before it is route-eligible.
	if m.dynamicAdmissionEnabled {
		mustAppendAdmissionUpdateJSONL(m.mappingUpdatePath, update)
	}
}

func (m *RuntimeUnknownAdmissionManager) maybeEmitRuntimeObservationRefreshLocked(account string, logicalRound int, obs *runtimeUnknownObservation) {
	if !m.dynamicAdmissionEnabled || obs == nil {
		return
	}
	if m.softDecisionMode == saaIntegratedDecisionMode {
		m.decideSAAIntegratedLocked(account, obs, logicalRound, false)
		return
	}
	if m.mappingUpdatePath == "" {
		return
	}
	if m.softDecisionMode == "soft_decay" {
		if _, pending := m.pendingRuntimeShard[account]; pending {
			return
		}
		currentShard, ok := m.runtimeShardByAccount[account]
		if !ok {
			return
		}
		decision := m.decideSAASoftTargetLocked(account, obs, logicalRound, currentShard, false)
		m.scheduleSAASoftRemapLocked(account, logicalRound, currentShard, decision, obs)
		return
	}
	m.maybeEmitLegacyObservationRefreshLocked(account, logicalRound, obs)
}

func (m *RuntimeUnknownAdmissionManager) dominantStableClusterForShardLocked(obs *runtimeUnknownObservation, targetShard uint64) (int, bool) {
	if obs == nil || len(obs.StableClusterEvidence) == 0 {
		return 0, false
	}
	bestClusterID := 0
	bestCount := -1
	for clusterID, count := range obs.StableClusterEvidence {
		if shardID, ok := m.stableShardByCluster[clusterID]; ok && shardID != targetShard {
			continue
		}
		if count > bestCount || (count == bestCount && (bestCount < 0 || clusterID < bestClusterID)) {
			bestClusterID = clusterID
			bestCount = count
		}
	}
	if bestCount <= 0 {
		return 0, false
	}
	return bestClusterID, true
}

func (m *RuntimeUnknownAdmissionManager) promoteRuntimeAssignmentsLocked(logicalRound int) {
	if m.softDecisionMode == saaIntegratedDecisionMode {
		return
	}
	if len(m.pendingRuntimeRouteRound) == 0 {
		return
	}
	promoted := 0
	safetyLag := m.routerSafetyLagEpochs
	if m.softDecisionMode == "soft_decay" {
		safetyLag = 0
	}
	for account, routeRound := range m.pendingRuntimeRouteRound {
		if logicalRound < routeRound+safetyLag {
			continue
		}
		targetShard, ok := m.pendingRuntimeShard[account]
		if !ok {
			delete(m.pendingRuntimeRouteRound, account)
			delete(m.pendingRuntimeUpdate, account)
			continue
		}
		if _, hasExternal := m.externalShardByAccount[account]; !hasExternal {
			update, hasUpdate := m.pendingRuntimeUpdate[account]
			if hasUpdate && update.EffectiveEpoch <= 0 {
				update.EpochID = logicalRound
				update.EffectiveEpoch = logicalRound + 1
				mustAppendAdmissionUpdateJSONL(m.mappingUpdatePath, update)
			}
			m.runtimeShardByAccount[account] = targetShard
			m.routeVersion++
			if hasUpdate {
				appendSupervisorRoutingMappingAudit(
					m.externalMappingAuditPath,
					m.mappingUpdatePath,
					logicalRound,
					update.ProposalID,
					&update,
					1,
					0,
					m.routeVersion,
					m.externalRollingHash,
				)
			}
			promoted++
		}
		delete(m.pendingRuntimeShard, account)
		delete(m.pendingRuntimeRouteRound, account)
		delete(m.pendingRuntimeUpdate, account)
	}
	if promoted > 0 && m.sl != nil {
		m.sl.Slog.Printf("Supervisor runtime-unknown admission promoted: observedEpoch=%d accounts=%d routerSafetyLagEpochs=%d\n", logicalRound, promoted, m.routerSafetyLagEpochs)
	}
}

func (m *RuntimeUnknownAdmissionManager) lookupAssignedShardLocked(account string) (uint64, bool) {
	accountAddr := normalizeAdmissionAddress(account)
	if accountAddr == "" {
		return 0, false
	}
	if m.softDecisionMode == saaIntegratedDecisionMode {
		return m.lookupSAAIntegratedLocked(accountAddr)
	}
	// DRL migration updates must override earlier runtime admissions. Runtime
	// unknown placement is an admission-time default, not a permanent routing
	// lock once the online scheduler has made a newer decision.
	if shardID, ok := m.externalShardByAccount[accountAddr]; ok {
		return shardID, true
	}
	if shardID, ok := m.runtimeShardByAccount[accountAddr]; ok {
		return shardID, true
	}
	shardID, ok := m.stableShardByAccount[accountAddr]
	return shardID, ok
}

func (m *RuntimeUnknownAdmissionManager) recordRoutedPressureLocked(senderShard uint64, recipient string, logicalRound int) {
	if m.lastRouteRound >= 0 && logicalRound != m.lastRouteRound {
		m.emitRouterPressureStatusLocked(m.lastRouteRound)
	}
	m.lastRouteRound = logicalRound
	counts := m.recentRoutePressure[logicalRound]
	if counts == nil {
		counts = make(map[uint64]int)
		m.recentRoutePressure[logicalRound] = counts
	}
	counts[senderShard]++
	if recipientShard, ok := m.lookupAssignedShardLocked(normalizeAdmissionAddress(recipient)); ok && recipientShard != senderShard {
		// Relay cross-shard execution pressures both participating shards.
		counts[recipientShard]++
	}
	cutoff := logicalRound - m.routePressureWindowEpochs + 1
	for epoch := range m.recentRoutePressure {
		if epoch < cutoff {
			delete(m.recentRoutePressure, epoch)
		}
	}
}

func (m *RuntimeUnknownAdmissionManager) emitRouterPressureStatusLocked(logicalRound int) {
	if m.routerStatusPath == "" {
		return
	}
	pressureByShard, signalSource, _ := m.activePressureByShardLocked()
	if len(pressureByShard) != int(m.shardNum) {
		return
	}
	total := 0.0
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		total += pressureByShard[shardID]
	}
	average := total / maxFloat(1.0, float64(m.shardNum))
	status := runtimeRouterPressureStatus{
		LogicalRound: logicalRound,
		SignalSource: signalSource,
		Shards:       make([]runtimeRouterShardStatus, 0, m.shardNum),
	}
	coldestShard := uint64(0)
	coldestPressure := 1e18
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		pressure := pressureByShard[shardID]
		isHot := pressure >= average*1.18
		isCold := pressure <= average*0.86
		if pressure < coldestPressure {
			coldestPressure = pressure
			coldestShard = shardID
		}
		if isHot {
			status.HotShards = append(status.HotShards, shardID)
		}
		if isCold {
			status.ColdShards = append(status.ColdShards, shardID)
		}
		status.Shards = append(status.Shards, runtimeRouterShardStatus{
			ShardID:       shardID,
			PressureScore: pressure,
			IsHot:         isHot,
			IsCold:        isCold,
		})
	}
	if len(status.ColdShards) == 0 && m.shardNum > 0 {
		status.ColdShards = append(status.ColdShards, coldestShard)
		for index := range status.Shards {
			if status.Shards[index].ShardID == coldestShard {
				status.Shards[index].IsCold = true
				break
			}
		}
	}
	data, err := json.Marshal(status)
	if err != nil {
		return
	}
	tmpPath := m.routerStatusPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmpPath, m.routerStatusPath)
	appendRouterPressureStatusJSONL(m.routerStatusPath+".history.jsonl", status)
}

func (m *RuntimeUnknownAdmissionManager) activePressureByShardLocked() (map[uint64]float64, string, int) {
	routeCounts := make(map[uint64]int, m.shardNum)
	totalRoutes := 0
	for _, epochCounts := range m.recentRoutePressure {
		for shardID, count := range epochCounts {
			routeCounts[shardID] += count
			totalRoutes += count
		}
	}
	routePressure := make(map[uint64]float64, m.shardNum)
	if totalRoutes > 0 {
		avgRoutes := float64(totalRoutes) / float64(m.shardNum)
		for shardID := uint64(0); shardID < m.shardNum; shardID++ {
			routePressure[shardID] = float64(routeCounts[shardID]) / maxFloat(1.0, avgRoutes)
		}
	}
	hintIsFresh := len(m.livePressureByShard) == int(m.shardNum) &&
		m.pressureHintEpoch > 0 && m.lastRouteRound-m.pressureHintEpoch <= 2
	if hintIsFresh && len(routePressure) > 0 {
		blended := make(map[uint64]float64, m.shardNum)
		for shardID := uint64(0); shardID < m.shardNum; shardID++ {
			blended[shardID] = 0.65*m.livePressureByShard[shardID] + 0.35*routePressure[shardID]
		}
		return blended, "fresh_snapshot_plus_recent_routed_workload", m.pressureHintEpoch
	}
	if len(routePressure) > 0 {
		return routePressure, "recent_routed_workload", m.lastRouteRound
	}
	if len(m.livePressureByShard) == int(m.shardNum) {
		return m.livePressureByShard, "stale_snapshot_fallback", m.pressureHintEpoch
	}
	return nil, "", 0
}

func (m *RuntimeUnknownAdmissionManager) refreshRuntimePressureHintLocked(logicalRound int) {
	if m == nil || !m.enabled || logicalRound == m.lastPressureHintRound {
		return
	}
	m.lastPressureHintRound = logicalRound
	path := strings.TrimSpace(os.Getenv(runtimeUnknownPressureHintPathEnv))
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	hint := new(runtimeShardPressureHint)
	if err := json.Unmarshal(data, hint); err != nil || len(hint.Shards) == 0 {
		return
	}
	pressureByShard := make(map[uint64]float64, m.shardNum)
	queueByShard := make(map[uint64]float64, m.shardNum)
	for _, shard := range hint.Shards {
		if shard.ShardID >= m.shardNum || shard.PressureScore < 0.0 || shard.QueueLength < 0.0 {
			continue
		}
		pressureByShard[shard.ShardID] = shard.PressureScore
		queueByShard[shard.ShardID] = shard.QueueLength
	}
	if len(pressureByShard) != int(m.shardNum) || len(queueByShard) != int(m.shardNum) {
		return
	}
	m.livePressureByShard = pressureByShard
	m.liveQueueByShard = queueByShard
	m.pressureHintEpoch = hint.BoundaryEpoch
}

func (m *RuntimeUnknownAdmissionManager) refreshExternalMappingUpdatesLocked(logicalRound int) {
	if m == nil || !m.Enabled() || !m.externalActionEnabled || logicalRound == m.lastExternalRefreshRound {
		return
	}
	// Forecast envelopes have their own version/queue checks. Never reinterpret
	// a legacy scheduler record as an authorized forecast proposal.
	if m.softDecisionMode == saaIntegratedDecisionMode && m.integratedStateLocked().controllerMode == forecastControllerMode {
		return
	}
	m.lastExternalRefreshRound = logicalRound
	path := strings.TrimSpace(os.Getenv(runtimeExternalMappingUpdatePathEnv))
	if path == "" {
		return
	}
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 32*1024*1024)
	appliedRecords := 0
	appliedAccounts := 0
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		update := new(runtimeUnknownAdmissionUpdate)
		if err := json.Unmarshal([]byte(line), update); err != nil {
			continue
		}
		updateKey := update.ProposalID
		if updateKey == "" {
			updateKey = path + ":" + strconv.Itoa(lineNumber)
		}
		if m.appliedExternalUpdates[updateKey] {
			continue
		}
		if m.mappingDecisionMode == "async_double_buffer" {
			// Async decisions are valid only at their declared boundary. Never
			// apply a late update in the middle of a later epoch.
			if update.EffectiveEpoch < logicalRound {
				m.appliedExternalUpdates[updateKey] = true
				continue
			}
			if update.EffectiveEpoch > logicalRound {
				continue
			}
			if !m.acceptedMappingDecisionRounds[logicalRound] {
				m.appliedExternalUpdates[updateKey] = true
				continue
			}
		}
		// External actions already carry an explicit effective epoch and are
		// consumed at that boundary. The runtime-unknown admission safety lag
		// applies only to implicit admission promotions; applying it here would
		// make a declared M_t -> M_{t+1} update appear late and break lineage.
		if m.mappingDecisionMode == "async_pipeline_lag1" ||
			m.mappingDecisionMode == "async_double_buffer" {
			if logicalRound < update.EffectiveEpoch {
				continue
			}
			if logicalRound > update.EffectiveEpoch {
				m.appliedExternalUpdates[updateKey] = true
				continue
			}
		} else if logicalRound < update.EffectiveEpoch+m.routerSafetyLagEpochs {
			continue
		}
		if m.softDecisionMode == saaIntegratedDecisionMode {
			addresses := make([]string, 0, len(update.Addresses))
			for _, rawAddr := range update.Addresses {
				if addr := normalizeMappingAuthorityAccount(rawAddr); addr != "" {
					addresses = append(addresses, addr)
				}
			}
			state := m.integratedStateLocked()
			proposal := mappingAuthorityProposal{
				ProposalID:       "external:" + updateKey,
				Source:           "external_scheduler",
				Addresses:        addresses,
				SourceShard:      update.SourceShard,
				TargetShard:      update.TargetShard,
				BaseRouteVersion: state.authority.routeVersion,
				EffectiveEpoch:   logicalRound + 1,
			}
			decision := state.authority.submit(proposal, logicalRound)
			m.appliedExternalUpdates[updateKey] = true
			m.externalAppliedSequence++
			m.routeVersion = state.authority.routeVersion
			m.appendSAAIntegratedAuditLocked(map[string]interface{}{
				"record_type":               "external_mapping_proposal",
				"logical_round":             logicalRound,
				"declared_effective_epoch":  update.EffectiveEpoch,
				"authority_effective_epoch": proposal.EffectiveEpoch,
				"proposal":                  proposal,
				"authority_decision":        decision,
			})
			continue
		}
		appliedForRecord := 0
		for _, rawAddr := range update.Addresses {
			addr := normalizeAdmissionAddress(rawAddr)
			if addr == "" {
				continue
			}
			m.externalShardByAccount[addr] = update.TargetShard
			appliedAccounts++
			appliedForRecord++
		}
		m.appliedExternalUpdates[updateKey] = true
		appliedRecords++
		m.externalAppliedSequence++
		if appliedForRecord > 0 {
			m.routeVersion++
		}
		m.externalRollingHash = sha256.Sum256([]byte(fmt.Sprintf(
			"%x|%s|%d|%d|%d|%s",
			m.externalRollingHash,
			updateKey,
			update.EffectiveEpoch,
			update.SourceShard,
			update.TargetShard,
			strings.Join(update.Addresses, ","),
		)))
		appendSupervisorRoutingMappingAudit(
			m.externalMappingAuditPath,
			path,
			logicalRound,
			updateKey,
			update,
			appliedForRecord,
			m.externalAppliedSequence,
			m.routeVersion,
			m.externalRollingHash,
		)
	}
	if appliedRecords > 0 && m.sl != nil {
		m.sl.Slog.Printf("Supervisor admission router applied external mapping updates: round=%d records=%d accounts=%d\n", logicalRound, appliedRecords, appliedAccounts)
	}
}

func appendSupervisorRoutingMappingAudit(
	auditPath string,
	updatePath string,
	logicalRound int,
	updateKey string,
	update *runtimeUnknownAdmissionUpdate,
	appliedAccounts int,
	sequence uint64,
	routeVersion uint64,
	rollingHash [32]byte,
) {
	if auditPath == "" || update == nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(auditPath), 0o755); err != nil {
		return
	}
	record := map[string]interface{}{
		"recorded_at_utc":          time.Now().UTC().Format(time.RFC3339Nano),
		"authority_mode":           "supervisor_frozen_route",
		"current_epoch":            logicalRound,
		"effective_epoch":          update.EffectiveEpoch,
		"proposal_id":              updateKey,
		"object_id":                update.ObjectID,
		"object_type":              update.ObjectType,
		"source_shard":             update.SourceShard,
		"target_shard":             update.TargetShard,
		"applied_accounts":         appliedAccounts,
		"batch_update_count":       1,
		"sequence":                 sequence,
		"route_version":            routeVersion,
		"rolling_hash":             fmt.Sprintf("%x", rollingHash),
		"update_path":              updatePath,
		"routing_frozen":           true,
		"node_partition_map_apply": false,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return
	}
	file, err := os.OpenFile(auditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(payload, '\n'))
}

func loadStableAccountAnchorMaps(accountClusterMapPath string, clusterShardMapPath string) (map[string]uint64, map[string]int, map[int]uint64) {
	stableClusterToShard := loadStableClusterToShard(clusterShardMapPath)
	if len(stableClusterToShard) == 0 {
		return map[string]uint64{}, map[string]int{}, map[int]uint64{}
	}

	file, err := os.Open(accountClusterMapPath)
	if err != nil {
		return map[string]uint64{}, map[string]int{}, stableClusterToShard
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return map[string]uint64{}, map[string]int{}, stableClusterToShard
	}
	indexByName := make(map[string]int, len(header))
	for idx, column := range header {
		indexByName[strings.TrimSpace(column)] = idx
	}
	accountIndex, okAccount := indexByName["account"]
	clusterIndex, okCluster := indexByName["cluster_id"]
	projectedUnknownIndex, hasProjectedUnknown := indexByName["is_projected_unknown"]
	if !okAccount || !okCluster {
		return map[string]uint64{}, map[string]int{}, stableClusterToShard
	}

	shardByAccount := make(map[string]uint64)
	clusterByAccount := make(map[string]int)
	for {
		row, readErr := reader.Read()
		if readErr != nil {
			break
		}
		account := normalizeAdmissionAddress(row[accountIndex])
		clusterID, clusterErr := strconv.Atoi(strings.TrimSpace(row[clusterIndex]))
		if account == "" || clusterErr != nil {
			continue
		}
		if hasProjectedUnknown && parseTruthValue(row[projectedUnknownIndex]) {
			continue
		}
		targetShard, ok := stableClusterToShard[clusterID]
		if !ok {
			continue
		}
		shardByAccount[account] = targetShard
		clusterByAccount[account] = clusterID
	}
	return shardByAccount, clusterByAccount, stableClusterToShard
}

func loadStableClusterToShard(clusterShardMapPath string) map[int]uint64 {
	file, err := os.Open(clusterShardMapPath)
	if err != nil {
		return map[int]uint64{}
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return map[int]uint64{}
	}
	indexByName := make(map[string]int, len(header))
	for idx, column := range header {
		indexByName[strings.TrimSpace(column)] = idx
	}
	clusterIndex, okCluster := indexByName["cluster_id"]
	shardIndex, okShard := indexByName["shard_id"]
	typeIndex, okType := indexByName["cluster_type"]
	if !okCluster || !okShard || !okType {
		return map[int]uint64{}
	}

	result := make(map[int]uint64)
	for {
		row, readErr := reader.Read()
		if readErr != nil {
			break
		}
		if strings.ToLower(strings.TrimSpace(row[typeIndex])) != "stable" {
			continue
		}
		clusterID, clusterErr := strconv.Atoi(strings.TrimSpace(row[clusterIndex]))
		shardID, shardErr := strconv.Atoi(strings.TrimSpace(row[shardIndex]))
		if clusterErr != nil || shardErr != nil || clusterID < 0 || shardID < 0 {
			continue
		}
		result[clusterID] = uint64(shardID)
	}
	return result
}

func appendRouterPressureStatusJSONL(path string, status runtimeRouterPressureStatus) {
	if strings.TrimSpace(path) == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return
	}
	handle, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer handle.Close()
	record, err := json.Marshal(status)
	if err != nil {
		return
	}
	_, _ = handle.Write(append(record, '\n'))
}

func normalizeAdmissionAddress(addr string) string {
	trimmed := strings.TrimSpace(strings.ToLower(addr))
	if trimmed == "" || trimmed == "none" || trimmed == "null" || trimmed == "nan" {
		return ""
	}
	withoutPrefix := trimmed
	if strings.HasPrefix(withoutPrefix, "0x") {
		withoutPrefix = strings.TrimPrefix(withoutPrefix, "0x")
	}
	if len(withoutPrefix) == 0 {
		return ""
	}
	for _, ch := range withoutPrefix {
		if !unicode.IsDigit(ch) && (ch < 'a' || ch > 'f') {
			return ""
		}
	}
	// Native CSV rows keep the 0x prefix while data2tx strips it before
	// constructing transactions. A single canonical key keeps stable anchors
	// and live transaction observations in the same address namespace.
	return "0x" + withoutPrefix
}

func parseEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "y"
}

func parseTruthValue(value string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	return trimmed == "1" || trimmed == "true" || trimmed == "yes" || trimmed == "y"
}

func parseSAADecisionMode(value string) string {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "" {
		return "legacy"
	}
	if mode != "legacy" && mode != "soft_decay" && mode != saaIntegratedDecisionMode {
		panic("invalid BLOCK_EMULATOR_SAA_DECISION_MODE: " + value)
	}
	return mode
}

func resolveSAASoftAuditPath() string {
	if path := strings.TrimSpace(os.Getenv(runtimeUnknownSoftAuditPathEnv)); path != "" {
		return path
	}
	if path := resolveRuntimeMappingUpdatePath(); path != "" {
		return path + ".saa_soft_decisions.jsonl"
	}
	return ""
}

func parseAnchorRealignmentMode(value string) string {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "stage3_proposal" {
		return mode
	}
	return "independent"
}

func parseMappingDecisionMode(value string) string {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "async_double_buffer" || mode == "async_pipeline_lag1" {
		return mode
	}
	return "blocking"
}

func parseQPSAMode(value string) string {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "shadow" || mode == "active" {
		return mode
	}
	return "legacy"
}

func parseQPSAV2Mode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "shadow":
		return "shadow"
	default:
		return "off"
	}
}

func parseEnvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseEnvFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func topShardEvidence(shardEvidence map[uint64]int, total int) (uint64, int, float64) {
	bestShard := uint64(0)
	bestCount := -1
	for shardID, count := range shardEvidence {
		if count > bestCount || (count == bestCount && shardID < bestShard) {
			bestShard = shardID
			bestCount = count
		}
	}
	if bestCount < 0 {
		bestCount = 0
	}
	return bestShard, bestCount, share(bestCount, total)
}

func share(part int, total int) float64 {
	if total <= 0 || part <= 0 {
		return 0.0
	}
	return float64(part) / float64(total)
}

func maxFloat(a float64, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minFloat(a float64, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
