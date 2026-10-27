package measure

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"math/big"
	"strconv"
	"time"
)

type txMetricDetailTime struct {
	// normal tx time
	TxProposeTimestamp, BlockProposeTimestamp, TxCommitTimestamp time.Time

	// relay tx time
	Relay1CommitTimestamp, Relay2CommitTimestamp time.Time

	// broker tx time
	Broker1CommitTimestamp, Broker2CommitTimestamp time.Time

	IngressSequence      uint64
	RoutingEpoch         int
	MappingVersion       uint64
	RoutedSenderShard    uint64
	RoutedRecipientShard uint64
	RouteStatus          string
	CompletionStage      string
	Sender               string
	Recipient            string
}

// captureRoutingMetadata preserves the historical two-argument call while
// allowing each serialization path to declare its endpoint state explicitly.
func captureRoutingMetadata(detail *txMetricDetailTime, tx *core.Transaction, endpoint ...string) {
	if detail == nil || tx == nil {
		return
	}
	detail.IngressSequence = tx.IngressSequence
	detail.RoutingEpoch = tx.RoutingEpoch
	detail.MappingVersion = tx.MappingVersion
	detail.RoutedSenderShard = tx.RoutedSenderShard
	detail.RoutedRecipientShard = tx.RoutedRecipientShard
	if len(endpoint) > 0 {
		detail.RouteStatus = endpoint[0]
	}
	if len(endpoint) > 1 {
		detail.CompletionStage = endpoint[1]
	}
	detail.Sender = string(tx.Sender)
	detail.Recipient = string(tx.Recipient)
}

// to test Tx detail
type TestTxDetail struct {
	txHash2DetailTime map[string]*txMetricDetailTime
}

func NewTestTxDetail() *TestTxDetail {
	return &TestTxDetail{
		txHash2DetailTime: make(map[string]*txMetricDetailTime),
	}
}

func (ttd *TestTxDetail) OutputMetricName() string {
	return "Tx_Details"
}

func (ttd *TestTxDetail) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}

	for _, innertx := range b.InnerShardTxs {
		if _, ok := ttd.txHash2DetailTime[string(innertx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(innertx.TxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(innertx.TxHash)].TxProposeTimestamp = innertx.Time
		ttd.txHash2DetailTime[string(innertx.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(innertx.TxHash)].TxCommitTimestamp = b.CommitTime
		captureRoutingMetadata(ttd.txHash2DetailTime[string(innertx.TxHash)], innertx, "final", "direct_final")
	}
	for _, r1tx := range b.Relay1Txs {
		if _, ok := ttd.txHash2DetailTime[string(r1tx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(r1tx.TxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(r1tx.TxHash)].TxProposeTimestamp = r1tx.Time
		ttd.txHash2DetailTime[string(r1tx.TxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(r1tx.TxHash)].Relay1CommitTimestamp = b.CommitTime
		captureRoutingMetadata(ttd.txHash2DetailTime[string(r1tx.TxHash)], r1tx, "in_flight", "relay_stage1")
	}
	for _, r2tx := range b.Relay2Txs {
		if _, ok := ttd.txHash2DetailTime[string(r2tx.TxHash)]; !ok {
			ttd.txHash2DetailTime[string(r2tx.TxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(r2tx.TxHash)].Relay2CommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(r2tx.TxHash)].TxCommitTimestamp = b.CommitTime
		captureRoutingMetadata(ttd.txHash2DetailTime[string(r2tx.TxHash)], r2tx, "final", "relay_final")
	}
	for _, b1tx := range b.Broker1Txs {
		if _, ok := ttd.txHash2DetailTime[string(b1tx.RawTxHash)]; !ok {
			ttd.txHash2DetailTime[string(b1tx.RawTxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].TxProposeTimestamp = b1tx.Time
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].BlockProposeTimestamp = b.ProposeTime
		ttd.txHash2DetailTime[string(b1tx.RawTxHash)].Broker1CommitTimestamp = b.CommitTime
		captureRoutingMetadata(ttd.txHash2DetailTime[string(b1tx.RawTxHash)], b1tx, "in_flight", "broker_stage1")
	}
	for _, b2tx := range b.Broker2Txs {
		if _, ok := ttd.txHash2DetailTime[string(b2tx.RawTxHash)]; !ok {
			ttd.txHash2DetailTime[string(b2tx.RawTxHash)] = &txMetricDetailTime{}
		}
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].Broker2CommitTimestamp = b.CommitTime
		ttd.txHash2DetailTime[string(b2tx.RawTxHash)].TxCommitTimestamp = b.CommitTime
		captureRoutingMetadata(ttd.txHash2DetailTime[string(b2tx.RawTxHash)], b2tx, "final", "broker_final")
	}
}

func (ttd *TestTxDetail) HandleExtraMessage([]byte) {}

func (ttd *TestTxDetail) OutputRecord() (perEpochCTXs []float64, totTxNum float64) {
	ttd.writeToCSV()
	return []float64{}, 0
}

func (ttd *TestTxDetail) writeToCSV() {
	fileName := ttd.OutputMetricName()
	measureName := []string{
		"TxHash (Byte -> Big Int)",
		"Tx propose timestamp",
		"Block propose timestamp",
		"Tx finally commit timestamp",
		"Relay1 Tx commit timestamp (not a relay tx -> nil)",
		"Relay2 Tx commit timestamp (not a relay tx -> nil)",
		"Broker1 Tx commit timestamp (not a broker tx -> nil)",
		"Broker2 Tx commit timestamp (not a broker tx -> nil)",
		"Confirmed latency of this tx (ms)",
		"Ingress sequence",
		"Routing epoch",
		"Mapping version",
		"Routed sender shard",
		"Routed recipient shard",
		"Route status",
		"Completion stage",
		"Sender",
		"Recipient",
	}
	measureVals := make([][]string, 0)

	for key, val := range ttd.txHash2DetailTime {
		csvLine := []string{
			new(big.Int).SetBytes([]byte(key)).String(),

			timestampToString(val.TxProposeTimestamp),
			timestampToString(val.BlockProposeTimestamp),
			timestampToString(val.TxCommitTimestamp),

			timestampToString(val.Relay1CommitTimestamp),
			timestampToString(val.Relay2CommitTimestamp),

			timestampToString(val.Broker1CommitTimestamp),
			timestampToString(val.Broker2CommitTimestamp),

			strconv.FormatInt(int64(val.TxCommitTimestamp.Sub(val.TxProposeTimestamp).Milliseconds()), 10),
			strconv.FormatUint(val.IngressSequence, 10),
			strconv.Itoa(val.RoutingEpoch),
			strconv.FormatUint(val.MappingVersion, 10),
			strconv.FormatUint(val.RoutedSenderShard, 10),
			strconv.FormatUint(val.RoutedRecipientShard, 10),
			val.RouteStatus,
			val.CompletionStage,
			val.Sender,
			val.Recipient,
		}
		measureVals = append(measureVals, csvLine)
	}

	WriteMetricsToCSV(fileName, measureName, measureVals)
}

// zero time to empty string
func timestampToString(thisTime time.Time) string {
	if thisTime.IsZero() {
		return ""
	}
	return strconv.FormatInt(thisTime.UnixMilli(), 10)
}
