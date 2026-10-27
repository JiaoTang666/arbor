package committee

import "blockEmulator/params"

func bootstrapDispatchTarget(batchDataNum int) int {
	target := params.MaxBlockSize_global
	if target <= 0 {
		target = int(params.InjectSpeed)
	}
	if target <= 0 {
		target = 1
	}
	if batchDataNum > 0 && target > batchDataNum {
		target = batchDataNum
	}
	return target
}

func shouldFlushTxBatch(currentBatchLen int, totalSent int, totalTarget int, batchDataNum int, dispatchCount int) bool {
	if currentBatchLen <= 0 {
		return false
	}
	if totalSent >= totalTarget {
		return true
	}
	target := batchDataNum
	if dispatchCount == 0 {
		target = bootstrapDispatchTarget(batchDataNum)
	}
	if target <= 0 {
		target = 1
	}
	return currentBatchLen >= target
}
