package adaptive

import (
	"amethyst/internal/common"
	"fmt"
	"time"
)

const MinSegmentSize = int64(4 * 1024)        // 4KB
const MinRewriteInterval = int64(1)           // 1 second (was 30)
const ReadWriteRatioThreshold = 4.0
const OverlapThreshold = 2
const WriteCountThreshold = int64(100)

type Controller interface {
	ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}

type FSMController struct{}

func NewFSMController() *FSMController {
	return &FSMController{}
}

func (c *FSMController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	now := time.Now().Unix()
	
	// Anti-thrashing check (reduced to 1 second for testing)
	if !meta.CooldownExpired(now, MinRewriteInterval) {
		return false, meta.Strategy, ""
	}
	
	// Too small to care
	if meta.Size() < MinSegmentSize {
		return false, meta.Strategy, ""
	}
	
	switch meta.Strategy {
	case common.TIERED:
		// Trigger rewrite if read-heavy (ignore overlap for single-segment case)
		if meta.ReadWriteRatio() > ReadWriteRatioThreshold {
			return true, common.LEVELED, fmt.Sprintf(
				"rw=%.2f (read-heavy workload detected), tiered→leveled", 
				meta.ReadWriteRatio(),
			)
		}
		
	case common.LEVELED:
		// Trigger rewrite if write-heavy
		if meta.WriteCount > WriteCountThreshold {
			return true, common.TIERED, fmt.Sprintf(
				"wc=%d (write-heavy workload detected), leveled→tiered", 
				meta.WriteCount,
			)
		}
	}
	
	return false, meta.Strategy, ""
}