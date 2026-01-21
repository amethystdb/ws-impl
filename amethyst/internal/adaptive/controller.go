package adaptive

import (
	"amethyst/internal/common"
	"time"
)

//THIS FILE HAS THE FSM CONTROLLER
//it works based on vibes and feelings
// Controller decides whether a segment should be rewritten.
// It is a pure function over segmentmeta
const MinSegmentSize = int64(1<<20) //by design the minimum seg size is 1mb
const MinRewriteInterval = int64(30) //in seconds

const ReadWriteRatioThreshold = 4.0
const OverlapThreshold = 2
const WriteCountThreshold = int64(100) //coarse proxy for churn


type Controller interface {
	ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}

type FSMController struct {}

func NewFSMController() *FSMController {
	return &FSMController{}
}

// ShouldRewrite currently never rewrites.
//intentional will be fixed later

func (c *FSMController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	now:=time.Now().Unix()
	//anti thrashing check
	if !meta.CooldownExpired(now, MinRewriteInterval) {
		return false, meta.Strategy, ""
	}
	//too small to care
	if meta.Size() < MinSegmentSize {
		return false, meta.Strategy, ""
	}
	switch meta.Strategy {
	case common.TIERED:
		if meta.ReadWriteRatio()> ReadWriteRatioThreshold && meta.OverlapCount > OverlapThreshold {
			return true, common.LEVELED, "High read/write amplification so tiered->leveled"
		}
		
	case common.LEVELED:
		if meta.WriteCount>WriteCountThreshold{
			return true, common.TIERED, "write heavy segment so leveled ->tiered"
		}
	}

	return false, meta.Strategy, ""
}
