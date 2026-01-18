package adaptive

import "amethyst/internal/common"


// Controller decides whether a segment should be rewritten.
// It is a pure function over segmentmeta
type Controller interface {
	ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}


// FSMController is a placeholder FSM.
// Tomorrow we fill in thresholds and logic.
type FSMController struct {
	// thresholds go here later
}

func NewFSMController() *FSMController {
	return &FSMController{}
}

// ShouldRewrite currently never rewrites.
//intentional will be fixed later

func (c *FSMController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	return false, meta.Strategy, ""
}
