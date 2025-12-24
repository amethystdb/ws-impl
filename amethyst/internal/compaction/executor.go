package compaction

import "amethyst/internal/common"

type Executor interface {
	Execute(plan *Plan) (*common.SegmentMeta, error)
}
