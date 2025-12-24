package compaction

import "amethyst/internal/common"

type Plan struct {
	Inputs         []*common.SegmentMeta
	OutputStrategy common.CompactionType
	Reason         string
}

type Director interface {
	MaybePlan() *Plan
}
