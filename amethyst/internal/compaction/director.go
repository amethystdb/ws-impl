package compaction

import (
	"amethyst/internal/adaptive"
	"amethyst/internal/common"
	"amethyst/internal/metadata"
)

type Plan struct {
	Inputs         []*common.SegmentMeta
	OutputStrategy common.CompactionType
	Reason         string
}

type Director interface {
	MaybePlan() *Plan
}

// director produces compaction plans based on metadata + FSM.
type director struct {
	meta metadata.Tracker
	fsm  adaptive.Controller
}

func NewDirector(
	meta metadata.Tracker,
	fsm adaptive.Controller,
) *director {
	return &director{
		meta: meta,
		fsm:  fsm,
	}
}

// MaybePlan returns at most one compaction plan.
// Returns nil if no rewrite is needed.
func (d *director) MaybePlan() *Plan {
	segments := d.meta.GetAllSegments()

	// Newest → oldest scan
	for _, seg := range segments {
		if seg.Obsolete {
			continue
		}

		should, newStrategy, reason := d.fsm.ShouldRewrite(seg)
		if !should {
			continue
		}

		return &Plan{
			Inputs:         []*common.SegmentMeta{seg},
			OutputStrategy: newStrategy,
			Reason:         reason,
		}
	}

	return nil
}
