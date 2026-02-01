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

type director struct {
	meta metadata.Tracker
	fsm  adaptive.Controller
}

func NewDirector(meta metadata.Tracker, fsm adaptive.Controller) *director {
	return &director{
		meta: meta,
		fsm:  fsm,
	}
}

func (d *director) MaybePlan() *Plan {
	segments := d.meta.GetAllSegments()

	for _, seg := range segments {
		if seg.Obsolete {
			continue
		}

		should, newStrategy, reason := d.fsm.ShouldRewrite(seg)
		if !should {
			continue
		}

		// INTEGRATION: Use recursive collection for Leveled transitions
		// to ensure the resulting segment has 0 overlaps.
		var inputs []*common.SegmentMeta
		if newStrategy == common.LEVELED {
			inputs = d.collectAllOverlaps(seg)
		} else {
			inputs = []*common.SegmentMeta{seg}
		}

		return &Plan{
			Inputs:         inputs,
			OutputStrategy: newStrategy,
			Reason:         reason,
		}
	}
	return nil
}

// collectAllOverlaps pulls in every segment that touches the range.
// This is what makes the Read Amp drop from 8 to 1.
func (d *director) collectAllOverlaps(target *common.SegmentMeta) []*common.SegmentMeta {
	inputs := []*common.SegmentMeta{target}
	seen := make(map[string]bool)
	seen[target.ID] = true

	changed := true
	for changed {
		changed = false
		for i := 0; i < len(inputs); i++ {
			input := inputs[i]
			overlaps := d.meta.GetOverlappingSegments(input)
			for _, overlap := range overlaps {
				if !seen[overlap.ID] && !overlap.Obsolete {
					inputs = append(inputs, overlap)
					seen[overlap.ID] = true
					changed = true
				}
			}
		}
	}
	return inputs
}
