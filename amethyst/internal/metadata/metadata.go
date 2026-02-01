package metadata

import (
	"amethyst/internal/common"
	"sync"
)

type Tracker interface {
	RegisterSegment(meta *common.SegmentMeta)
	GetSegmentsForKey(key string) []*common.SegmentMeta
	GetAllSegments() []*common.SegmentMeta
	// ADD THIS: The Director needs this for the recursive loop
	GetOverlappingSegments(target *common.SegmentMeta) []*common.SegmentMeta

	MarkObsolete(id string)
	UpdateStats(id string, reads int64, writes int64)
}

type tracker struct {
	mu       sync.RWMutex // Use RWMutex for better read performance
	segments map[string]*common.SegmentMeta
	ordered  []*common.SegmentMeta
}

// NewTracker creates a new MetadataTracker.
func NewTracker() Tracker {
	return &tracker{
		segments: make(map[string]*common.SegmentMeta),
		ordered:  make([]*common.SegmentMeta, 0),
	}
}

func (t *tracker) RegisterSegment(meta *common.SegmentMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var overlaps int64
	for _, other := range t.segments {
		if other.Obsolete {
			continue
		}
		// Logic: Two segments overlap if they don't sit entirely to the left or right of each other
		// This is the core metric for your "Adaptive" transition proof
		if !(meta.MaxKey < other.MinKey || meta.MinKey > other.MaxKey) {
			overlaps++
		}
	}

	// This allows the FSM to detect "Tiered" behavior (high overlap)
	// and transition to "Leveled" (zero overlap)
	meta.OverlapCount = overlaps

	t.segments[meta.ID] = meta
	t.ordered = append([]*common.SegmentMeta{meta}, t.ordered...)
}

// NEW METHOD: This fixes the "MissingFieldOrMethod" error in your screenshot
func (t *tracker) GetOverlappingSegments(target *common.SegmentMeta) []*common.SegmentMeta {
	t.mu.Lock()
	defer t.mu.Unlock()

	var overlaps []*common.SegmentMeta
	for _, seg := range t.segments {
		if seg.ID == target.ID || seg.Obsolete {
			continue
		}
		// Logic: If ranges touch, they overlap
		if !(target.MaxKey < seg.MinKey || target.MinKey > seg.MaxKey) {
			overlaps = append(overlaps, seg)
		}
	}
	return overlaps
}

func (t *tracker) GetSegmentsForKey(key string) []*common.SegmentMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]*common.SegmentMeta, 0)
	for _, seg := range t.ordered {
		if seg.Obsolete {
			continue
		}
		if key >= seg.MinKey && key <= seg.MaxKey {
			result = append(result, seg)
		}
	}
	return result
}

func (t *tracker) GetAllSegments() []*common.SegmentMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]*common.SegmentMeta, 0, len(t.ordered))
	for _, seg := range t.ordered {
		if !seg.Obsolete {
			result = append(result, seg)
		}
	}
	return result
}

func (t *tracker) MarkObsolete(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if seg, ok := t.segments[id]; ok {
		seg.Obsolete = true
	}
}

func (t *tracker) UpdateStats(id string, reads int64, writes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if seg, ok := t.segments[id]; ok {
		seg.ReadCount += reads
		seg.WriteCount += writes
	}
}
