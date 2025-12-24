package writer

import "amethyst/internal/common"

type SSTableWriter interface {
	WriteSegment(
		sortedData map[string][]byte,
		strategy common.CompactionType,
	) (*common.SegmentMeta, error)
}
