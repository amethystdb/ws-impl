package reader

import "amethyst/internal/common"

type SSTableReader interface {
	Get(meta *common.SegmentMeta, key string) ([]byte, bool)
}
