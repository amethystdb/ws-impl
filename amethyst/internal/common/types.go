package common

type CompactionType int

const (
	TIERED CompactionType = iota
	LEVELED
)

type SegmentMeta struct {
	ID     string
	Offset int64
	Length int64

	MinKey string
	MaxKey string

	Strategy CompactionType

	ReadCount    int64
	WriteCount   int64
	OverlapCount int64

	CreatedAt     int64
	LastRewriteAt int64

	Obsolete    bool
	SparseIndex interface{} // filled later
}
