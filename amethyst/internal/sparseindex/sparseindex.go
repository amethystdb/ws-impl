package sparseindex

type SparseIndex struct {
	Keys    []string
	Offsets []int64
}

type Builder interface {
	Build(keys []string, offsets []int64) *SparseIndex
	Seek(key string) int64
}
