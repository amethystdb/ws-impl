package segmentfile

type SegmentFileManager interface {
	Append(data []byte) (offset int64, length int64, err error)
	ReadAt(offset int64, length int64) ([]byte, error)
	Delete(offset int64) error
}
