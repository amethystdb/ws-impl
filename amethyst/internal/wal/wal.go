package wal

type Entry struct {
	Key       string
	Value     []byte
	Tombstone bool
}

type WAL interface {
	LogPut(key string, value []byte) error
	LogDelete(key string) error
	ReadAll() ([]Entry, error)
	Truncate() error
}
