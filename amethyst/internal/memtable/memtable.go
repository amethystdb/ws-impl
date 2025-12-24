package memtable

type Memtable interface {
	Put(key string, value []byte)
	Delete(key string)
	Get(key string) ([]byte, bool)

	ShouldFlush() bool
	Flush() map[string][]byte
}
