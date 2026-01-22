package main

import (
	"amethyst/internal/adaptive"
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/metadata"
	"amethyst/internal/memtable"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"amethyst/internal/wal"
	"fmt"
	"log"
	"time"
)

func main() {
	// ---- Setup ----
	w, err := wal.NewDiskWAL("wal.log")
	if err != nil {
		panic(err)
	}

	mem := memtable.NewMemtable(1 << 20) // 1MB
	meta := metadata.NewTracker()

	fileMgr, err := segmentfile.NewSegmentFileManager("sstable.data")
	if err != nil {
		panic(err)
	}

	indexBuilder := sparseindex.NewBuilder(16)
	sstWriter := writer.NewWriter(fileMgr, indexBuilder)
	sstReader := reader.NewReader(fileMgr)

	fsm := adaptive.NewFSMController()
	director := compaction.NewDirector(meta, fsm)
	executor := compaction.NewExecutor(meta, sstReader, sstWriter)

	log.Println("Amethyst started")

	// ---- Write workload ----
	for i := 0; i < 50000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := []byte("value")

		_ = w.LogPut(key, val)
		mem.Put(key, val)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, err := sstWriter.WriteSegment(data, common.TIERED)
			if err != nil {
				panic(err)
			}
			meta.RegisterSegment(seg)
			_ = w.Truncate()

			log.Printf("FLUSH → segment %s (%s)\n", seg.ID, string(seg.Strategy))
		}
	}

	// ---- Background compaction loop ----
	for {
		plan := director.MaybePlan()
		if plan != nil {
			log.Printf("REWRITE triggered: %s\n", plan.Reason)
			_, err := executor.Execute(plan)
			if err != nil {
				panic(err)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}
