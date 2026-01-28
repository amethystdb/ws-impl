package main

import (
	"amethyst/internal/adaptive"
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/memtable"
	"amethyst/internal/metadata"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"amethyst/internal/wal"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"time"
)

var (
	workload  = flag.String("workload", "shift", "Workload type")
	numKeys   = flag.Int("keys", 100000, "Number of keys")
	valueSize = flag.Int("value-size", 256, "Value size")
)

type Results struct {
	Engine             string  `json:"engine"`
	Workload           string  `json:"workload"`
	NumKeys            int     `json:"num_keys"`
	WriteAmplification float64 `json:"write_amplification"`
	ReadAmplification  float64 `json:"read_amplification"`
	CompactionCount    int     `json:"compaction_count"`
	TotalDurationSec   float64 `json:"total_duration_sec"`

	// Debug info
	LogicalBytes  int64 `json:"logical_bytes"`
	PhysicalBytes int64 `json:"physical_bytes"`
	TotalReads    int64 `json:"total_reads"`
	SegmentScans  int64 `json:"segment_scans"`
}

// hasLen reports whether the flushed value has any length/entries.
// It handles slices, arrays, maps and strings; for other non-nil values it returns true.
func hasLen(v interface{}) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map, reflect.String:
		return rv.Len() > 0
	default:
		// For other kinds (e.g. structs, pointers) consider non-nil as having data.
		return true
	}
}

func main() {
	flag.Parse()

	os.Remove("wal.log")
	os.Remove("sstable.data")

	w, _ := wal.NewDiskWAL("wal.log")
	mem := memtable.NewMemtable(4 * 1024)
	meta := metadata.NewTracker()
	fileMgr, _ := segmentfile.NewSegmentFileManager("sstable.data")
	indexBuilder := sparseindex.NewBuilder(16)
	sstWriter := writer.NewWriter(fileMgr, indexBuilder)
	sstReader := reader.NewReader(fileMgr)
	fsm := adaptive.NewFSMController()
	director := compaction.NewDirector(meta, fsm)
	executor := compaction.NewExecutor(meta, sstReader, sstWriter)

	fmt.Printf("Running %s workload with %d keys...\n", *workload, *numKeys)
	start := time.Now()

	var logicalBytes int64 = 0
	var physicalBytes int64 = 0
	var totalReads int64 = 0
	var totalSegmentsScanned int64 = 0
	compactionCount := 0

	switch *workload {
	case "shift":
		// PHASE 1: Write
		fmt.Println("Phase 1: Writing...")
		for i := 0; i < *numKeys; i++ {
			key := fmt.Sprintf("key-%010d", i)
			val := make([]byte, *valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			logicalBytes += int64(len(key) + *valueSize)

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.TIERED)
				physicalBytes += seg.Length
				meta.RegisterSegment(seg)
				w.Truncate()
			}

			if i > 0 && i%1000 == 0 {
				fmt.Printf("  Written: %d/%d\r", i, *numKeys)
			}
		}
		fmt.Println()

		// Flush remaining
		if data := mem.Flush(); hasLen(data) {
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			physicalBytes += seg.Length
			meta.RegisterSegment(seg)
		}

		fmt.Printf("  Segments after write: %d\n", len(meta.GetAllSegments()))

		// PHASE 2: Read
		fmt.Println("Phase 2: Reading (3x)...")
		time.Sleep(2 * time.Second)

		numReads := *numKeys * 3
		for i := 0; i < numReads; i++ {
			key := fmt.Sprintf("key-%010d", rand.Intn(*numKeys))
			segs := meta.GetAllSegments()

			totalReads++

			// Check each segment (this is read amplification)
			found := false
			for _, seg := range segs {
				totalSegmentsScanned++ // Count every check

				if !found && key >= seg.MinKey && key <= seg.MaxKey {
					sstReader.Get(seg, key)
					meta.UpdateStats(seg.ID, 1, 0)
					found = true
				}
			}

			if i > 0 && i%5000 == 0 {
				fmt.Printf("  Read: %d/%d\r", i, numReads)
			}
		}
		fmt.Println()

		fmt.Printf("  Current RA: %.2f\n", float64(totalSegmentsScanned)/float64(totalReads))

		// Try compaction
		time.Sleep(2 * time.Second)
		if plan := director.MaybePlan(); plan != nil {
			fmt.Println("  Compaction triggered...")
			newSeg, _ := executor.Execute(plan)
			physicalBytes += newSeg.Length // ← CRITICAL: Count rewrite!
			compactionCount++
			fmt.Printf("  Segments after compaction: %d\n", len(meta.GetAllSegments()))
		}

		// PHASE 3: Write again
		fmt.Println("Phase 3: Writing (50%)...")
		for i := 0; i < *numKeys/2; i++ {
			key := fmt.Sprintf("key-%010d", rand.Intn(*numKeys))
			val := make([]byte, *valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			logicalBytes += int64(len(key) + *valueSize)

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.TIERED)
				physicalBytes += seg.Length
				meta.RegisterSegment(seg)
				w.Truncate()
			}

			if i > 0 && i%1000 == 0 {
				fmt.Printf("  Written: %d/%d\r", i, *numKeys/2)
			}
		}
		fmt.Println()

		// Flush remaining
		if data := mem.Flush(); hasLen(data) {
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			physicalBytes += seg.Length
			meta.RegisterSegment(seg)
		}

		// Try compaction again
		time.Sleep(2 * time.Second)
		if plan := director.MaybePlan(); plan != nil {
			fmt.Println("  Compaction triggered...")
			newSeg, _ := executor.Execute(plan)
			physicalBytes += newSeg.Length // Count rewrite
			compactionCount++
		}

		duration := time.Since(start)

		// Calculate metrics
		writeAmp := float64(physicalBytes) / float64(logicalBytes)
		readAmp := float64(totalSegmentsScanned) / float64(totalReads)

		// Save results
		results := Results{
			Engine:             "adaptive",
			Workload:           *workload,
			NumKeys:            *numKeys,
			WriteAmplification: writeAmp,
			ReadAmplification:  readAmp,
			CompactionCount:    compactionCount,
			TotalDurationSec:   duration.Seconds(),
			LogicalBytes:       logicalBytes,
			PhysicalBytes:      physicalBytes,
			TotalReads:         totalReads,
			SegmentScans:       totalSegmentsScanned,
		}

		data, _ := json.MarshalIndent(results, "", "  ")
		filename := fmt.Sprintf("results_adaptive_%s.json", *workload)
		os.WriteFile(filename, data, 0644)

		fmt.Printf("\n✓ Results:\n")
		fmt.Printf("  Logical:  %d bytes\n", logicalBytes)
		fmt.Printf("  Physical: %d bytes\n", physicalBytes)
		fmt.Printf("  Write Amp: %.2f\n", writeAmp)
		fmt.Printf("  Read Amp: %.2f\n", readAmp)
		fmt.Printf("  Compactions: %d\n", compactionCount)
		fmt.Printf("  Duration: %v\n", duration)
		fmt.Printf("  Saved to: %s\n", filename)
	}
	;}