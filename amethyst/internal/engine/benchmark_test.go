package engine

//just checking if benchmarks work the same way as adaptive
//change later for offical ws

import (
	"amethyst/internal/adaptive"
	"amethyst/internal/benchmarks"
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/metadata"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"testing"
)

func TestWorkshopPaperBenchmarks(t *testing.T) {
	testCases := []struct {
		name string
		ctrl adaptive.Controller
	}{
		{"Adaptive_FSM", adaptive.NewFSMController()},
		{"Static_Tiered", benchmarks.NewTieredController()},
		{"Static_Leveled", benchmarks.NewLeveledController()},
	}

	//loop through adp, tiered, leveled
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup components
			tracker := metadata.NewTracker()
			sfm, _ := segmentfile.NewSegmentFileManager("test_data")
			sw := writer.NewWriter(sfm, sparseindex.NewBuilder(128))
			sr := reader.NewReader(sfm)

			dir := compaction.NewDirector(tracker, tc.ctrl)
			exe := compaction.NewExecutor(tracker, sr, sw)

			//SIMULATION DATA
			//dummy segment large enough and has high activity
			dummyMeta := &common.SegmentMeta{
				ID:         "bench-seg-1",
				Strategy:   common.TIERED,
				Length:     5000, // Above MinSegmentSize (4KB)
				WriteCount: 150,  // Above WriteCountThreshold
				ReadCount:  800,  // High read ratio
				MinKey:     "a",
				MaxKey:     "z",
			}
			tracker.RegisterSegment(dummyMeta)

			// Trigger the cycle
			plan := dir.MaybePlan()
			if plan != nil {
				_, err := exe.Execute(plan)
				if err != nil {
					t.Fatalf("Execution failed: %v", err)
				}
			} else {
				t.Log("No compaction plan generated. Check thresholds!")
			}
		})
	}
}
