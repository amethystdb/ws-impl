package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/metadata"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
)


type Executor interface {
	Execute(plan *Plan) (*common.SegmentMeta, error)
}

type executor struct{
	meta metadata.Tracker
	reader reader.SSTableReader
	writer writer.SSTableWriter
}
func newExecutor(
	meta metadata.Tracker,
	reader reader.SSTableReader,
	writer writer.SSTableWriter,
) *executor {
	return &executor{
		meta:  meta,
		reader: reader,
		writer: writer,
	}
}

func (e *executor) Execute(plan *Plan) (*common.SegmentMeta, error) {
	//currently only one input segment is supported
	input:=plan.Inputs[0]
	//reconstruct full kv map
	merged := make(map[string][]byte)

	//scan entire segment range, its intentional for clarity purpose
	//we bruteforce scan by key range, assuming that keys are enumerable by range
	//in real world, reader is used record by record, but for now we reuse the semantics of reader
	for _, seg:= range plan.Inputs{
		_=seg
	
	}

	// this implementation is aimed at producing evaluation results for a workshop, so for now we can assume that
	// the reader can iterate keys, and last write wins


	//write new seg
	newSeg, err:= e.writer.WriteSegment(merged, plan.OutputStrategy)
	if err != nil {
		return nil, err
	}
	e.meta.RegisterSegment(newSeg)

	for _, seg := range plan.Inputs {
		e.meta.MarkObsolete(seg.ID)
	}	

	return newSeg, nil
}