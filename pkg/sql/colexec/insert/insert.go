// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package insert

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "insert"

func (insert *Insert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": insert")
}

func (insert *Insert) OpType() vm.OpType {
	return vm.Insert
}

func (insert *Insert) Prepare(proc *process.Process) error {
	if insert.OpAnalyzer == nil {
		insert.OpAnalyzer = process.NewAnalyzer(insert.GetIdx(), insert.IsFirst, insert.IsLast, "insert")
	} else {
		insert.OpAnalyzer.Reset()
	}

	insert.ctr.state = vm.Build
	insert.ctr.affectedRows = 0
	insert.getFlushableS3WriterFunc = insert.getFlushableS3Writer
	insert.getS3WriterFunc = insert.getS3Writer
	insert.addAffectedRowsFunc = insert.addAffectedRows

	if insert.ToWriteS3 {
		s3Writer, err := colexec.NewS3Writer(insert.InsertCtx.TableDef)
		if err != nil {
			return err
		}
		insert.ctr.s3Writer = s3Writer

		if insert.ctr.buf == nil {
			insert.initBufForS3()
		}
	} else {
		ref := insert.InsertCtx.Ref
		eng := insert.InsertCtx.Engine

		if insert.ctr.source == nil {
			rel, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref)
			if err != nil {
				return err
			}
			insert.ctr.source = rel
		} else {
			err := insert.ctr.source.Reset(proc.GetTxnOperator())
			if err != nil {
				return err
			}
		}

		if insert.ctr.buf == nil {
			insert.ctr.buf = batch.NewWithSize(len(insert.InsertCtx.Attrs))
			insert.ctr.buf.SetAttributes(insert.InsertCtx.Attrs)
		}
	}

	return nil
}

// first parameter: true represents whether the current pipeline has ended
// first parameter: false
func (insert *Insert) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := insert.OpAnalyzer

	t := time.Now()
	defer func() {
		analyzer.AddInsertTime(t)
	}()

	if insert.ToWriteS3 {
		return insert.writeToS3(proc, analyzer)
	}
	return insert.writeToWorkspace(proc, analyzer)
}

func (insert *Insert) writeToS3(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if insert.ctr.state == vm.Build {
		input, err := insert.getInput(proc, analyzer)
		if err != nil {
			return input, err
		}

		if input.Batch == nil || input.Batch.IsEmpty() {
			if input.Batch == nil {
				insert.ctr.state = vm.Eval
			}
			result := vm.NewCallResult()
			result.Batch = batch.EmptyBatch
			return result, nil
		}

		if insert.InsertCtx.AddAffectedRows {
			affectedRows := uint64(input.Batch.RowCount())
			insert.addAffectedRowsFunc(affectedRows)
		}

		// write to s3.
		w, err := insert.getS3WriterFunc(insert.getTableID(proc))
		if err != nil {
			return input, err
		}

		input.Batch.Attrs = append(input.Batch.Attrs[:0], insert.InsertCtx.Attrs...)
		err = writeBatch(proc, w, input.Batch, analyzer)
		if err != nil {
			insert.ctr.state = vm.End
			return vm.CancelResult, err
		}
		result := vm.NewCallResult()
		result.Batch = batch.EmptyBatch
		return result, nil
	}

	result := vm.NewCallResult()
	result.Batch = insert.ctr.buf
	if insert.ctr.state == vm.Eval {
		for {
			writer := insert.getFlushableS3WriterFunc()
			if writer == nil {
				insert.ctr.state = vm.End
				return result, nil
			}

			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			err := flushTailBatch(proc, writer, &result, analyzer)
			if err != nil {
				return result, err
			}
		}
	}

	if insert.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}

func (insert *Insert) writeToWorkspace(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	input, err := insert.getInput(proc, analyzer)
	if err != nil {
		return input, err
	}
	if input.Batch == nil || input.Batch.IsEmpty() {
		return input, nil
	}

	affectedRows := uint64(input.Batch.RowCount())
	insert.ctr.buf.CleanOnlyData()
	for i := range insert.ctr.buf.Attrs {
		if insert.ctr.buf.Vecs[i] == nil {
			insert.ctr.buf.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
		}
		if err := insert.ctr.buf.Vecs[i].UnionBatch(input.Batch.Vecs[i], 0, input.Batch.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
			return input, err
		}
	}
	insert.ctr.buf.SetRowCount(input.Batch.RowCount())

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

	// insert into table, insertBat will be deeply copied into txn's workspace.
	err = insert.ctr.source.Write(newCtx, insert.ctr.buf)
	if err != nil {
		return input, err
	}
	analyzer.AddWrittenRows(int64(insert.ctr.buf.RowCount()))
	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)

	if insert.InsertCtx.AddAffectedRows {
		insert.addAffectedRowsFunc(affectedRows)
	}
	// `insertBat` does not include partition expression columns
	return input, nil
}

func (insert *Insert) getInput(
	proc *process.Process,
	analyzer process.Analyzer,
) (vm.CallResult, error) {
	if !insert.delegated {
		input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
		if err != nil {
			return input, err
		}

		insert.input = input
	}

	return insert.input, nil
}

func (insert *Insert) getS3Writer(id uint64) (*colexec.S3Writer, error) {
	return insert.ctr.s3Writer, nil
}

func (insert *Insert) getFlushableS3Writer() *colexec.S3Writer {
	w := insert.ctr.s3Writer
	insert.ctr.s3Writer = nil
	return w
}

func writeBatch(proc *process.Process, writer *colexec.S3Writer, bat *batch.Batch, analyzer process.Analyzer) error {
	if writer.StashBatch(proc, bat) {
		crs := analyzer.GetOpCounterSet()
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

		blockInfos, stats, err := writer.SortAndSync(newCtx, proc)
		if err != nil {
			return err
		}
		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)

		err = writer.FillBlockInfoBat(blockInfos, stats, proc.GetMPool())
		if err != nil {
			return err
		}
	}
	return nil
}

func flushTailBatch(
	proc *process.Process,
	writer *colexec.S3Writer,
	result *vm.CallResult,
	analyzer process.Analyzer,
) error {
	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

	blockInfos, stats, err := writer.FlushTailBatch(newCtx, proc)
	if err != nil {
		return err
	}
	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)

	// if stats is not zero, then the blockInfos must not be nil
	if !stats.IsZero() {
		err = writer.FillBlockInfoBat(blockInfos, stats, proc.GetMPool())
		if err != nil {
			return err
		}
	}

	return writer.Output(proc, result)
}

func (insert *Insert) getTableID(
	proc *process.Process,
) uint64 {
	id := uint64(0)
	if insert.ctr.source != nil {
		id = insert.ctr.source.GetTableID(proc.Ctx)
	}
	return id
}

func (insert *Insert) addAffectedRows(
	affectedRows uint64,
) {
	insert.ctr.affectedRows += affectedRows
}
