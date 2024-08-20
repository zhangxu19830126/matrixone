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

package product

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "product"

func (product *Product) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": cross join ")
}

func (product *Product) OpType() vm.OpType {
	return vm.Product
}

func (product *Product) Prepare(proc *process.Process) error {
	ap := product
	ap.ctr = new(container)
	if product.ProjectList != nil {
		return product.PrepareProjection(proc)
	}
	return nil
}

func (product *Product) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(product.GetIdx(), product.GetParallelIdx(), product.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ap := product
	ctr := ap.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err = product.build(proc, anal); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			if ctr.inBat == nil {
				result, err = product.Children[0].Call(proc)
				if err != nil {
					return result, err
				}
				ctr.inBat = result.Batch
				if ctr.inBat == nil {
					ctr.state = End
					continue
				}
				if ctr.inBat.IsEmpty() {
					ctr.inBat = nil
					continue
				}
				if ctr.bat == nil {
					ctr.inBat = nil
					continue
				}
				anal.Input(ctr.inBat, product.GetIsFirst())
			}

			if err := ctr.probe(ap, proc, &result); err != nil {
				return result, err
			}
			if product.ProjectList != nil {
				var err error
				result.Batch, err = product.EvalProjection(result.Batch, proc)
				if err != nil {
					return result, err
				}
			}
			anal.Output(result.Batch, product.GetIsLast())
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (product *Product) build(proc *process.Process, anal process.Analyze) error {
	ctr := product.ctr
	start := time.Now()
	defer anal.WaitStop(start)
	mp := message.ReceiveJoinMap(product.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
	if mp == nil {
		return nil
	}
	batches := mp.GetBatches()
	var err error
	//maybe optimize this in the future
	for i := range batches {
		ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), batches[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) probe(ap *Product, proc *process.Process, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.inBat.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}
	count := ctr.inBat.RowCount()
	count2 := ctr.bat.RowCount()
	var i, j int
	for j = ctr.probeIdx; j < count2; j++ {
		for i = 0; i < count; i++ {
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
						return err
					}
				}
			}
		}
		if ctr.rbat.Vecs[0].Length() >= colexec.DefaultBatchSize {
			result.Batch = ctr.rbat
			ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
			ctr.probeIdx = j + 1
			return nil
		}
	}
	// ctr.rbat.AddRowCount(count * count2)
	ctr.probeIdx = 0
	ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
	result.Batch = ctr.rbat

	proc.PutBatch(ctr.inBat)
	ctr.inBat = nil
	return nil
}
