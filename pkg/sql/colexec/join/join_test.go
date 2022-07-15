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

package join

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/joincondition"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []joinTestCase
)

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []joinTestCase{
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_decimal64}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_decimal64}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_decimal128}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_decimal128}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_int64})},
				},
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_int64})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_int64})},
				},
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_int64})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal64}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal64})},
				},
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal64, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal64}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal64})},
				},
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal64, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal128}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal128})},
				},
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal128, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal128}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal128})},
				},
				{
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_decimal128, Scale: 1})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_decimal64}, {Oid: types.T_char}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64, Scale: 1})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_decimal64}, {Oid: types.T_char}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64, Scale: 1})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal64})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_decimal128}, {Oid: types.T_char}}, []ResultPos{{0, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128, Scale: 1})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_decimal128}, {Oid: types.T_char}}, []ResultPos{{0, 0}, {1, 0}},
			[][]joincondition.Condition{
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128, Scale: 1})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
				{
					{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_decimal128})},
					{Scale: 0, Expr: newExpr(1, types.Type{Oid: types.T_char})},
				},
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
	}
}

func TestJoin(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			if ok, err := Call(tc.proc, tc.arg); ok || err != nil {
				break
			}
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

func BenchmarkJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hm := host.New(1 << 30)
		gm := guest.New(1<<30, hm)
		tcs = []joinTestCase{
			newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}, {1, 0}},
				[][]joincondition.Condition{
					{
						{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
					},
					{
						{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
					},
				}),
			newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}, {1, 0}},
				[][]joincondition.Condition{
					{
						{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
					},
					{
						{Scale: 0, Expr: newExpr(0, types.Type{Oid: types.T_int8})},
					},
				}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
			for {
				if ok, err := Call(tc.proc, tc.arg); ok || err != nil {
					break
				}
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
			}
		}
	}
}

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: &plan.Type{
			Size:  typ.Size,
			Scale: typ.Scale,
			Width: typ.Width,
			Id:    plan.Type_TypeId(typ.Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type, rp []ResultPos, cs [][]joincondition.Condition) joinTestCase {
	proc := process.New(m)
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Result:     rp,
			Conditions: cs,
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp)
}
