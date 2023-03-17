// Copyright 2023 Matrix Origin
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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func constructLockWithInsert(
	n *plan.Node,
	eg engine.Engine,
	proc *process.Process) (*lockop.Argument, error) {
	if proc.TxnOperator.Txn().Mode != txn.TxnMode_Pessimistic {
		return nil, nil
	}

	ctx := n.InsertCtx
	return constructLock(n, eg, proc, ctx.TableDef)
}

func constructLockWithDelete(
	n *plan.Node,
	eg engine.Engine,
	proc *process.Process) ([]*lockop.Argument, error) {
	if proc.TxnOperator.Txn().Mode != txn.TxnMode_Pessimistic {
		return nil, nil
	}

	ctx := n.DeleteCtx
	var args []*lockop.Argument
	for _, def := range ctx.OnSetDef {
		v, err := constructLock(n, eg, proc, def)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}
	return args, nil
}

func constructLock(
	n *plan.Node,
	eg engine.Engine,
	proc *process.Process,
	tableDef *plan.TableDef) (*lockop.Argument, error) {
	if proc.TxnOperator.Txn().Mode != txn.TxnMode_Pessimistic {
		return nil, nil
	}

	if tableDef == nil {
		panic("missing table def")
	}
	// no primary key, no lock needed
	if tableDef.Pkey == nil {
		return nil, nil
	}

	pkIdx := -1
	var pkType types.Type
	name := tableDef.Pkey.PkeyColName
	if name == "" {
		if len(tableDef.Pkey.Names) != 1 {
			panic("BUG: multi pk names")
		}
		name = tableDef.Pkey.Names[0]
	}
	for idx, c := range tableDef.Cols {
		if c.Name == name {
			pkIdx = idx
			pkType = types.Type{
				Oid:   types.T(c.Typ.Id),
				Width: c.Typ.Width,
				Size:  c.Typ.Size,
				Scale: c.Typ.Scale,
			}
			break
		}
	}
	if pkIdx == -1 {
		panic("pk column not found")
	}

	return lockop.NewArgument(
		tableDef.TblId,
		tableDef.Name,
		int32(pkIdx),
		pkType,
		lock.LockMode_Exclusive,
	), nil
}
