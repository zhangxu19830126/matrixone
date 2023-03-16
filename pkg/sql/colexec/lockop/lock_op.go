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

package lockop

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(
	v any,
	buf *bytes.Buffer) {
	arg := v.(*Argument)
	buf.WriteString(fmt.Sprintf("lock-%s(%d)",
		arg.TableName,
		arg.TableID))
}

func Prepare(
	proc *process.Process,
	arg any) error {

	return nil
}

// Call returning only the first n tuples from its input
func Call(
	idx int,
	proc *process.Process,
	v any,
	isFirst bool,
	isLast bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}

	arg := v.(*Argument)
	pv := bat.GetVector(arg.PrimaryKeyIndex)
	arg.rows = arg.fetcher(pv, arg.rows)
	return false, nil
}
