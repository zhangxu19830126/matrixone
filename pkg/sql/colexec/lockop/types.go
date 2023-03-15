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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	argPool = sync.Pool{
		New: func() any {
			return &Argument{}
		},
	}
)

type Argument struct {
	// TableID table id
	TableID uint64
	// PrimaryKeyIndex primary key index in table columns
	PrimaryKeyIndex int
	// Service lock service
	Service lockservice.LockService
}

// NewArgument create new argument
func NewArgument() *Argument {
	return argPool.Get().(*Argument)
}

// interface
func (arg *Argument) Free(
	proc *process.Process,
	pipelineFailed bool) {
	argPool.Put(arg)
}
