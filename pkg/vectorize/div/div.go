// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package div

/*
#include "mo.h"

#cgo CFLAGS: -I../../../cgo
#cgo LDFLAGS: -L../../../cgo -lmo -lm
*/
import "C"

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"golang.org/x/exp/constraints"
)

const (
	LEFT_IS_SCALAR  = 1
	RIGHT_IS_SCALAR = 2
)

func NumericDivFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[T](rs)
	flag := 0
	if xs.IsConst() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsConst() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.Float_VecDiv(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(rs.GetType().TypeSize()))
	if rc != 0 {
		return moerr.NewDivByZeroNoCtx()
	}
	return nil
}

func NumericIntegerDivFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[int64](rs)
	flag := 0
	if xs.IsConst() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsConst() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.Float_VecIntegerDiv(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag), C.int32_t(rs.GetType().TypeSize()))
	if rc != 0 {
		return moerr.NewDivByZeroNoCtx()
	}
	return nil
}
