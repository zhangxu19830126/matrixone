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

package sum

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewDecimal64(typ types.Type) *Decimal64Ring {
	//return &Decimal64Ring{Typ: typ}
	return &Decimal64Ring{Typ: types.Type{Oid: typ.Oid, Size: typ.Size, Width: typ.Width, Scale: typ.Scale}}
}

func (r *Decimal64Ring) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *Decimal64Ring) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *Decimal64Ring) Count() int {
	return len(r.Vs)
}

func (r *Decimal64Ring) Size() int {
	return cap(r.Da)
}

func (r *Decimal64Ring) Dup() ring.Ring {
	return &Decimal64Ring{
		Typ: r.Typ,
	}
}

func (r *Decimal64Ring) Type() types.Type {
	return r.Typ
}

func (r *Decimal64Ring) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
}

func (r *Decimal64Ring) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
}

func (r *Decimal64Ring) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *Decimal64Ring) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, 64)
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
		r.Vs = encoding.DecodeDecimal64Slice(data)
	} else if n+1 >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeDecimal64Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Da = r.Da[:(n+1)*8]
	r.Vs[n] = 0
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *Decimal64Ring) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
		r.Vs = encoding.DecodeDecimal64Slice(data)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeDecimal64Slice(data)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
	}
	return nil
}

// Fill Fixme:what is this z?
func (r *Decimal64Ring) Fill(i int64, sel, z int64, vec *vector.Vector) {
	r.Vs[i] += types.Decimal64Int64Mul(vec.Col.([]types.Decimal64)[sel], z)
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
	}
}

func (r *Decimal64Ring) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]types.Decimal64)
	for i := range os {
		r.Vs[vps[i]-1] += types.Decimal64Int64Mul(vs[int64(i)+start], zs[int64(i)+start])
	}
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.Ns[vps[i]-1] += zs[int64(i)+start]
			}
		}
	}
}

func (r *Decimal64Ring) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]types.Decimal64)
	for j, v := range vs {
		tmp := types.Decimal64Int64Mul(v, zs[j])
		r.Vs[i] = types.Decimal64AddAligned(r.Vs[i], tmp)
		if nulls.Any(vec.Nsp) {
			for k := range vs {
				if nulls.Contains(vec.Nsp, uint64(k)) {
					r.Ns[i] += zs[k]
				}
			}
		}
	}
}

func (r *Decimal64Ring) Add(a interface{}, x, y int64) {
	ar := a.(*Decimal64Ring)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	r.Vs[x] += ar.Vs[y]
	r.Ns[x] += ar.Ns[y]
}

func (r *Decimal64Ring) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*Decimal64Ring)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	for i := range os {
		r.Vs[vps[i]-1] += ar.Vs[int64(i)+start]
		r.Ns[vps[i]-1] += ar.Ns[int64(i)+start]
	}
}

// Mul r[x] += a[y] * z
func (r *Decimal64Ring) Mul(a interface{}, x, y, z int64) {
	ar := a.(*Decimal64Ring)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	r.Ns[x] += ar.Ns[y] * z
	r.Vs[x] += types.Decimal64Int64Mul(r.Vs[y], z)
}

func (r *Decimal64Ring) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if z-r.Ns[i] == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}
	resultVecType := r.Typ
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Da,
		Col:  r.Vs,
		Or:   false,
		Typ:  resultVecType,
	}
}
