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

package lockservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFixedSlicePool(t *testing.T) {
	assert.Equal(t, 1, len(NewFixedSlicePool(1).slices))
	assert.Equal(t, 2, len(NewFixedSlicePool(2).slices))
	assert.Equal(t, 3, len(NewFixedSlicePool(3).slices))
	assert.Equal(t, 3, len(NewFixedSlicePool(4).slices))
	assert.Equal(t, 4, len(NewFixedSlicePool(5).slices))
	assert.Equal(t, 4, len(NewFixedSlicePool(6).slices))
	assert.Equal(t, 4, len(NewFixedSlicePool(7).slices))
	assert.Equal(t, 4, len(NewFixedSlicePool(8).slices))
}

func TestAcquire(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	fs := fsp.Acquire(1)
	assert.Equal(t, 1, fs.Cap())
	fs.Close()

	fs = fsp.Acquire(3)
	assert.Equal(t, 4, fs.Cap())
	fs.Close()

	fs = fsp.Acquire(5)
	assert.Equal(t, 8, fs.Cap())
	fs.Close()

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	fsp.Acquire(1024)
}

func TestRelease(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	fs := fsp.Acquire(1)
	fsp.Release(fs)
	assert.Equal(t, uint64(1), fsp.releaseV.Load())

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	fs = fsp.Acquire(1)
	fs.values = make([][]byte, 1024)
	fsp.Release(fs)
}

func TestFixedSliceAppend(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	fs := fsp.Acquire(4)
	defer fs.Close()

	for i := byte(0); i < 4; i++ {
		fs.Append([][]byte{{i}})
		assert.Equal(t, int(i+1), fs.Len())
	}
}

func TestFixedSliceJoin(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	fs1 := fsp.Acquire(4)
	defer fs1.Close()

	fs2 := fsp.Acquire(1)
	defer fs2.Close()
	fs2.Append([][]byte{{1}})

	fs1.Join(fs2, [][]byte{{2}})
	assert.Equal(t, 2, fs1.Len())
	assert.Equal(t, [][]byte{{1}, {2}}, fs1.values[:fs1.Len()])
}

func TestFxiedSliceRefAndUnRef(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	fs := fsp.Acquire(1)
	assert.Equal(t, int32(1), fs.atomic.ref.Load())
	fs.ref()
	assert.Equal(t, int32(2), fs.atomic.ref.Load())
	fs.Append([][]byte{{1}})

	fs.unref()
	assert.Equal(t, int32(1), fs.atomic.ref.Load())
	assert.Equal(t, 1, fs.Len())

	fs.unref()
	assert.Equal(t, int32(0), fs.atomic.ref.Load())
	assert.Equal(t, 0, fs.Len())

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	fs.unref()
}

func TestFxiedSliceIter(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	fs := fsp.Acquire(4)
	defer fs.Close()

	for i := byte(0); i < 4; i++ {
		fs.Append([][]byte{{i}})
	}

	var values [][]byte
	fs.Iter(func(b []byte) bool {
		values = append(values, b)
		return true
	})
	assert.Equal(t, fs.values[:fs.Len()], values)

	values = values[:0]
	fs.Iter(func(b []byte) bool {
		values = append(values, b)
		return false
	})
	assert.Equal(t, fs.values[:1], values)
}

func TestCowSliceAppend(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}, {2}, {3}})
	assert.Equal(t, 4, cs.fs.Load().(*FixedSlice).Cap())
	assert.Equal(t, uint64(1), fsp.acquireV.Load())

	cs.append([][]byte{{4}})
	assert.Equal(t, 4, cs.fs.Load().(*FixedSlice).Cap())
	assert.Equal(t, uint64(1), fsp.acquireV.Load())

	assert.Equal(t, [][]byte{{1}, {2}, {3}, {4}},
		cs.fs.Load().(*FixedSlice).values[:cs.fs.Load().(*FixedSlice).Len()])
}

func TestCowSliceAppendWithCow(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})
	assert.Equal(t, 1, cs.fs.Load().(*FixedSlice).Cap())
	assert.Equal(t, uint64(1), fsp.acquireV.Load())

	cs.append([][]byte{{2}})
	assert.Equal(t, 2, cs.fs.Load().(*FixedSlice).Cap())
	assert.Equal(t, uint64(2), fsp.acquireV.Load())

	assert.Equal(t, [][]byte{{1}, {2}},
		cs.fs.Load().(*FixedSlice).values[:cs.fs.Load().(*FixedSlice).Len()])
}

func TestCowSliceRead(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})

	s := cs.slice()
	assert.Equal(t, [][]byte{{1}}, s.values[:s.Len()])

	cs.append([][]byte{{2}})
	assert.Equal(t, uint64(0), fsp.releaseV.Load())

	assert.Equal(t, [][]byte{{1}}, s.values[:s.Len()])
	s.unref()
	assert.Equal(t, uint64(1), fsp.releaseV.Load())

	cs.close()
	assert.Equal(t, uint64(2), fsp.releaseV.Load())
}

func TestCowSliceAppendConcurrentWithSliceGetNew(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})
	var s *FixedSlice
	n := 0
	cs.hack.replace = func() {
		if s == nil {
			s = cs.slice()
		}
		n++
	}
	cs.append([][]byte{{2}})
	assert.Equal(t, 2, n)
	assert.Equal(t, uint64(1), fsp.releaseV.Load())
}

func TestCowSliceAppendConcurrentWithSliceGetOld(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})
	old := cs.fs.Load().(*FixedSlice)
	n := 0
	cs.hack.replace = func() {
		if n == 0 {
			old.ref()
			cs.v.Add(1)
		}
		n++
	}
	cs.append([][]byte{{2}})
	assert.Equal(t, 2, n)
	assert.Equal(t, uint64(0), fsp.releaseV.Load())

	old.unref()
	assert.Equal(t, uint64(1), fsp.releaseV.Load())
}

func TestCowSliceSliceReadConcurrentWithAppend(t *testing.T) {
	fsp := NewFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})

	n := 0
	cs.hack.slice = func() {
		if n == 0 {
			cs.append([][]byte{{2}})
		}
		n++
	}
	s := cs.slice()
	assert.Equal(t, [][]byte{{1}, {2}}, s.values[:s.Len()])
	s.unref()
	assert.Equal(t, uint64(1), fsp.releaseV.Load())

	s.Close()
	assert.Equal(t, uint64(2), fsp.releaseV.Load())
}
