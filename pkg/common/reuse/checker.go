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

package reuse

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"
)

var (
	idle  = step(0)
	inUse = step(1)
)

type step int

type checker[T ReusableObject] struct {
	enable bool
	mu     struct {
		sync.RWMutex
		// we use uintptr as key, to check leak free in gc triggered.
		// We cannot hold the *T in checker.
		m     map[uintptr]step
		stack map[uintptr]string
	}
}

func newChecker[T ReusableObject](enable bool) *checker[T] {
	c := &checker[T]{
		enable: enable,
	}
	c.mu.m = make(map[uintptr]step)
	c.mu.stack = make(map[uintptr]string)
	return c
}

func (c *checker[T]) created(v *T) {
	if !enableChecker || !c.enable {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	k := uintptr(unsafe.Pointer(v))
	c.mu.m[k] = idle
}

func (c *checker[T]) got(v *T) {
	if !enableChecker || !c.enable {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := uintptr(unsafe.Pointer(v))
	s, ok := c.mu.m[k]
	if !ok {
		panic("missing status")
	}

	switch s {
	case inUse:
		panic(fmt.Sprintf("double got from pool for type: %T", v))
	}
	c.mu.m[k] = inUse
	c.mu.stack[k] = string(debug.Stack())
}

func (c *checker[T]) free(v *T) {
	if !enableChecker || !c.enable {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := uintptr(unsafe.Pointer(v))
	s, ok := c.mu.m[k]
	if !ok {
		return
	}

	switch s {
	// the v is marked idle, means already free
	case idle:
		panic(fmt.Sprintf("double free for type: %T", v))
	}
	c.mu.m[k] = idle
}

func (c *checker[T]) gc(v *T) {
	if !enableChecker || !c.enable {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := uintptr(unsafe.Pointer(v))
	s, ok := c.mu.m[k]
	if !ok {
		return
	}

	switch s {
	// the v is marked in use, but v is release by gc
	case inUse:
		panic(fmt.Sprintf("%d missing free for type: %T(%p), %+v, create by: \n<<<%s>>>\n",
			k, v, v, v, c.mu.stack[k]))
	}

	delete(c.mu.m, k)
}

func RunReuseTests(fn func()) {
	enableChecker = true
	defer func() {
		enableChecker = false
	}()
	fn()
	v := &waiterGC{
		data: make([]byte, 1024),
	}
	c := make(chan struct{})
	runtime.SetFinalizer(
		v,
		func(v *waiterGC) {
			close(c)
		})
	v = nil
	debug.FreeOSMemory()
	<-c
}

type waiterGC struct {
	data []byte
}
