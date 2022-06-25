// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"context"
	"errors"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAcquireFutureWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must panic")
		}
	}()
	acquireFuture(context.Background(), nil, SendOptions{})
}

func TestCloseChanAfterGC(t *testing.T) {
	f := &Future{c: make(chan struct{}, 1)}
	c := f.c
	c <- struct{}{}
	f.setFinalizer()
	f = nil
	debug.FreeOSMemory()
	for {
		select {
		case _, ok := <-c:
			if !ok {
				return
			}
		case <-time.After(time.Second * 5):
			assert.Fail(t, "failed")
		}
	}
}

func TestAcquireFuture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	defer f.Close()

	assert.NotNil(t, f)
	assert.False(t, f.mu.closed, false)
	assert.NotNil(t, f.c)
	assert.Equal(t, 0, len(f.c))
	assert.Equal(t, f.request, req)
	assert.Equal(t, ctx, f.ctx)
}

func TestReleaseFuture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	f.c <- struct{}{}
	f.err = errors.New("error")
	f.response = req
	f.Close()
	assert.True(t, f.mu.closed)
	assert.Equal(t, 0, len(f.c))
	assert.Nil(t, f.request)
	assert.Nil(t, f.response)
	assert.Nil(t, f.ctx)
}

func TestGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	defer f.Close()

	f.done(req, nil)
	resp, err := f.Get()
	assert.Nil(t, err)
	assert.Equal(t, req, resp)
}

func TestGetWithError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	defer f.Close()

	e := errors.New("error")
	f.done(nil, e)
	resp, err := f.Get()
	assert.NotNil(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, err, e)
}

func TestGetWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()

	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	defer f.Close()

	resp, err := f.Get()
	assert.NotNil(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, ctx.Err(), err)
}

func TestGetWithInvalidResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	defer f.Close()

	f.done(newTestMessage([]byte("id2")), nil)
	assert.Equal(t, 0, len(f.c))
}

func TestTimeoutDuration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	defer f.Close()

	d := f.timeoutDuration()
	assert.True(t, d > 0)
	assert.True(t, d <= time.Second)
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	req := newTestMessage([]byte("id"))
	f := acquireFuture(ctx, req, SendOptions{})
	defer f.Close()

	assert.False(t, f.timeout())
	cancel()
	assert.True(t, f.timeout())
}
