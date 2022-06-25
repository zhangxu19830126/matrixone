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
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/util/hack"
	"github.com/matrixorigin/matrixone/pkg/common/stop"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var (
	stateRunning = int32(0)
	stateStopped = int32(1)
)

// WithBackendLogger set the backend logger
func WithBackendLogger(logger *zap.Logger) BackendOption {
	return func(rb *remoteBackend) {
		rb.logger = logger
	}
}

// WithBackendBufferSize set the buffer size of the wait send chan.
// Default is 1024.
func WithBackendBufferSize(size int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.bufferSize = size
	}
}

// WithBackendBusyBufferSize if len(writeC) >= size, backend is busy.
// Default is 3/4 buffer size.
func WithBackendBusyBufferSize(size int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.busySize = size
	}
}

// WithBackendFilter set send fiter func. Input ready to send futures, output
// is really need to be send futures.
func WithBackendFilter(filter func([]*Future) []*Future) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.filter = filter
	}
}

// WithBackendBatchSendSize set the maximum number of messages to be sent together
// at each batch. Default is 8.
func WithBackendBatchSendSize(size int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.batchSendSize = size
	}
}

// WithBackendConnectWhenCreate connection the goetty connection while create the
// backend.
func WithBackendConnectWhenCreate() BackendOption {
	return func(rb *remoteBackend) {
		rb.options.connect = true
	}
}

// WithBackendConnectTimeout set the timeout for connect to remote. Default 10s.
func WithBackendConnectTimeout(timeout time.Duration) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.connectTimeout = timeout
	}
}

// WithBackendGoettyOptions set goetty connection options. e.g. set read/write buffer
// size, adjust net.Conn attribute etc.
func WithBackendGoettyOptions(options ...goetty.Option) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.goettyOptions = options
	}
}

type remoteBackend struct {
	remote     string
	logger     *zap.Logger
	codec      Codec
	conn       goetty.IOSession
	writeC     chan *Future
	resetReadC chan struct{}
	resetConnC chan struct{}
	stopper    *stop.Stopper
	closeOnce  sync.Once

	options struct {
		connect        bool
		goettyOptions  []goetty.Option
		connectTimeout time.Duration
		bufferSize     int
		busySize       int
		batchSendSize  int
		filter         func([]*Future) []*Future
	}

	stateMu struct {
		sync.RWMutex
		state int32
	}

	mu struct {
		sync.RWMutex
		futures map[string]*Future
	}
}

// NewRemoteBackend create a goetty connection based backend. This backend will start 2
// goroutiune, one for read and one for write. If there is a network error in the underlying
// goetty connection, it will automatically retry until the Future times out.
func NewRemoteBackend(
	remote string,
	codec Codec,
	options ...BackendOption) (Backend, error) {
	rb := &remoteBackend{
		stopper:    stop.NewStopper(fmt.Sprintf("backend-%s", remote)),
		remote:     remote,
		codec:      codec,
		resetReadC: make(chan struct{}, 1),
		resetConnC: make(chan struct{}),
	}

	for _, opt := range options {
		opt(rb)
	}
	rb.adjust()

	rb.writeC = make(chan *Future, rb.options.bufferSize)
	rb.mu.futures = make(map[string]*Future, rb.options.bufferSize)
	rb.conn = goetty.NewIOSession(rb.options.goettyOptions...)

	if rb.options.connect {
		if err := rb.resetConn(); err != nil {
			rb.logger.Error("connect to remote failed", zap.Error(err))
			return nil, err
		}
	}

	if err := rb.stopper.RunTask(rb.writeLoop); err != nil {
		return nil, err
	}
	if err := rb.stopper.RunTask(rb.readLoop); err != nil {
		return nil, err
	}

	rb.activeReadLoop()
	return rb, nil
}

func (rb *remoteBackend) adjust() {
	if rb.options.bufferSize == 0 {
		rb.options.bufferSize = 1024
	}
	if rb.options.busySize == 0 {
		rb.options.busySize = rb.options.bufferSize * 3 / 4
		if rb.options.busySize == 0 {
			rb.options.busySize = 1
		}
	}
	if rb.options.batchSendSize == 0 {
		rb.options.batchSendSize = 8
	}
	if rb.options.connectTimeout == 0 {
		rb.options.connectTimeout = time.Second * 10
	}
	if rb.options.filter == nil {
		rb.options.filter = func(futures []*Future) []*Future {
			return futures
		}
	}

	rb.logger = logutil.Adjust(rb.logger).With(zap.String("remote", rb.remote))
	rb.options.goettyOptions = append(rb.options.goettyOptions,
		goetty.WithCodec(rb.codec, rb.codec),
		goetty.WithLogger(rb.logger))
}

func (rb *remoteBackend) Send(future *Future) error {
	added := false
	for {
		rb.stateMu.RLock()
		if rb.stateMu.state == stateStopped {
			rb.stateMu.RUnlock()
			return errBackendClosed
		}

		if !added {
			if !rb.addFuture(future) {
				return nil
			}
			added = true
		}

		// The close method need acquire the write lock, so we cannot block at here.
		// The write loop may reset the backend's network link and may not be able to
		// process writeC for a long time, causing the writeC buffer to reach its limit.
		select {
		case rb.writeC <- future:
			rb.stateMu.RUnlock()
			return nil
		case <-future.ctx.Done():
			rb.stateMu.RUnlock()
			return nil
		default:
			rb.stateMu.RUnlock()
		}
	}
}

func (rb *remoteBackend) Close() {
	rb.stateMu.Lock()
	if rb.stateMu.state == stateStopped {
		rb.stateMu.Unlock()
		return
	}
	rb.stateMu.state = stateStopped
	rb.stateMu.Unlock()

	rb.closeConn()
	rb.stopper.Stop()
	rb.doClose()
}

func (rb *remoteBackend) Busy() bool {
	return len(rb.writeC) >= rb.options.busySize
}

func (rb *remoteBackend) writeLoop(ctx context.Context) {
	rb.logger.Info("write loop started")
	defer rb.logger.Info("write loop stopped")

	retry := false
	futures := make([]*Future, 0, rb.options.batchSendSize)
	fetch := func() {
		for i := 0; i < len(futures); i++ {
			futures[i] = nil
		}
		futures = futures[:0]

		for i := 0; i < rb.options.batchSendSize; i++ {
			select {
			case f, ok := <-rb.writeC:
				if ok {
					futures = append(futures, f)
				}
			default:
				if len(futures) > 0 {
					return
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-rb.resetConnC:
			if ok {
				if err := rb.resetConn(); err != nil {
					rb.logger.Error("fail to reset backend connection",
						zap.Error(err))
				}
			}
		default:
			if !retry {
				fetch()
			}

			if len(futures) > 0 {
				retry = false
				written := 0
				writeTimeout := time.Duration(0)
				sendFutures := rb.options.filter(futures)
				for _, f := range sendFutures {
					if !f.timeout() {
						writeTimeout += f.opts.Timeout
						if err := rb.conn.Write(f.request, goetty.WriteOptions{}); err != nil {
							rb.logger.Error("write request failed",
								zap.String("request-id", hex.EncodeToString(f.request.ID())),
								zap.Error(err))
							retry = true
							written = 0
							break
						}
						written++
					}
				}

				if written > 0 {
					if err := rb.conn.Flush(writeTimeout); err != nil {
						for _, f := range sendFutures {
							rb.logger.Error("write request failed",
								zap.String("request-id", hex.EncodeToString(f.request.ID())),
								zap.Error(err))
						}
						retry = true
					}
				}
			}
		}
	}
}

func (rb *remoteBackend) readLoop(ctx context.Context) {
	rb.logger.Info("read loop started")
	defer rb.logger.Error("read loop stopped")

	for {
		select {
		case <-ctx.Done():
			rb.clean()
			return
		case _, ok := <-rb.resetReadC:
			if ok {
				rb.logger.Info("read loop actived, ready to read from backend")
				for {
					msg, err := rb.conn.Read(goetty.ReadOptions{})
					if err != nil {
						rb.logger.Error("read from backend failed, wait for reactive read loop",
							zap.Error(err))
						rb.scheduleResetConn()
						break
					}

					rb.requestDone(msg.(Message))
				}
			}
		}
	}
}

func (rb *remoteBackend) doClose() {
	rb.closeOnce.Do(func() {
		close(rb.resetConnC)
		close(rb.resetReadC)
		close(rb.writeC)
		rb.closeConn()
	})
}

func (rb *remoteBackend) clean() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for id := range rb.mu.futures {
		delete(rb.mu.futures, id)
	}
}

func (rb *remoteBackend) requestDone(response Message) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	id := hack.SliceToString(response.ID())
	if f, ok := rb.mu.futures[id]; ok {
		delete(rb.mu.futures, id)
		f.done(response, nil)
	}
}

func (rb *remoteBackend) addFuture(future *Future) bool {
	id := hack.SliceToString(future.request.ID())
	rb.mu.Lock()
	defer rb.mu.Unlock()
	d := future.timeoutDuration()
	if d <= 0 {
		return false
	}

	future.setContextDoneCallback(rb.onTimeout)
	rb.mu.futures[id] = future
	return true
}

func (bc *remoteBackend) onTimeout(request Message) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	delete(bc.mu.futures, hack.SliceToString(request.ID()))
}

func (rb *remoteBackend) resetConn() error {
	wait := time.Second
	for {
		if !rb.running() {
			return errBackendClosed
		}

		rb.logger.Info("start connect to remote")
		rb.closeConn()
		ok, err := rb.conn.Connect(rb.remote, rb.options.connectTimeout)
		if err == nil && ok {
			rb.logger.Info("connect to remote succeed")
			rb.activeReadLoop()
			return nil
		}
		rb.logger.Error("init remote connection failed, retry later",
			zap.Error(err))
		time.Sleep(wait)
		wait += wait / 2
	}
}

func (rb *remoteBackend) activeReadLoop() {
	select {
	case rb.resetReadC <- struct{}{}:
	default:
	}
}

func (rb *remoteBackend) running() bool {
	rb.stateMu.RLock()
	defer rb.stateMu.RUnlock()

	return rb.stateMu.state == stateRunning
}

func (rb *remoteBackend) scheduleResetConn() {
	if !rb.running() {
		return
	}

	select {
	case rb.resetConnC <- struct{}{}:
		rb.logger.Debug("schedule reset remote connection")
	case <-time.After(time.Second * 10):
		rb.logger.Fatal("BUG: schedule reset remote connection timeout")
	}
}

func (rb *remoteBackend) closeConn() {
	if err := rb.conn.Close(); err != nil {
		rb.logger.Error("close remote conn failed",
			zap.Error(err))
	}
}

type goettyBasedBackendFactory struct {
	codec   Codec
	options []BackendOption
}

func NewGoettyBasedBackendFactory(codec Codec, options ...BackendOption) BackendFactory {
	return &goettyBasedBackendFactory{
		codec:   codec,
		options: options,
	}
}

func (bf *goettyBasedBackendFactory) Create(remote string) (Backend, error) {
	return NewRemoteBackend(remote, bf.codec, bf.options...)
}
