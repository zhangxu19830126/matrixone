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
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	eventsWorkers = 4
)

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	bind     pb.LockTable
	fsp      *fixedSlicePool
	detector *detector
	clock    clock.Clock
	events   *waiterEvents
	mu       struct {
		sync.RWMutex
		closed           bool
		store            LockStorage
		tableCommittedAt timestamp.Timestamp
	}
}

func newLocalLockTable(
	bind pb.LockTable,
	fsp *fixedSlicePool,
	detector *detector,
	events *waiterEvents,
	clock clock.Clock) lockTable {
	l := &localLockTable{
		bind:     bind,
		fsp:      fsp,
		detector: detector,
		clock:    clock,
		events:   events,
	}
	l.mu.store = newBtreeBasedStorage()
	l.mu.tableCommittedAt, _ = clock.Now()
	return l
}

func (l *localLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	cb func(pb.Result, error)) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock.local")
	defer span.End()

	logLocalLock(txn, l.bind.Table, rows, opts)
	c := l.newLockContext(ctx, txn, rows, opts, cb, l.bind)
	if opts.async {
		c.lockFunc = l.doLock
	}
	l.doLock(c, false)
}

func (l *localLockTable) doLock(
	c *lockContext,
	blocked bool) {
	// deadlock detected, return
	if c.txn.deadlockFound {
		c.done(ErrDeadLockDetected)
		return
	}

	var err error
	table := l.bind.Table
	for {
		// blocked used for async callback, waiter is created, and added to wait list.
		// So only need wait notify.
		if !blocked {
			err = l.doAcquireLock(c)
			if err != nil {
				logLocalLockFailed(c.txn, table, c.rows, c.opts, err)
				c.done(err)
				return
			}
			// no waiter, all locks are added
			if c.w == nil {
				c.txn.clearBlocked(c.txn.txnID)
				logLocalLockAdded(c.txn, l.bind.Table, c.rows, c.opts)
				if c.result.Timestamp.IsEmpty() {
					c.result.Timestamp = c.lockedTS
				}
				c.done(nil)
				return
			}

			// we handle remote lock on current rpc io read goroutine, so we can not wait here, otherwise
			// the rpc will be blocked.
			if c.opts.async {
				return
			}
		}

		// txn is locked by service.lock or service_remote.lock. We must unlock here. And lock again after
		// wait result. Because during wait, deadlock detection may be triggered, and need call txn.fetchWhoWaitingMe,
		// or other concurrent txn method.
		oldTxnID := c.txn.txnID
		c.txn.Unlock()
		v := c.w.wait(c.ctx)
		c.txn.Lock()

		logLocalLockWaitOnResult(c.txn, table, c.rows[c.idx], c.opts, c.w, v)
		if v.err != nil {
			// TODO: c.w's ref is 2, after close is 1. leak.
			c.w.close()
			c.done(v.err)
			return
		}
		// txn closed between Unlock and get Lock again
		if !bytes.Equal(oldTxnID, c.txn.txnID) {
			c.w.close()
			c.done(ErrTxnNotFound)
			return
		}

		c.w.resetWait()
		c.offset = c.idx
		c.result.Timestamp = v.ts
		c.result.HasConflict = true
		c.result.TableDefChanged = v.defChanged
		if !c.result.HasPrevCommit {
			c.result.HasPrevCommit = !v.ts.IsEmpty()
		}
		if c.opts.TableDefChanged {
			c.opts.TableDefChanged = v.defChanged
		}
		blocked = false
	}
}

func (l *localLockTable) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp) {
	logUnlockTableOnLocal(
		l.bind.ServiceID,
		txn,
		l.bind)

	locks := ls.slice()
	defer locks.unref()

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.closed {
		return
	}

	var startKey []byte
	locks.iter(func(key []byte) bool {
		if lock, ok := l.mu.store.Get(key); ok {
			if lock.isLockRangeStart() {
				startKey = key
				return true
			}

			lock.closeTxn(
				txn,
				notifyValue{ts: commitTS})
			logLockUnlocked(txn, key, lock)
			if lock.isEmpty() {
				l.mu.store.Delete(key)
				if len(startKey) > 0 {
					l.mu.store.Delete(startKey)
					startKey = nil
				}
			}
		}
		return true
	})
	if l.mu.tableCommittedAt.Less(commitTS) {
		l.mu.tableCommittedAt = commitTS
	}
}

func (l *localLockTable) getLock(
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.mu.closed {
		return
	}
	lock, ok := l.mu.store.Get(key)
	if ok {
		fn(lock)
	}
}

func (l *localLockTable) getBind() pb.LockTable {
	return l.bind
}

func (l *localLockTable) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.closed = true

	l.mu.store.Iter(func(key []byte, lock Lock) bool {
		if lock.isLockRow() || lock.isLockRangeEnd() {
			// if there are waiters in the current lock, just notify
			// the head, and the subsequent waiters will be notified
			// by the previous waiter.
			lock.close(notifyValue{err: ErrLockTableNotFound})
		}
		return true
	})
	l.mu.store.Clear()
	logLockTableClosed(l.bind, false)
}

func (l *localLockTable) doAcquireLock(c *lockContext) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.mu.closed {
		return moerr.NewInvalidStateNoCtx("local lock table closed")
	}

	switch c.opts.Granularity {
	case pb.Granularity_Row:
		return l.acquireRowLockLocked(c)
	case pb.Granularity_Range:
		if len(c.rows) == 0 ||
			len(c.rows)%2 != 0 {
			panic("invalid range lock")
		}
		return l.acquireRangeLockLocked(c)
	default:
		panic(fmt.Sprintf("not support lock granularity %d", c.opts.Granularity))
	}
}

func (l *localLockTable) acquireRowLockLocked(c *lockContext) error {
	n := len(c.rows)
	for idx := c.offset; idx < n; idx++ {
		row := c.rows[idx]

		key, lock, ok := l.mu.store.Seek(row)
		if ok &&
			(bytes.Equal(key, row) ||
				lock.isLockRangeEnd()) {
			if lock.tryHold(c) {
				if c.w != nil {
					c.w.close()
					c.w = nil
				}
				c.txn.lockAdded(l.bind.Table, [][]byte{key})
				continue
			}

			// need wait for prev txn closed
			if c.w == nil {
				c.w = acquireWaiter(c.waitTxn)
			}

			c.offset = idx
			if c.opts.async {
				l.events.add(c)
			}
			l.handleLockConflictLocked(c, key, lock)
			return nil
		}
		l.addRowLockLocked(c, row)
		// lock added, need create new waiter next time
		c.w = nil
	}

	c.offset = 0
	c.lockedTS = l.mu.tableCommittedAt
	return nil
}

func (l *localLockTable) acquireRangeLockLocked(c *lockContext) error {
	n := len(c.rows)
	for i := c.offset; i < n; i += 2 {
		start := c.rows[i]
		end := c.rows[i+1]
		if bytes.Compare(start, end) >= 0 {
			panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
				start, end))
		}

		logLocalLockRange(c.txn, l.bind.Table, start, end, c.opts.Mode)
		conflict, conflictWith, err := l.addRangeLockLocked(c, start, end)
		if err != nil {
			return err
		}
		if len(conflict) > 0 {
			c.w = acquireWaiter(c.waitTxn)
			if c.opts.async {
				l.events.add(c)
			}
			l.handleLockConflictLocked(c, conflict, conflictWith)
			c.offset = i
			return nil
		}

		// lock added, need create new waiter next time
		c.w = nil
	}
	c.offset = 0
	c.lockedTS = l.mu.tableCommittedAt
	return nil
}

func (l *localLockTable) addRowLockLocked(
	c *lockContext,
	row []byte) {
	lock := newRowLock(c)

	// we must first add the lock to txn to ensure that the
	// lock can be read when the deadlock is detected.
	c.txn.lockAdded(l.bind.Table, [][]byte{row})
	l.mu.store.Add(row, lock)
}

func (l *localLockTable) handleLockConflictLocked(
	c *lockContext,
	key []byte,
	conflictWith Lock) error {
	// find conflict, and wait prev txn completed, and a new
	// waiter added, we need to active deadlock check.
	c.txn.setBlocked(c.txn.txnID, c.w)

	var err error
	conflictWith.waiters.beginChange()
	defer func() {
		if err != nil {
			conflictWith.waiters.rollbackChange()
			return
		}

		conflictWith.waiters.commitChange()
		logLocalLockWaitOn(c.txn, l.bind.Table, c.w, key, conflictWith)
	}()

	// added to waiters list, and wait for notify
	conflictWith.addWaiter(c.w)
	for _, txn := range conflictWith.holders.txns {
		if err = l.detector.check(txn.TxnID, c.waitTxn); err != nil {
			return err
		}
	}
	return err
}

func (l *localLockTable) addRangeLockLocked(
	c *lockContext,
	start, end []byte) ([]byte, Lock, error) {

	wq := newWaiterQueue()
	mc := newMergeContext(wq)
	defer mc.close()

	var err error
	var conflictWith Lock
	var conflictKey []byte
	var prevStartKey []byte
	rangeStartEncountered := false
	// TODO: remove mem allocate.
	upperBounded := nextKey(end, nil)

	for {
		l.mu.store.Range(
			start,
			nil,
			func(key []byte, keyLock Lock) bool {
				// current txn is not holder, maybe conflict
				if !keyLock.holders.contains(c.txn.txnID) {
					hasConflict := false
					if keyLock.isLockRow() {
						// row lock, start <= key <= end
						hasConflict = bytes.Compare(key, end) <= 0
					} else if keyLock.isLockRangeStart() {
						// range start lock, [1, 4] + [2, any]
						hasConflict = bytes.Compare(key, end) <= 0
					} else {
						// range end lock, always conflict
						hasConflict = true
					}

					if hasConflict {
						conflictWith = keyLock
						conflictKey = key
					}
					return false
				}

				if keyLock.holders.size() > 1 {
					err = ErrMergeRangeLockNotSupport
					return false
				}

				// merge current txn locks
				if keyLock.isLockRangeStart() {
					prevStartKey = key
					rangeStartEncountered = true
					return bytes.Compare(key, end) < 0
				}
				if rangeStartEncountered &&
					!keyLock.isLockRangeEnd() {
					panic("BUG, missing range end key")
				}

				start, end = l.mergeRangeLocked(
					start, end,
					prevStartKey,
					key, keyLock,
					mc,
					c.txn)
				prevStartKey = nil
				rangeStartEncountered = false
				return bytes.Compare(key, end) < 0
			})
		if err != nil {
			mc.rollback()
			return nil, Lock{}, err
		}

		if len(conflictKey) > 0 {
			if conflictWith.tryHold(c) {
				c.txn.lockAdded(l.bind.Table, [][]byte{conflictKey})
				conflictWith = Lock{}
				conflictKey = nil
				rangeStartEncountered = false
				continue
			}

			mc.rollback()
			return conflictKey, conflictWith, nil
		}

		if rangeStartEncountered {
			key, keyLock, ok := l.mu.store.Seek(upperBounded)
			if !ok {
				panic("BUG, missing range end key")
			}
			start, end = l.mergeRangeLocked(
				start, end,
				prevStartKey,
				key, keyLock,
				mc,
				c.txn)
		}
		break
	}

	mc.commit(l.bind, c.txn, l.mu.store)
	startLock, endLock := newRangeLock(c)
	startLock.waiters = wq
	endLock.waiters = wq

	// similar to row lock
	c.txn.lockAdded(l.bind.Table, [][]byte{start, end})
	l.mu.store.Add(start, startLock)
	l.mu.store.Add(end, endLock)
	return nil, Lock{}, nil
}

func (l *localLockTable) mergeRangeLocked(
	start, end []byte,
	prevStartKey []byte,
	seekKey []byte,
	seekLock Lock,
	mc *mergeContext,
	txn *activeTxn) ([]byte, []byte) {
	// range lock encountered a row lock
	if seekLock.isLockRow() {
		// 5 + [1, 4] => [1, 4] + [5]
		if bytes.Compare(seekKey, end) > 0 {
			return start, end
		}

		// [1~4] + [1, 4] => [1, 4]
		mc.mergeLocks([][]byte{seekKey})
		mc.mergeWaiter(seekLock.waiters)
		return start, end
	}

	if len(prevStartKey) == 0 {
		prevStartKey = l.mustGetRangeStart(seekKey)
	}

	oldStart, oldEnd := prevStartKey, seekKey

	// no overlap
	if bytes.Compare(oldStart, end) > 0 ||
		bytes.Compare(start, oldEnd) > 0 {
		return start, end
	}

	min, max := oldStart, oldEnd
	if bytes.Compare(min, start) > 0 {
		min = start
	}
	if bytes.Compare(max, end) < 0 {
		max = end
	}

	mc.mergeLocks([][]byte{oldStart, oldEnd})
	mc.mergeWaiter(seekLock.waiters)
	return min, max
}

func (l *localLockTable) mustGetRangeStart(endKey []byte) []byte {
	v, _, ok := l.mu.store.Prev(endKey)
	if !ok {
		panic("missing start key")
	}
	return v
}

func nextKey(src, dst []byte) []byte {
	dst = append(dst, src...)
	dst = append(dst, 0)
	return dst
}

var (
	mergePool = sync.Pool{
		New: func() any {
			return &mergeContext{
				mergedLocks: make(map[string]struct{}, 8),
			}
		},
	}
)

type mergeContext struct {
	to            waiterQueue
	mergedWaiters []waiterQueue
	mergedLocks   map[string]struct{}
}

func newMergeContext(to waiterQueue) *mergeContext {
	c := mergePool.Get().(*mergeContext)
	c.to = to
	c.to.beginChange()
	return c
}

func (c *mergeContext) close() {
	for k := range c.mergedLocks {
		delete(c.mergedLocks, k)
	}
	c.to = nil
	c.mergedWaiters = c.mergedWaiters[:0]
	mergePool.Put(c)
}

func (c *mergeContext) mergeWaiter(from waiterQueue) {
	from.moveTo(c.to)
	c.mergedWaiters = append(c.mergedWaiters, from)
}

func (c *mergeContext) mergeLocks(locks [][]byte) {
	for _, v := range locks {
		c.mergedLocks[util.UnsafeBytesToString(v)] = struct{}{}
	}
}

func (c *mergeContext) commit(
	bind pb.LockTable,
	txn *activeTxn,
	s LockStorage) {
	for k := range c.mergedLocks {
		s.Delete(util.UnsafeStringToBytes(k))
	}
	txn.lockRemoved(
		bind.ServiceID,
		bind.Table,
		c.mergedLocks)

	for _, q := range c.mergedWaiters {
		q.reset()
	}
	c.to.commitChange()
}

func (c *mergeContext) rollback() {
	c.to.rollbackChange()
}
