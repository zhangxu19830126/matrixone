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
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	tableID  uint64
	detector *detector

	mu struct {
		sync.RWMutex
		closed bool
		store  LockStorage
	}
}

func newLocalLockTable(
	tableID uint64,
	detector *detector) lockTable {
	l := &localLockTable{tableID: tableID, detector: detector}
	l.mu.store = newBtreeBasedStorage()
	return l
}

func (l *localLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	options LockOptions) error {
	waiter := acquireWaiter(txn.txnID)
	for {
		added, err := l.doAcquireLock(txn, waiter, rows, options)
		if err != nil {
			return err
		}
		if added {
			return nil
		}

		if err := waiter.wait(ctx); err != nil {
			return err
		}
		waiter.resetWait()
	}
}

func (l *localLockTable) unlock(
	ctx context.Context,
	txn *activeTxn,
	ls *cowSlice) error {
	locks := ls.slice()
	defer locks.unref()

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.closed {
		return moerr.NewInvalidStateNoCtx("local lock table closed")
	}

	locks.iter(func(key []byte) bool {
		if lock, ok := l.mu.store.Get(key); ok {
			if lock.isLockRow() || lock.isLockRangeEnd() {
				lock.waiter.close()
			}
			l.mu.store.Delete(key)
		}
		return true
	})
	return nil
}

func (l *localLockTable) getLock(ctx context.Context, key []byte) (Lock, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.mu.closed {
		return Lock{}, false
	}
	return l.mu.store.Get(key)
}

func (l *localLockTable) getBind() pb.LockTable {
	// TODO: implement
	return pb.LockTable{}
}

func (l *localLockTable) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.closed = true
}

func (l *localLockTable) doAcquireLock(
	txn *activeTxn,
	waiter *waiter,
	rows [][]byte,
	opts LockOptions) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.mu.closed {
		return false, moerr.NewInvalidStateNoCtx("local lock table closed")
	}

	switch opts.Granularity {
	case pb.Granularity_Row:
		return l.acquireRowLockLocked(txn, waiter, rows[0], opts.Mode), nil
	case pb.Granularity_Range:
		if len(rows) != 2 {
			panic("invalid range lock")
		}
		return l.acquireRangeLockLocked(txn, waiter, rows[0], rows[1], opts.Mode), nil
	default:
		panic(fmt.Sprintf("not support lock granularity %d", opts))
	}
}

func (l *localLockTable) acquireRowLockLocked(
	txn *activeTxn,
	w *waiter,
	row []byte,
	mode pb.LockMode) bool {
	key, lock, ok := l.mu.store.Seek(row)
	if ok &&
		(bytes.Equal(key, row) ||
			lock.isLockRangeEnd()) {
		l.handleLockConflict(txn, w, lock)
		return false
	}
	l.addRowLockLocked(txn, row, w, mode)
	return true
}

func (l *localLockTable) acquireRangeLockLocked(
	txn *activeTxn,
	w *waiter,
	start, end []byte,
	mode pb.LockMode) bool {
	if bytes.Compare(start, end) >= 0 {
		panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
			start, end))
	}
	key, lock, ok := l.mu.store.Seek(start)
	if ok &&
		bytes.Compare(key, end) <= 0 {
		l.handleLockConflict(txn, w, lock)
		return false
	}

	l.addRangeLockLocked(txn, start, end, w, mode)
	return true
}

func (l *localLockTable) addRowLockLocked(
	txn *activeTxn,
	row []byte,
	waiter *waiter,
	mode pb.LockMode) {
	lock := newRowLock(txn.txnID, mode)
	lock.waiter = waiter

	// we must first add the lock to txn to ensure that the
	// lock can be read when the deadlock is detected.
	txn.lockAdded(l.tableID, [][]byte{row})
	l.mu.store.Add(row, lock)
}

func (l *localLockTable) addRangeLockLocked(
	txn *activeTxn,
	start, end []byte,
	waiter *waiter,
	mode pb.LockMode) {
	startLock, endLock := newRangeLock(txn.txnID, mode)
	startLock.waiter = waiter
	endLock.waiter = waiter

	// we must first add the lock to txn to ensure that the
	// lock can be read when the deadlock is detected.
	txn.lockAdded(l.tableID, [][]byte{start, end})
	l.mu.store.Add(start, startLock)
	l.mu.store.Add(end, endLock)
}

func (l *localLockTable) handleLockConflict(txn *activeTxn, w *waiter, conflictWith Lock) {
	// find conflict, and wait prev txn completed, and a new
	// waiter added, we need to active deadlock check.
	txn.setBlocked(w)
	conflictWith.waiter.add(w)
	if err := l.detector.check(txn.txnID); err != nil {
		panic("BUG: active dead lock check can not fail")
	}
}
