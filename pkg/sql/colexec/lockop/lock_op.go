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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// Lock lock
func Lock(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
	lockService lockservice.LockService,
	vec *vector.Vector,
	pkType types.Type,
	opts LockOptions) (RetryTimestamp, error) {
	if !txnOp.Txn().IsPessimistic() {
		return RetryTimestamp{}, nil
	}

	if opts.maxBytesPerLock == 0 {
		opts.maxBytesPerLock = int(lockService.GetConfig().MaxLockRowBytes)
	}
	fetchFunc := opts.fetchFunc
	if fetchFunc == nil {
		fetchFunc = GetFetchRowsFunc(pkType)
	}

	rows, g := fetchFunc(
		vec,
		opts.parker,
		pkType,
		opts.maxBytesPerLock,
		opts.lockTable)
	result, err := lockService.Lock(
		ctx,
		tableID,
		rows,
		txnOp.Txn().ID,
		lock.LockOptions{
			Granularity: g,
			Policy:      lock.WaitPolicy_Wait,
			Mode:        opts.mode,
		})
	if err != nil {
		return RetryTimestamp{}, err
	}

	// add bind locks
	if err := txnOp.AddLockTable(result.LockedOn); err != nil {
		return RetryTimestamp{}, err
	}

	// no conflict or has conflict, but all prev txn all aborted
	// current txn can read and write normally
	if !result.HasConflict ||
		!result.HasPrevCommit {
		return RetryTimestamp{}, nil
	}

	// arriving here means that at least one of the conflicting transactions
	// has committed. Arriving here means that at least one of the conflicting
	// transactions has committed.
	//
	// For the RC schema we need some retries between
	// [txn.snapshotts, prev.committs] (de-duplication for insert, re-query for
	// update and delete).
	//
	// For the SI schema the current transaction needs to be abort (TODO: later
	// we can consider recording the ReadSet of the transaction and check if data
	// is modified between [snapshotTS,prev.commits] and raise the SnapshotTS of
	// the SI transaction to eliminate conflicts)
	if !txnOp.Txn().IsRCIsolation() {
		return RetryTimestamp{}, moerr.NewTxnWWConflict(ctx)
	}

	// forward rc's snapshot ts
	oldTS := txnOp.Txn().SnapshotTS
	if err := txnOp.UpdateSnapshot(result.Timestamp); err != nil {
		return RetryTimestamp{}, err
	}
	return RetryTimestamp{From: oldTS, To: result.Timestamp.Next()}, nil
}

// DefaultLockOptions create a default lock operation. The parker is used to
// encode primary key into lock row.
func DefaultLockOptions(parker *types.Packer) LockOptions {
	return LockOptions{
		mode:            lock.LockMode_Exclusive,
		lockTable:       false,
		maxBytesPerLock: 0,
		parker:          parker,
	}
}

// WithLockMode set lock mode, Exclusive or Shared
func (opts LockOptions) WithLockMode(mode lock.LockMode) LockOptions {
	opts.mode = mode
	return opts
}

// WithLockTable set lock all table
func (opts LockOptions) WithLockTable() LockOptions {
	opts.lockTable = true
	return opts
}

// WithMaxBytesPerLock every lock operation, will add some lock rows into
// lockservice. If very many rows of data are added at once, this can result
// in an excessive memory footprint. This value limits the amount of lock memory
// that can be allocated per lock operation, and if it is exceeded, it will be
// converted to a range lock.
func (opts LockOptions) WithMaxBytesPerLock() LockOptions {
	opts.lockTable = true
	return opts
}

// WithFetchLockRowsFunc set the primary key into lock rows conversion function.
func (opts LockOptions) WithFetchLockRowsFunc(fetchFunc FetchLockRowsFunc) LockOptions {
	opts.fetchFunc = fetchFunc
	return opts
}

// NeedRetry return true if need retry
func (v RetryTimestamp) NeedRetry() bool {
	return v.To.Greater(v.From)
}
