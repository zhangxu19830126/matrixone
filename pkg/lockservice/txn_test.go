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
	"context"
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestLockAdded(t *testing.T) {
	id := []byte("t1")
	fsp := NewFixedSlicePool(2)
	txn := newActiveTxn(id, string(id), fsp, "")

	txn.lockAdded(1, [][]byte{[]byte("k1")}, false)
	txn.lockAdded(1, [][]byte{[]byte("k11")}, false)
	txn.lockAdded(2, [][]byte{[]byte("k2"), []byte("k22")}, false)

	assert.Equal(t, 2, len(txn.holdLocks))

	sp := txn.holdLocks[1]
	s := sp.slice()
	defer s.unref()
	assert.Equal(t, 2, s.Len())

	sp2 := txn.holdLocks[2]
	s2 := sp2.slice()
	defer s2.unref()
	assert.Equal(t, 2, s2.Len())
}

func TestClose(t *testing.T) {
	id := []byte("t1")
	fsp := NewFixedSlicePool(2)
	txn := newActiveTxn(id, string(id), fsp, "")
	tables := map[uint64]lockTable{
		1: newLocalLockTable(pb.LockTable{Table: 1}, nil),
		2: newLocalLockTable(pb.LockTable{Table: 2}, nil),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	assert.NoError(t, tables[1].lock(ctx, txn, [][]byte{[]byte("k1")}, LockOptions{}))
	assert.NoError(t, tables[2].lock(ctx, txn, [][]byte{[]byte("k2")}, LockOptions{}))

	txn.close(
		txn.txnID,
		func(table uint64) (lockTable, error) {
			return tables[table], nil
		})
	assert.Empty(t, txn.txnID)
	assert.Empty(t, txn.txnKey)
	assert.Nil(t, txn.blockedWaiter)
	assert.Empty(t, txn.holdLocks)
	assert.Equal(t, 0, tables[1].(*localLockTable).mu.store.Len())
	assert.Equal(t, 0, tables[2].(*localLockTable).mu.store.Len())
}
