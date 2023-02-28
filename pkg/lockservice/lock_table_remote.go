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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

// remoteLockTable the lock corresponding to the Table is managed by a remote LockTable. And the
// remoteLockTable acts as a proxy for this LockTable locally.
type remoteLockTable struct {
	binding  pb.LockTable
	client   Client
	detector *detector
}

func newRemoteLockTable(
	binding pb.LockTable,
	client Client,
	detector *detector) *remoteLockTable {
	l := &remoteLockTable{
		binding:  binding,
		detector: detector,
	}
	return l
}

func (l *remoteLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	options LockOptions) error {
	req := acquireRequest()
	defer releaseRequest(req)

	req.ServiceID = l.binding.ServiceID
	req.Method = pb.Method_Lock
	req.Lock.Options = options
	req.Lock.LockTable = l.binding
	req.Lock.TxnID = txn.txnID
	req.Lock.Rows = rows

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		releaseResponse(resp)
		return nil
	}

	// handle errors
	return err
}

func (l *remoteLockTable) unlock(
	ctx context.Context,
	txn *activeTxn,
	ls *cowSlice) error {
	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_Unlock
	req.ServiceID = l.binding.ServiceID
	req.Unlock.TxnID = txn.txnID

	// we need retry unlock remote lock if encounter an error util context timeout.
	return l.doSendRetryUntilContextTimeout(ctx, req)
}

func (l *remoteLockTable) doSendRetryUntilContextTimeout(
	ctx context.Context,
	req *pb.Request) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			resp, err := l.client.Send(ctx, req)
			releaseResponse(resp)
			if err == nil {
				return nil
			}
		}
	}
}
