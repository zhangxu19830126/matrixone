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

package dn

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

func (s *store) registerRPCHandlers() {
	// request from CN node
	s.server.RegisterMethodHandler(txn.TxnMethod_Read, s.handleRead)
	s.server.RegisterMethodHandler(txn.TxnMethod_Write, s.handleWrite)
	s.server.RegisterMethodHandler(txn.TxnMethod_Commit, s.handleCommit)
	s.server.RegisterMethodHandler(txn.TxnMethod_Rollback, s.handleRollback)

	// request from other DN node
	s.server.RegisterMethodHandler(txn.TxnMethod_Prepare, s.handlePrepare)
	s.server.RegisterMethodHandler(txn.TxnMethod_CommitDNShard, s.handleCommitDNShard)
	s.server.RegisterMethodHandler(txn.TxnMethod_RollbackDNShard, s.handleRollbackDNShard)
	s.server.RegisterMethodHandler(txn.TxnMethod_GetStatus, s.handleGetStatus)
}

func (s *store) dispatchLocalRequest(shard metadata.DNShard) rpc.TxnRequestHandleFunc {
	return nil
}

func (s *store) handleRead(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *store) handleWrite(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *store) handleCommit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *store) handleRollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *store) handlePrepare(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *store) handleCommitDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *store) handleRollbackDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *store) handleGetStatus(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
