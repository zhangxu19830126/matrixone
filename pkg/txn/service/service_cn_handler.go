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

package service

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

func (s *service) Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	s.validDNShard(request.GetTargetDN())

	// We do not write transaction information to sync.Map during read operations because commit and abort
	// for read-only transactions are not sent to the DN node, so there is no way to clean up the transaction
	// information in sync.Map.

	result, err := s.storage.Read(request.Txn, request.CNRequest.OpCode, request.CNRequest.Payload)
	if err != nil {
		s.logger.Error("execute read failed",
			util.TxnIDFieldWithID(request.Txn.ID),
			zap.Error(err))
		response.TxnError = newTAEReadError(err)
		return nil
	}
	defer result.Release()

	if len(result.WaitTxns()) > 0 {
		waiters := make([]*waiter, 0, len(result.WaitTxns()))
		for _, txnID := range result.WaitTxns() {
			txnCtx := s.getTxnContext(txnID)
			// The transaction can not found, it means the concurrent transaction to be waited for has already
			// been committed or aborted.
			if txnCtx == nil {
				continue
			}

			w := acquireWaiter()
			// txn has been committed or aborted between call s.getTxnContext and txnCtx.addWaiter
			if !txnCtx.addWaiter(txnID, w, txn.TxnStatus_Committed) {
				w.close()
				continue
			}

			waiters = append(waiters, w)
		}

		for _, w := range waiters {
			if err != nil {
				w.close()
				continue
			}

			// If no error occurs, then it must have waited until the final state of the transaction, not caring
			// whether the final state is committed or aborted.
			_, err = w.wait(ctx)
			w.close()
		}

		if err != nil {
			s.logger.Error("wait txns failed",
				util.TxnIDFieldWithID(request.Txn.ID),
				zap.Error(err))
			response.TxnError = newWaitTxnError(err)
			return nil
		}
	}

	data, err := result.Read()
	if err != nil {
		s.logger.Error("read failed",
			zap.Error(err))
		response.TxnError = newTAEReadError(err)
		return nil
	}

	response.CNOpResponse.Payload = data
	return nil
}

func (s *service) Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	s.validDNShard(request.GetTargetDN())

	txnID := request.Txn.ID
	txnCtx, _ := s.maybeAddTxn(request.Txn)

	// only commit and rollback can held write Lock
	if !txnCtx.mu.TryRLock() {
		response.TxnError = newTxnNotFoundError()
		return nil
	}
	defer txnCtx.mu.RUnlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		response.TxnError = newTxnNotActiveError()
		return nil
	}

	data, err := s.storage.Write(request.Txn, request.CNRequest.OpCode, request.CNRequest.Payload)
	if err != nil {
		response.TxnError = newTAEWriteError(err)
		return nil
	}

	response.CNOpResponse.Payload = data
	return nil
}

func (s *service) Commit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	response.CommitResponse = &txn.TxnCommitResponse{}
	s.validDNShard(request.GetTargetDN())
	if len(request.Txn.DNShards) == 0 {
		s.logger.Fatal("commit with empty dn shards")
	}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	// block all other concurrent read and write operations.
	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	completed := true
	defer func() {
		s.removeTxn(txnID)
		if completed {
			s.releaseTxnContext(txnCtx)
		}
	}()

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		response.TxnError = newTxnNotActiveError()
		return nil
	}

	newTxn.DNShards = request.Txn.DNShards
	changeStatus := func(status txn.TxnStatus) {
		newTxn.Status = status
		txnCtx.changeStatusLocked(status)
	}

	// fast path: write in only one DNShard.
	if len(newTxn.DNShards) == 1 {
		commitTS, _ := s.clocker.Now()
		txnCtx.updateCommitTimestampLocked(commitTS)

		if err := s.storage.Commit(txnCtx.mu.txn); err != nil {
			s.logger.Error("commit failed",
				util.TxnIDFieldWithID(txnID),
				zap.Error(err))
			response.TxnError = newTAECommitError(err)
			changeStatus(txn.TxnStatus_Aborted)
		} else {
			changeStatus(txn.TxnStatus_Committed)
		}

		return nil
	}

	// slow path. 2pc transaction.
	// 1. send prepare request to all DNShards.
	// 2. start async commit task if all prepare succeed.
	// 3. response to client txn committed.
	for _, dn := range newTxn.DNShards {
		txnCtx.mu.requests = append(txnCtx.mu.requests, txn.TxnRequest{
			Txn:            newTxn,
			Method:         txn.TxnMethod_Prepare,
			TimeoutAt:      s.mustGetTimeoutAtFromContext(ctx),
			PrepareRequest: &txn.TxnPrepareRequest{DNShard: dn},
		})
	}

	// unlock and lock here, because the prepare request will be sent to the current TxnService, it
	// will need to get the Lock when processing the Prepare.
	txnCtx.mu.Unlock()
	responses, err := s.sender.Send(ctx, txnCtx.mu.requests)
	txnCtx.mu.Lock()
	if err != nil {
		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = newRPCError(err)
		s.startAsyncRollbackTask(newTxn)
		return nil
	}

	// get latest txn metadata
	newTxn = txnCtx.getTxnLocked()
	if newTxn.PreparedTS.IsEmpty() {
		s.logger.Fatal("missing prepared timestamp",
			util.TxnIDFieldWithID(newTxn.ID))
	}
	newTxn.CommitTS = newTxn.PreparedTS

	hasError := false
	for idx, resp := range responses {
		if resp.TxnError != nil {
			hasError = true
			s.logger.Error("prepare dn failed",
				util.TxnIDFieldWithID(txnID),
				zap.String("target-dn-shard", newTxn.DNShards[idx].DebugString()),
				zap.String("error", resp.TxnError.DebugString()))
			continue
		}

		if resp.Txn.PreparedTS.IsEmpty() {
			s.logger.Fatal("missing prepared timestamp",
				zap.String("target-dn-shard", newTxn.DNShards[idx].DebugString()),
				util.TxnIDFieldWithID(newTxn.ID))
		}

		if newTxn.CommitTS.Less(resp.Txn.PreparedTS) {
			newTxn.CommitTS = resp.Txn.PreparedTS
		}
	}
	if hasError {
		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = newRPCError(err)
		s.startAsyncRollbackTask(newTxn)
		return nil
	}

	// All DNShards prepared means the transaction is committed
	completed = false
	txnCtx.updateTxnLocked(newTxn)
	s.startAsyncCommitTask(txnCtx)
	return nil
}

func (s *service) Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	response.RollbackResponse = &txn.TxnRollbackResponse{}
	s.validDNShard(request.GetTargetDN())
	if len(request.Txn.DNShards) == 0 {
		s.logger.Fatal("rollback with empty dn shards")
	}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	defer func() {
		s.removeTxn(txnID)
		s.releaseTxnContext(txnCtx)
	}()

	response.Txn = &newTxn
	newTxn.DNShards = request.Txn.DNShards

	s.startAsyncRollbackTask(newTxn)
	newTxn.Status = txn.TxnStatus_Aborted
	txnCtx.changeStatusLocked(txn.TxnStatus_Aborted)
	return nil
}

func (s *service) startAsyncRollbackTask(txnMeta txn.TxnMeta) {
	s.stopper.RunTask(func(ctx context.Context) {
		requests := make([]txn.TxnRequest, 0, len(txnMeta.DNShards))
		for _, dn := range txnMeta.DNShards {
			requests = append(requests, txn.TxnRequest{
				Txn:            txnMeta,
				Method:         txn.TxnMethod_Prepare,
				PrepareRequest: &txn.TxnPrepareRequest{DNShard: dn},
			})
		}

		s.parallelSendWithRetry(ctx, "rollback txn", txnMeta, requests)
	})
}

func (s *service) startAsyncCommitTask(txnCtx *txnContext) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		txnCtx.mu.Lock()
		defer txnCtx.mu.Unlock()

		if txnCtx.mu.txn.Status != txn.TxnStatus_Committing {
			for {
				err := s.storage.Committing(txnCtx.mu.txn)
				if err == nil {
					txnCtx.changeStatusLocked(txn.TxnStatus_Committing)
					break
				}
				s.logger.Error("save committing txn failed, retry later",
					util.TxnIDFieldWithID(txnCtx.mu.txn.ID),
					zap.Error(err))
				// TODO: make config
				time.Sleep(time.Second)
			}
		}

		requests := make([]txn.TxnRequest, 0, len(txnCtx.mu.txn.DNShards))
		for _, dn := range txnCtx.mu.txn.DNShards {
			requests = append(requests, txn.TxnRequest{
				Txn:                  txnCtx.mu.txn,
				Method:               txn.TxnMethod_CommitDNShard,
				CommitDNShardRequest: &txn.TxnCommitDNShardRequest{DNShard: dn},
			})
		}

		// not timeout, keep retry until TxnService.Close
		ctx, cancel := context.WithTimeout(ctx, time.Duration(math.MaxInt64))
		defer cancel()

		if len(s.parallelSendWithRetry(ctx, "commit txn", txnCtx.mu.txn, requests[1:])) > 0 {
			if ce := s.logger.Check(zap.DebugLevel, "other dnshards committed"); ce != nil {
				ce.Write(util.TxnIDFieldWithID(txnCtx.mu.txn.ID))
			}
		}

		if len(s.parallelSendWithRetry(ctx, "commit txn", txnCtx.mu.txn, requests[:1])) > 0 {
			if ce := s.logger.Check(zap.DebugLevel, "coordinator dnshard committed, txn committed"); ce != nil {
				ce.Write(util.TxnIDFieldWithID(txnCtx.mu.txn.ID))
			}
		}
	})
}

func (s *service) checkCNRequest(request *txn.TxnRequest) {
	if request.CNRequest == nil {
		s.logger.Fatal("missing CNRequest")
	}
}
