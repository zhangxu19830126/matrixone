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
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

func (s *service) startRecovery() {
	s.stopper.RunTask(s.doRecovery)
	s.storage.StartRecovery(s.txnC)
	s.waitRecoveryCompleted()
}

func (s *service) doRecovery(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case txn, ok := <-s.txnC:
			if !ok {
				s.end()
				return
			}
			s.addLog(txn)
		}
	}
}

func (s *service) addLog(txnMeta txn.TxnMeta) error {
	if len(txnMeta.DNShards) <= 1 {
		return nil
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Committing:
		s.checkRecoveryStatus(txnMeta)
		txnCtx := s.getTxnContext(txnMeta.ID)
		if txnCtx == nil {
			s.maybeAddTxn(txnMeta)
		} else {
			if txnCtx.getTxn().Status != txn.TxnStatus_Prepared ||
				txnCtx.getTxn().Status != txn.TxnStatus_Committing {
				s.logger.Fatal("invalid txn status before committing",
					zap.String("prev-status", txnCtx.getTxn().Status.String()),
					util.TxnField(txnMeta))
			}
			txnCtx.updateTxn(txnMeta)
		}
	case txn.TxnStatus_Prepared:
		s.checkRecoveryStatus(txnMeta)
		txnCtx := s.getTxnContext(txnMeta.ID)
		if txnCtx == nil {
			s.maybeAddTxn(txnMeta)
			break
		}

		if txnCtx.getTxn().Status != txn.TxnStatus_Prepared {
			s.logger.Fatal("invalid txn status before prepare status",
				zap.String("prev-status", txnCtx.getTxn().Status.String()),
				util.TxnField(txnMeta))
		}
		txnCtx.updateTxn(txnMeta)
	case txn.TxnStatus_Committed:
		s.checkRecoveryStatus(txnMeta)
		s.removeTxn(txnMeta.ID)
	default:
		s.logger.Fatal("invalid recovery status",
			util.TxnField(txnMeta))
	}
	return nil
}

func (s *service) end() error {
	defer close(s.recoveryC)
	s.transactions.Range(func(key, value any) bool {
		txnCtx := value.(*txnContext)
		txnMeta := txnCtx.getTxn()
		if !s.shard.Equal(txnMeta.DNShards[0]) {
			return true
		}

		switch txnMeta.Status {
		case txn.TxnStatus_Prepared:
			s.removeTxn(txnMeta.ID)
			if err := s.startAsyncCheckCommitTask(txnCtx); err != nil {
				panic(err)
			}
		case txn.TxnStatus_Committing:
			s.removeTxn(txnMeta.ID)
			if err := s.startAsyncCommitTask(txnCtx); err != nil {
				panic(err)
			}
		}
		return true
	})

	return nil
}

func (s *service) waitRecoveryCompleted() {
	<-s.recoveryC
}

func (s *service) startAsyncCheckCommitTask(txnCtx *txnContext) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		txnMeta := txnCtx.getTxn()
		s.removeTxn(txnMeta.ID)

		requests := make([]txn.TxnRequest, 0, len(txnMeta.DNShards)-1)
		for _, dn := range txnMeta.DNShards[1:] {
			requests = append(requests, txn.TxnRequest{
				Txn:              txnMeta,
				Method:           txn.TxnMethod_GetStatus,
				GetStatusRequest: &txn.TxnGetStatusRequest{DNShard: dn},
			})
		}

		responses := s.parallelSendWithRetry(ctx, "get txn status", txnMeta, requests, prepareIngoreErrorCodes)
		if len(responses) == 0 {
			return
		}

		prepared := 1
		txnMeta.CommitTS = txnMeta.PreparedTS
		for _, resp := range responses {
			if resp.Txn != nil && resp.Txn.Status == txn.TxnStatus_Prepared {
				prepared++
				if txnMeta.CommitTS.Less(resp.Txn.PreparedTS) {
					txnMeta.PreparedTS = resp.Txn.PreparedTS
				}
			}
		}

		txnCtx.mu.Lock()
		defer txnCtx.mu.Unlock()

		if prepared == len(txnMeta.DNShards) {
			txnCtx.updateTxnLocked(txnMeta)
			if err := s.startAsyncCommitTask(txnCtx); err != nil {
				s.logger.Error("start commit task failed",
					zap.Error(err),
					util.TxnField(txnMeta))
			}
		} else {
			txnCtx.changeStatusLocked(txn.TxnStatus_Aborted)
			s.releaseTxnContext(txnCtx)
			s.startAsyncRollbackTask(txnMeta)
		}
	})
}

func (s *service) checkRecoveryStatus(txnMeta txn.TxnMeta) {
	if txnMeta.PreparedTS.IsEmpty() ||
		(txnMeta.Status != txn.TxnStatus_Prepared &&
			txnMeta.CommitTS.IsEmpty()) {
		s.logger.Fatal("invalid preparedTS or commitTS",
			util.TxnField(txnMeta))
	}

	if txnMeta.Status == txn.TxnStatus_Committing {
		s.validDNShard(txnMeta.DNShards[0])
	}
}
