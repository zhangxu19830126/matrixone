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
// See the License for the s

package service

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// TxnService is a transaction service that runs on the DNStore and is used to receive transaction requests
// from the CN. In the case of a 2 pc distributed transaction, it acts as a transaction coordinator to handle
// distributed transactions.
// The txn service use Clock-SI to implement transaction.
type TxnService interface {
	// Metadata returns the metadata of DNShard
	Metadata() metadata.DNShard
	// Close close the txn service
	Close() error

	// Read handle txn read request from CN. For reuse, the response is provided by the caller
	Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// Write handle txn write request from CN. For reuse, the response is provided by the caller
	Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// Commit handle txn commit request from CN. For reuse, the response is provided by the caller
	Commit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// Rollback handle txn rollback request from CN. For reuse, the response is provided by the caller
	Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error

	// Rollback handle txn rollback request from coordinator DN. For reuse, the response is provided by the caller
	Prepare(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// GetStatus handle get txn status in current DNShard request from coordinator DN. For reuse, the response
	// is provided by the caller
	GetStatus(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// CommitDNShard handle commit txn data in current DNShard request from coordinator DN. For reuse, the response
	// is provided by the caller
	CommitDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// RollbackDNShard handle rollback txn data in current DNShard request from coordinator DN. For reuse, the response
	// is provided by the caller
	RollbackDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
}

// TxnStorage txn storage
type TxnStorage interface {
	// Read execute read requests sent by CN.
	//
	// The Payload parameter is unsafe and should no longer be held by Storage
	// after the return of the current call.
	//
	// If any of the data in the current read has been written by other transactions,
	// these write transaction IDs need to be returned. The transaction IDs that need to be returned include
	// the following:
	// case1. Txn.Status == Committing && CurrentTxn.SnapshotTimestamp > Txn.CommitTimestamp
	// case2. Txn.Status == Prepared && CurrentTxn.SnapshotTimestamp > Txn.PreparedTimestamp
	Read(txnMeta txn.TxnMeta, op int, payload []byte) (ReadResult, error)
	// Write execute write requests sent by CN.
	Write(txnMeta txn.TxnMeta, op int, payload []byte) error
	// Commit commit the transaction. Only the transaction commit of a single DNShard will call.
	Commit(txnMeta txn.TxnMeta) error
	// Rollback rollback the transaction. Only the transaction commit of a single DNShard will call.
	Rollback(txnMeta txn.TxnMeta) error

	// Prepare prepare data written by a transaction on a DNShard. TxnStorage needs to do conflict
	// detection locally. The txn metadata(status change to prepared) and the data should be written to
	// LogService.
	Prepare(txnMeta txn.TxnMeta) error
	// CommitPrepared commit the prepared data.
	CommitPrepared(txnMeta txn.TxnMeta) error
	// RollbackPrepared rollback the prepared data.
	RollbackPrepared(txnMeta txn.TxnMeta) error
}

// ReadResult read result from TxnStorage. When a read operation encounters any concurrent write transaction,
// it is necessary to wait for the write transaction to complete to confirm that the latest write is visible
// to the current transaction.
//
// To avoid the read Payload being parsed multiple times, TxnStorage can store the parsed state in the ReadResult
// and continue to use it while the ReadResult continues.
type ReadResult interface {
	// WaitTxns returns the ID of the concurrent write transaction encountered.
	WaitTxns() [][]byte
	// GetResponse returns the response data.
	GetResponse() ([]byte, error)
	// Release release the ReadResult. TxnStorage can resuse the response data and the ReadResult.
	Release()
}
