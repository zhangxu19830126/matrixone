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

package logtailreplay

import (
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (p *PartitionState) PrimaryKeysMayBeModified(
	txnID []byte,
	from types.TS, // snapshot ts
	to types.TS, // locked ts
	keysVector *vector.Vector,
	packer *types.Packer,
) bool {
	packer.Reset()

	keys := EncodePrimaryKeyVector(keysVector, packer)
	for _, key := range keys {
		if p.PrimaryKeyMayBeModified(txnID, from, to, key) {
			return true
		}
	}

	return false
}

func (p *PartitionState) PrimaryKeyMayBeModified(
	txnID []byte,
	from types.TS,
	to types.TS,
	key []byte,
) bool {
	iter2 := p.NewRowsIter(
		to,
		nil,
		false,
	)
	for iter2.Next() {
		row := iter2.Entry()
		fmt.Printf("txn %s batch: %+v\n", hex.EncodeToString(txnID), row.Batch)
	}

	iter := p.NewPrimaryKeyIter(to, Exact(key))
	defer iter.Close()

	empty := true
	fmt.Printf("txn %s try to check changed, from %+v, found ts %+v", hex.EncodeToString(txnID), from.ToTimestamp(), key)
	for iter.Next() {
		empty = false
		row := iter.Entry()
		if row.Time.Greater(from) {
			fmt.Printf("txn %s found changed, from %+v, found ts %+v for %+v", hex.EncodeToString(txnID), from.ToTimestamp(), row.Time, key)
			return true
		}
	}

	return empty
}
