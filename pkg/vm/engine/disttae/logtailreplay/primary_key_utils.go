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
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func (p *PartitionState) PrimaryKeyMayBeModified(
	from types.TS,
	to types.TS,
	key []byte,
) (string, bool) {

	p.shared.Lock()
	lastFlushTimestamp := p.shared.lastFlushTimestamp
	p.shared.Unlock()

	if !lastFlushTimestamp.IsEmpty() {
		if from.LessEq(lastFlushTimestamp) {
			return true
		}
	} else {
		changed = false
	}
	if changed {
		return "flushed", true
	}
	tree := p.primaryIndex.Copy()
	iter := tree.Iter()
	defer iter.Release()

	for ok := iter.Seek(&PrimaryIndexEntry{
		Bytes: key,
	}); ok; ok = iter.Next() {

		entry := iter.Item()

		if !bytes.Equal(entry.Bytes, key) {
			break
		}

		hasData = true
		if entry.Time.GreaterEq(from) {
			return "mem changed", true
		}

		// some legacy deletion entries may not indexed, check all rows for changes
		pivot := RowEntry{
			BlockID: entry.BlockID,
			RowID:   entry.RowID,
			Time:    types.BuildTS(math.MaxInt64, math.MaxUint32),
		}
		iter := p.rows.Copy().Iter()
		seek := false
		for {
			if !seek {
				seek = true
				if !iter.Seek(pivot) {
					break
				}
			} else {
				if !iter.Next() {
					break
				}
			}
			row := iter.Item()
			if row.BlockID.Compare(entry.BlockID) != 0 {
				break
			}
			if !row.RowID.Equal(entry.RowID) {
				break
			}
			if row.Time.GreaterEq(from) {
				iter.Release()
				return "mem2 changed", true
			}
		}
		iter.Release()

	}

	if hasData {
		return "mem not changed", false
	}
	return "no data in mem", false
}
