// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (txn *Transaction) getBlockInfos(
	ctx context.Context,
	tbl *txnTable,
) (blocks []catalog.BlockInfo, err error) {
	ts := types.TimestampToTS(txn.meta.SnapshotTS)
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	var objectName objectio.ObjectNameShort
	iter := state.NewBlocksIter(ts)
	for iter.Next() {
		entry := iter.Entry()
		location := entry.MetaLocation()
		if !objectio.IsSameObjectLocVsShort(location, &objectName) {
			// Prefetch object meta
			if err = blockio.PrefetchMeta(txn.proc.FileService, location); err != nil {
				iter.Close()
				return
			}
			objectName = *location.Name().Short()
		}
		blocks = append(blocks, entry.BlockInfo)
	}
	iter.Close()
	return
}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly.Load()
}

// WriteBatch used to write data to the transaction buffer
// insert/delete/update all use this api
// insertBatchHasRowId : it denotes the batch has Rowid when the typ is INSERT.
// if typ is not INSERT, it is always false.
// truncate : it denotes the batch with typ DELETE on mo_tables is generated when Truncating
// a table.
func (txn *Transaction) WriteBatch(
	typ int,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	dnStore DNStore,
	primaryIdx int, // pass -1 to indicate no primary key or disable primary key checking
	insertBatchHasRowId bool,
	truncate bool) error {
	txn.readOnly.Store(false)
	bat.Cnt = 1
	txn.Lock()
	defer txn.Unlock()
	if typ == INSERT {
		if !insertBatchHasRowId {
			txn.genBlock()
			len := bat.Length()
			vec := vector.NewVec(types.T_Rowid.ToType())
			for i := 0; i < len; i++ {
				if err := vector.AppendFixed(vec, txn.genRowId(), false,
					txn.proc.Mp()); err != nil {
					return err
				}
			}
			bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
			bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
		}
		// for TestPrimaryKeyCheck
		if txn.blockId_raw_batch != nil {
			txn.blockId_raw_batch[*txn.getCurrentBlockId()] = bat
		}
		txn.workspaceSize += uint64(bat.Size())
	}
	txn.writes = append(txn.writes, Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		dnStore:      dnStore,
		truncate:     truncate,
	})
	return nil
}

func (txn *Transaction) DumpBatch(force bool, offset int) error {
	var size uint64
	txn.Lock()
	defer txn.Unlock()
	if !(offset > 0 || txn.workspaceSize >= colexec.WriteS3Threshold ||
		(force && txn.workspaceSize >= colexec.TagS3Size)) {
		return nil
	}
	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].bat == nil {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			size += uint64(txn.writes[i].bat.Size())
		}
	}
	if offset > 0 && size < txn.workspaceSize {
		return nil
	}
	mp := make(map[[2]string][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		// TODO: after shrink, we should update workspace size
		if txn.writes[i].bat == nil || txn.writes[i].bat.Length() == 0 {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			key := [2]string{txn.writes[i].databaseName, txn.writes[i].tableName}
			bat := txn.writes[i].bat
			// skip rowid
			bat.Attrs = bat.Attrs[1:]
			bat.Vecs = bat.Vecs[1:]
			mp[key] = append(mp[key], bat)
			// DON'T MODIFY THE IDX OF AN ENTRY IN LOG
			// THIS IS VERY IMPORTANT FOR CN BLOCK COMPACTION
			// maybe this will cause that the log imcrements unlimitly
			// txn.writes = append(txn.writes[:i], txn.writes[i+1:]...)
			// i--
			txn.writes[i].bat = nil
		}
	}

	for key := range mp {
		s3Writer, tbl, err := txn.getS3Writer(key)
		if err != nil {
			return err
		}
		s3Writer.InitBuffers(txn.proc, mp[key][0])
		for i := 0; i < len(mp[key]); i++ {
			s3Writer.Put(mp[key][i], txn.proc)
		}
		err = s3Writer.SortAndFlush(txn.proc)

		if err != nil {
			return err
		}
		metaLoc := s3Writer.GetMetaLocBat()

		lenVecs := len(metaLoc.Attrs)
		// only remain the metaLoc col
		metaLoc.Vecs = metaLoc.Vecs[lenVecs-1:]
		metaLoc.Attrs = metaLoc.Attrs[lenVecs-1:]
		metaLoc.SetZs(metaLoc.Vecs[0].Length(), txn.proc.GetMPool())
		err = tbl.Write(txn.proc.Ctx, metaLoc)
		if err != nil {
			return err
		}
		// free batches
		for _, bat := range mp[key] {
			bat.Clean(txn.proc.GetMPool())
		}
	}
	txn.workspaceSize -= size

	return nil
}

// func (txn *Transaction) dumpWrites(offset int) (size uint64, mp map[[2]string][]*batch.Batch) {
// 	txn.Lock()
// 	defer txn.Unlock()

// 	for i := offset; i < len(txn.writes); i++ {
// 		if txn.writes[i].bat == nil {
// 			continue
// 		}
// 		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
// 			size += uint64(txn.writes[i].bat.Size())
// 		}
// 	}
// 	if offset > 0 && size < txn.workspaceSize {
// 		return 0, nil
// 	}

// 	mp = make(map[[2]string][]*batch.Batch)
// 	for i := offset; i < len(txn.writes); i++ {
// 		// TODO: after shrink, we should update workspace size
// 		if txn.writes[i].bat == nil || txn.writes[i].bat.Length() == 0 {
// 			continue
// 		}
// 		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
// 			key := [2]string{txn.writes[i].databaseName, txn.writes[i].tableName}
// 			bat := txn.writes[i].bat
// 			// skip rowid
// 			bat.Attrs = bat.Attrs[1:]
// 			bat.Vecs = bat.Vecs[1:]
// 			mp[key] = append(mp[key], bat)
// 			// DON'T MODIFY THE IDX OF AN ENTRY IN LOG
// 			// THIS IS VERY IMPORTANT FOR CN BLOCK COMPACTION
// 			// maybe this will cause that the log imcrements unlimitly
// 			// txn.writes = append(txn.writes[:i], txn.writes[i+1:]...)
// 			// i--
// 			txn.writes[i].bat = nil
// 		}
// 	}

// 	return
// }

func (txn *Transaction) getS3Writer(key [2]string) (*colexec.S3Writer, engine.Relation, error) {
	sortIdx, attrs, tbl, err := txn.getSortIdx(key)
	if err != nil {
		return nil, nil, err
	}
	s3Writer := &colexec.S3Writer{}
	s3Writer.SetSortIdx(-1)
	s3Writer.Init(txn.proc)
	s3Writer.SetMp(attrs)
	if sortIdx != -1 {
		s3Writer.SetSortIdx(sortIdx)
	}
	return s3Writer, tbl, nil
}

func (txn *Transaction) getSortIdx(key [2]string) (int, []*engine.Attribute, engine.Relation, error) {
	databaseName := key[0]
	tableName := key[1]

	database, err := txn.engine.Database(txn.proc.Ctx, databaseName, txn.proc.TxnOperator)
	if err != nil {
		return -1, nil, nil, err
	}
	tbl, err := database.Relation(txn.proc.Ctx, tableName)
	if err != nil {
		return -1, nil, nil, err
	}
	attrs, err := tbl.TableColumns(txn.proc.Ctx)
	if err != nil {
		return -1, nil, nil, err
	}
	for i := 0; i < len(attrs); i++ {
		if attrs[i].ClusterBy ||
			(attrs[i].Primary && !attrs[i].IsHidden) {
			return i, attrs, tbl, err
		}
	}
	return -1, attrs, tbl, nil
}

func (txn *Transaction) updatePosForCNBlock(vec *vector.Vector, idx int) error {
	metaLocs := vector.MustStrCol(vec)
	for i, metaLoc := range metaLocs {
		if location, err := blockio.EncodeLocationFromString(metaLoc); err != nil {
			return err
		} else {
			sid := location.Name().SegmentId()
			blkid := objectio.NewBlockid(&sid, location.Name().Num(), uint16(location.ID()))
			txn.cnBlkId_Pos[*blkid] = Pos{idx: idx, offset: int64(i)}
		}
	}
	return nil
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(typ int, databaseId, tableId uint64,
	databaseName, tableName string, fileName string, bat *batch.Batch, dnStore DNStore) error {
	idx := len(txn.writes)
	// used for cn block compaction (next pr)
	if typ == COMPACTION_CN {
		typ = INSERT
	} else if typ == INSERT {
		txn.updatePosForCNBlock(bat.GetVector(0), idx)
	}
	txn.readOnly.Store(false)
	txn.writes = append(txn.writes, Entry{
		typ:          typ,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          bat,
		dnStore:      dnStore,
	})

	if uid, err := types.ParseUuid(strings.Split(fileName, "_")[0]); err != nil {
		panic("fileName parse Uuid error")
	} else {
		// get uuid string
		if typ == INSERT {
			colexec.Srv.PutCnSegment(&uid, colexec.CnBlockIdType)
		}
	}
	return nil
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {
	mp := make(map[types.Rowid]uint8)
	deleteBlkId := make(map[types.Blockid]bool)
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(0))
	min1 := uint32(math.MaxUint32)
	max1 := uint32(0)
	cnRowIdOffsets := make([]int64, 0, len(rowids))
	for i, rowid := range rowids {
		// process cn block deletes
		uid := rowid.BorrowSegmentID()
		blkid := rowid.CloneBlockID()
		deleteBlkId[blkid] = true
		mp[rowid] = 0
		rowOffset := rowid.GetRowOffset()
		if colexec.Srv != nil && colexec.Srv.GetCnSegmentType(uid) == colexec.CnBlockIdType {
			txn.deletedBlocks.addDeletedBlocks(&blkid, []int64{int64(rowOffset)})
			cnRowIdOffsets = append(cnRowIdOffsets, int64(i))
			continue
		}
		if rowOffset < (min1) {
			min1 = rowOffset
		}

		if rowOffset > max1 {
			max1 = rowOffset
		}
		// update workspace
	}
	// cn rowId antiShrink
	bat.AntiShrink(cnRowIdOffsets)
	if bat.Length() == 0 {
		return bat
	}
	sels := txn.proc.Mp().GetSels()
	txn.deleteTableWrites(databaseId, tableId, sels, deleteBlkId, min1, max1, mp)
	sels = sels[:0]
	for k, rowid := range rowids {
		if mp[rowid] == 0 {
			sels = append(sels, int64(k))
		}
	}
	bat.Shrink(sels)
	txn.proc.Mp().PutSels(sels)
	return bat
}

func (txn *Transaction) deleteTableWrites(
	databaseId uint64,
	tableId uint64,
	sels []int64,
	deleteBlkId map[types.Blockid]bool,
	min, max uint32,
	mp map[types.Rowid]uint8,
) {
	txn.Lock()
	defer txn.Unlock()

	// txn worksapce will have four batch type:
	// 1.RawBatch 2.DN Block RowId(mixed rowid from different block)
	// 3.CN block Meta batch(record block meta generated by cn insert write s3)
	// 4.DN delete Block Meta batch(record block meta generated by cn delete write s3)
	for _, e := range txn.writes {
		// nil batch will generated by comapction or dumpBatch
		if e.bat == nil {
			continue
		}
		// for 3 and 4 above.
		if e.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			continue
		}
		sels = sels[:0]
		if e.tableId == tableId && e.databaseId == databaseId {
			vs := vector.MustFixedCol[types.Rowid](e.bat.GetVector(0))
			if len(vs) == 0 {
				continue
			}
			// skip 2 above
			if !vs[0].BorrowSegmentID().Eq(txn.segId) {
				continue
			}
			// current batch is not be deleted
			if !deleteBlkId[vs[0].CloneBlockID()] {
				continue
			}
			min2 := vs[0].GetRowOffset()
			max2 := vs[len(vs)-1].GetRowOffset()
			if min > max2 || max < min2 {
				continue
			}
			for k, v := range vs {
				if _, ok := mp[v]; !ok {
					sels = append(sels, int64(k))
				} else {
					mp[v]++
				}
			}
			if len(sels) != len(vs) {
				txn.batchSelectList[e.bat] = append(txn.batchSelectList[e.bat], sels...)

			}
		}
	}
}

func (txn *Transaction) allocateID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return txn.idGen.AllocateID(ctx)
}

func (txn *Transaction) genBlock() {
	txn.rowId[4]++
	txn.rowId[5] = INIT_ROWID_OFFSET
}

func (txn *Transaction) getCurrentBlockId() *types.Blockid {
	rowId := types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
	return rowId.BorrowBlockID()
}

func (txn *Transaction) genRowId() types.Rowid {
	if txn.rowId[5] != INIT_ROWID_OFFSET {
		txn.rowId[5]++
	} else {
		txn.rowId[5] = 0
	}
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
}

func (txn *Transaction) mergeTxnWorkspace() {
	txn.Lock()
	defer txn.Unlock()
	for _, e := range txn.writes {
		if sels, ok := txn.batchSelectList[e.bat]; ok {
			e.bat.Shrink(sels)
			delete(txn.batchSelectList, e.bat)
		}
	}
}

func evalFilterExprWithZonemap(
	ctx context.Context,
	meta objectio.ColumnMetaFetcher,
	expr *plan.Expr,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector,
	columnMap map[int]int,
	proc *process.Process,
) (selected bool) {
	return colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, columnMap, zms, vecs)
}

/* used by multi-dn
func needSyncDnStores(ctx context.Context, expr *plan.Expr, tableDef *plan.TableDef,
	priKeys []*engine.Attribute, dnStores []DNStore, proc *process.Process) []int {
	var pk *engine.Attribute

	fullList := func() []int {
		dnList := make([]int, len(dnStores))
		for i := range dnStores {
			dnList[i] = i
		}
		return dnList
	}
	if len(dnStores) == 1 {
		return []int{0}
	}
	for _, key := range priKeys {
		// If it is a composite primary key, skip
		if key.Name == catalog.CPrimaryKeyColName {
			continue
		}
		pk = key
		break
	}
	// have no PrimaryKey, return all the list
	if expr == nil || pk == nil || tableDef == nil {
		return fullList()
	}
	if pk.Type.IsIntOrUint() {
		canComputeRange, intPkRange := computeRangeByIntPk(expr, pk.Name, "")
		if !canComputeRange {
			return fullList()
		}
		if intPkRange.isRange {
			r := intPkRange.ranges
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return fullList()
			}
			intPkRange.isRange = false
			for i := intPkRange.ranges[0]; i <= intPkRange.ranges[1]; i++ {
				intPkRange.items = append(intPkRange.items, i)
			}
		}
		return getListByItems(dnStores, intPkRange.items)
	}
	canComputeRange, hashVal := computeRangeByNonIntPk(ctx, expr, pk.Name, proc)
	if !canComputeRange {
		return fullList()
	}
	listLen := uint64(len(dnStores))
	idx := hashVal % listLen
	return []int{int(idx)}
}
*/

func (txn *Transaction) getTableWrites(databaseId uint64, tableId uint64, writes []Entry) []Entry {
	txn.Lock()
	defer txn.Unlock()
	for _, entry := range txn.writes {
		if entry.databaseId != databaseId {
			continue
		}
		if entry.tableId != tableId {
			continue
		}
		writes = append(writes, entry)
	}
	return writes
}
