package logtailreplay

import (
	"hash/crc64"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

func (p *PartitionState) deleteInAll(entry RowEntry) {
	for _, rows := range p.rows {
		rows.Delete(entry)
	}
}

func (p *PartitionState) get(entry RowEntry) (*btree.BTreeG[PrimaryIndexEntry], RowEntry, bool) {
	for i, rows := range p.rows {
		if old, ok := rows.Get(entry); ok {
			return p.primaryIndexes[i], old, true
		}
	}
	return nil, RowEntry{}, false
}

func (p *PartitionState) getShard(
	pk []byte,
) (*btree.BTreeG[RowEntry], *btree.BTreeG[PrimaryIndexEntry], uint64) {
	hash := crc64.Checksum(pk, crc64.MakeTable(crc64.ECMA))
	idx := hash % shards
	return p.rows[idx], p.primaryIndexes[idx], hash
}

func (p *PartitionState) getMergedRows() *btree.BTreeG[RowEntry] {
	ret := btree.NewBTreeGOptions((RowEntry).Less, btree.Options{NoLocks: true, Degree: 64})
	for _, row := range p.rows {
		iter := row.Copy().Iter()
		for iter.Next() {
			ret.Set(iter.Item())
		}
		iter.Release()
	}
	return ret
}

func (p *PartitionState) getMergedPKs() *btree.BTreeG[PrimaryIndexEntry] {
	ret := btree.NewBTreeGOptions((PrimaryIndexEntry).Less, btree.Options{NoLocks: true, Degree: 64})
	for _, pk := range p.primaryIndexes {
		iter := pk.Copy().Iter()
		for iter.Next() {
			ret.Set(iter.Item())
		}
		iter.Release()
	}
	return ret
}

func exists(
	rows *btree.BTreeG[RowEntry],
	rowID types.Rowid,
	ts types.TS,
) bool {
	iter := rows.Iter()
	defer iter.Release()

	blockID := rowID.CloneBlockID()
	for ok := iter.Seek(RowEntry{
		BlockID: blockID,
		RowID:   rowID,
		Time:    ts,
	}); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.BlockID != blockID {
			break
		}
		if entry.RowID != rowID {
			break
		}
		if entry.Time.Greater(ts) {
			// not visible
			continue
		}
		if entry.Deleted {
			// deleted
			return false
		}
		return true
	}

	return false
}
