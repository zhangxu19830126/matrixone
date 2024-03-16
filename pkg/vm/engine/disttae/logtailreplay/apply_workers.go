package logtailreplay

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

var w *workers

func init() {
	w = newWorkers(1)
}

type insertCommand struct {
	noData       bool
	rows         *btree.BTreeG[RowEntry]
	primaryIndex *btree.BTreeG[PrimaryIndexEntry]
	rowIDs       []types.Rowid
	timestamps   []types.TS
	primaryKeys  [][]byte
	batch        *batch.Batch
	idx          int
	cb           func()
}

type workers struct {
	max     uint64
	stopper *stopper.Stopper

	inserts []chan insertCommand
}

func newWorkers(max uint64) *workers {
	w := &workers{
		max:     max,
		stopper: stopper.NewStopper("apply-logtail"),
		inserts: make([]chan insertCommand, max),
	}

	for i := uint64(0); i < max; i++ {
		w.inserts[i] = make(chan insertCommand, 10000)
	}

	w.start()
	return w
}

func (w *workers) start() {
	for _, c := range w.inserts {
		cc := c
		w.stopper.RunTask(func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				case cmd := <-cc:
					w.handleRowInsert(cmd)
				}
			}
		})
	}
}

func (w *workers) addInsert(
	hash uint64,
	cmd insertCommand,
) {
	w.inserts[hash%w.max] <- cmd
}

func (w *workers) handleRowInsert(cmd insertCommand) {
	rowID := cmd.rowIDs[cmd.idx]
	ts := cmd.timestamps[cmd.idx]

	blockID := rowID.CloneBlockID()
	pivot := RowEntry{
		BlockID: blockID,
		RowID:   rowID,
		Time:    ts,
	}

	entry, ok := cmd.rows.Get(pivot)
	if !ok {
		entry = pivot
		entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
	}

	if !cmd.noData {
		entry.Batch = cmd.batch
		entry.Offset = int64(cmd.idx)
	}

	entry.PrimaryIndexBytes = cmd.primaryKeys[cmd.idx]

	pkEntry := PrimaryIndexEntry{
		Bytes:      cmd.primaryKeys[cmd.idx],
		RowEntryID: entry.ID,
		BlockID:    blockID,
		RowID:      rowID,
		Time:       entry.Time,
	}

	cmd.rows.Set(entry)
	cmd.primaryIndex.Set(pkEntry)
	cmd.cb()
}
