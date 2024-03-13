// Copyright 2024 Matrix Origin
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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	taeLogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

var (
	unsubscribeCmdType byte = 1
	timestampCmdType   byte = 2
	tableCmdType       byte = 3
)

type stateCache struct {
	tableCache *cache.TableItem
	partition  *logtailreplay.Partition
	state      *logtailreplay.PartitionState
	done       func()
}

func (c *PushClient) newConsumeWorker(
	ctx context.Context,
	workerID int,
	bufferSize int,
	engine *Engine,
) logtailConsumerWorker {
	w := logtailConsumerWorker{
		engine:    engine,
		workerID:  workerID,
		closeC:    make(chan bool),
		commandsC: make(chan command, bufferSize),
		errC:      c.errC,
	}

	w.start(ctx)
	return w
}

type logtailConsumerWorker struct {
	workerID  int
	engine    *Engine
	closeC    chan bool
	commandsC chan command
	errC      chan error
	applied   timestamp.Timestamp
}

func (w *logtailConsumerWorker) syncApply(
	ctx context.Context,
	tail logtail.TableLogtail,
	receiveAt time.Time,
) error {
	return w.apply(ctx, newTableCmd(tail, receiveAt), false)
}

func (w *logtailConsumerWorker) addTail(
	tail logtail.TableLogtail,
	receiveAt time.Time,
) {
	w.commandsC <- newTableCmd(tail, receiveAt)
}

func (w *logtailConsumerWorker) addTimestamp(
	target timestamp.Timestamp,
	receiveAt time.Time,
) {
	w.commandsC <- newTimestampCmd(target, receiveAt)
}

func (w *logtailConsumerWorker) addUnsubscribe(
	tail *logtail.UnSubscribeResponse,
	receiveAt time.Time,
) {
	w.commandsC <- newUnsubscribeCmd(tail, receiveAt)
}

func (w *logtailConsumerWorker) close() {
	w.closeC <- true
}

func (w *logtailConsumerWorker) start(ctx context.Context) {
	go func() {
		hasError := false

		for {
			select {
			case cmd := <-w.commandsC:
				if hasError {
					continue
				}
				if err := w.apply(ctx, cmd, true); err != nil {
					hasError = true
					w.errC <- err
				}
			case <-w.closeC:
				close(w.closeC)
				close(w.commandsC)
				return
			}
		}
	}()
}

func (w *logtailConsumerWorker) apply(
	ctx context.Context,
	cmd command,
	async bool,
) error {
	switch cmd.cmdType {
	case tableCmdType:
		return w.applyTable(ctx, cmd, async)
	case timestampCmdType:
		w.applyTimestamp(ctx, cmd)
		return nil
	case unsubscribeCmdType:
		return w.applyUnsubscribe(ctx, cmd)
	default:
		panic(fmt.Sprintf("not support cmd type: %d", cmd.cmdType))
	}
}

func (w *logtailConsumerWorker) applyTable(
	ctx context.Context,
	cmd command,
	async bool,
) error {
	return w.doApply(
		ctx,
		cmd.tail.table,
		cmd.receiveAt,
		async)
}

func (w *logtailConsumerWorker) applyUnsubscribe(
	_ context.Context,
	cmd command,
) error {
	table := cmd.tail.unSubscribe.Table
	w.engine.cleanMemoryTableWithTable(table.DbId, table.TbId)
	w.engine.pClient.subscribed.setTableUnsubscribe(table.DbId, table.TbId)
	return nil
}

func (w *logtailConsumerWorker) applyTimestamp(
	_ context.Context,
	cmd command,
) {
	if cmd.target.Greater(w.applied) {
		w.applied = cmd.target

		if w.workerID < consumeWorkers {
			w.engine.pClient.receivedLogTailTime.updateApplied(w.workerID, w.applied)
		}
	}
}

func (w *logtailConsumerWorker) doApply(
	ctx context.Context,
	tail logtail.TableLogtail,
	receiveAt time.Time,
	async bool,
) error {
	start := time.Now()
	v2.LogTailApplyLatencyDurationHistogram.Observe(start.Sub(receiveAt).Seconds())
	defer func() {
		v2.LogTailApplyDurationHistogram.Observe(time.Since(start).Seconds())

		// after consume the logtail, enqueue it to global stats.
		w.engine.globalStats.enqueue(tail)
	}()

	db, table := tail.Table.GetDbId(), tail.Table.GetTbId()
	stateCache, err := w.getStateCache(ctx, db, table)
	if err != nil {
		return err
	}
	defer func() {
		stateCache.partition.Unlock()
	}()

	if async && len(tail.CkpLocation) > 0 {
		stateCache.state.AppendCheckpoint(tail.CkpLocation, stateCache.partition)
	} else if !async {
		var entries []*api.Entry
		var closeCBs []func()
		if entries, closeCBs, err = taeLogtail.LoadCheckpointEntries(
			ctx,
			tail.CkpLocation,
			stateCache.tableCache.Id,
			stateCache.tableCache.Name,
			db,
			"",
			w.engine.mp,
			w.engine.fs); err != nil {
			return err
		}
		defer func() {
			for _, cb := range closeCBs {
				cb()
			}
		}()
		for _, entry := range entries {
			if err = consumeEntry(
				ctx,
				stateCache.tableCache.PrimarySeqnum,
				w.engine,
				stateCache.state,
				entry); err != nil {
				return err
			}
		}
	}

	err = hackConsumeLogtail(
		ctx,
		stateCache.tableCache.PrimarySeqnum,
		w.engine,
		stateCache.state,
		tail,
	)
	if err != nil {
		// logutil.Errorf("%s consume %d-%s log tail error: %v\n", logTag, key.Id, key.Name, err)
		return err
	}

	stateCache.partition.TS = *tail.Ts

	stateCache.done()
	return nil
}

func (w *logtailConsumerWorker) getStateCache(
	ctx context.Context,
	db, table uint64,
) (stateCache, error) {
	partition := w.engine.getPartition(db, table)
	tableCache := w.engine.catalog.GetTableById(db, table)

	if err := partition.Lock(ctx); err != nil {
		return stateCache{}, err
	}

	state, done := partition.MutateState()
	value := stateCache{
		partition:  partition,
		state:      state,
		done:       done,
		tableCache: tableCache,
	}
	return value, nil
}

type command struct {
	cmdType   byte
	receiveAt time.Time
	target    timestamp.Timestamp
	tail      struct {
		subscribe   *logtail.SubscribeResponse
		unSubscribe *logtail.UnSubscribeResponse
		table       logtail.TableLogtail
	}
}

func newUnsubscribeCmd(
	tail *logtail.UnSubscribeResponse,
	receiveAt time.Time,
) command {
	c := command{
		cmdType:   unsubscribeCmdType,
		receiveAt: receiveAt,
	}
	c.tail.unSubscribe = tail
	return c
}

func newTableCmd(
	tail logtail.TableLogtail,
	receiveAt time.Time,
) command {
	c := command{
		cmdType:   tableCmdType,
		receiveAt: receiveAt,
	}
	c.tail.table = tail
	return c
}

func newTimestampCmd(
	target timestamp.Timestamp,
	receiveAt time.Time,
) command {
	c := command{
		cmdType:   timestampCmdType,
		receiveAt: receiveAt,
	}
	c.target = target
	return c
}
