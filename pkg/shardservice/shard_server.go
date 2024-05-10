// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"go.uber.org/zap"
)

type ServerOption func(*server)

type server struct {
	cfg        Config
	r          *rt
	env        Env
	schedulers []scheduler
	filters    []filter
	stopper    *stopper.Stopper
	rpc        morpc.MethodBasedServer[*pb.Request, *pb.Response]
}

func NewShardServer(
	cfg Config,
	env Env,
	opts ...ServerOption,
) ShardServer {
	s := &server{
		cfg: cfg,
		env: env,
		r:   newRuntime(env),
		stopper: stopper.NewStopper(
			"shard-server",
			stopper.WithLogger(getLogger().RawLogger()),
		),
	}
	for _, opt := range opts {
		opt(s)
	}

	s.initRPC()
	s.initSchedulers()
	return s
}

func (s *server) Start() {
	if err := s.stopper.RunTask(s.schedule); err != nil {
		panic(err)
	}

	if err := s.rpc.Start(); err != nil {
		panic(err)
	}
}

func (s *server) Stop() {
	if err := s.rpc.Close(); err != nil {
		panic(err)
	}
	s.stopper.Stop()
}

func (s *server) initSchedulers() {
	s.schedulers = append(
		s.schedulers,
		newDownScheduler(),
		newAllocateScheduler(),
		newBalanceScheduler(s.cfg.MaxScheduleTablesOnce))
}

func (s *server) initRPC() {
	pool := morpc.NewMessagePool[*pb.Request, *pb.Response](
		func() *pb.Request {
			return &pb.Request{}
		},
		func() *pb.Response {
			return &pb.Response{}
		},
	)

	rpc, err := morpc.NewMessageHandler[*pb.Request, *pb.Response](
		"shard-server",
		s.cfg.ListenAddress,
		s.cfg.RPC,
		pool,
	)
	if err != nil {
		panic(err)
	}
	s.rpc = rpc

	s.initHandlers()
}

func (s *server) initHandlers() {
	s.rpc.RegisterMethod(uint32(pb.Method_Heartbeat), s.handleHeartbeat, true)
	s.rpc.RegisterMethod(uint32(pb.Method_CreateShards), s.handleCreateShards, true)
	s.rpc.RegisterMethod(uint32(pb.Method_DeleteShards), s.handleDeleteShards, true)
}

func (s *server) handleHeartbeat(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
) error {
	resp.Heartbeat.CMDs = s.r.heartbeat(
		req.Heartbeat.CN,
		req.Heartbeat.Shards,
	)
	return nil
}

func (s *server) handleCreateShards(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
) error {
	id := req.CreateShards.ID
	v := req.CreateShards.TableShards

	if v.Policy == pb.Policy_None {
		return nil
	}
	if v.ShardsCount == 0 {
		panic("shards count is 0")
	}

	table := &table{
		metadata:  v,
		id:        id,
		allocated: false,
	}
	table.shards = make([]pb.TableShard, 0, v.ShardsCount)
	for i := uint32(0); i < v.ShardsCount; i++ {
		table.shards = append(table.shards,
			pb.TableShard{
				TableID:       id,
				State:         pb.ShardState_Allocating,
				BindVersion:   0,
				ShardsVersion: v.Version,
				ShardID:       uint64(i),
			})
	}
	if v.Policy == pb.Policy_Partition {
		ids := req.CreateShards.PhysicalShardIDs
		if len(ids) != len(table.shards) {
			panic("partition and shard count not match")
		}
		for i, id := range ids {
			table.shards[i].ShardID = id
		}
	}

	s.r.add(table)
	return nil
}

func (s *server) handleDeleteShards(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
) error {
	id := req.DeleteShards.ID
	s.r.delete(id)
	return nil
}

func (s *server) schedule(ctx context.Context) {
	timer := time.NewTimer(s.cfg.ScheduleDuration.Duration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.doSchedule()
			timer.Reset(s.cfg.ScheduleDuration.Duration)
		}
	}
}

func (s *server) doSchedule() {
	for _, scheduler := range s.schedulers {
		if err := scheduler.schedule(s.r, s.filters...); err != nil {
			getLogger().Error("schedule shards failed",
				zap.Error(err))
		}
	}
}
