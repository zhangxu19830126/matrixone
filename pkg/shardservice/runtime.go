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
	"sort"
	"sync"

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"go.uber.org/zap"
)

// rt is the information about the Shards of the MO cluster maintained
// on the ShardServer, which contains information about which CNs are
// available, which Shards' Tables are available, the distribution of
// Shards and CNs, and so on.
type rt struct {
	sync.RWMutex
	env    Env
	tables map[uint64]*table
	cns    map[string]*cn
}

func newRuntime(env Env) *rt {
	return &rt{
		env:    env,
		tables: make(map[uint64]*table, 256),
		cns:    make(map[string]*cn, 256),
	}
}

// heartbeat each cn node periodically reports the table shards it manages
// to the shard server.
//
// Based on the reported information, the shard server will determine whether
// the operator sent to the corresponding cn is complete or not.
//
// When shard server restarts, all table shards information is lost. The table
// shard metadata is persistent data, and the shard and CN binding metadata is
// dynamically changing data from runtime.
//
// When the shard server discovers that the table's shards metadata is missing
// from memory, it returns a CreateTable operator that allows the corresponding
// CN.
func (r *rt) heartbeat(
	cn string,
	shards []pb.TableShard,
) []pb.Operator {
	if !r.env.HasCN(cn) {
		return []pb.Operator{newDeleteAllOp()}
	}

	r.Lock()
	defer r.Unlock()

	c, ok := r.cns[cn]
	if !ok {
		c = r.newCN(cn)
		r.cns[cn] = c
	}
	if c.isDown() {
		return []pb.Operator{newDeleteAllOp()}
	}

	c.closeCompletedOp(
		shards,
		func(
			shard pb.TableShard,
			replica pb.ShardReplica,
		) {
			if t, ok := r.tables[shard.TableID]; ok {
				t.allocateCompleted(shard, replica)
			}
		},
	)

	var ops []pb.Operator
	for _, s := range shards {
		if t, ok := r.tables[s.TableID]; ok {
			for _, r := range s.Replicas {
				if !t.valid(s, r) {
					c.addOps(newDeleteReplicaOp(s, r))
				}
			}
		} else {
			ops = append(ops, newCreateTableOp(s.TableID))
		}
	}
	if len(ops) == 0 &&
		len(c.incompleteOps) == 0 {
		return nil
	}

	return append(ops, c.incompleteOps...)
}

func (r *rt) add(
	t *table,
) {
	r.Lock()
	defer r.Unlock()
	old, ok := r.tables[t.id]
	if !ok {
		r.tables[t.id] = t
		return
	}

	// shards not changed
	if old.metadata.Version >= t.metadata.Version {
		return
	}

	r.deleteTableLocked(old)
	r.tables[t.id] = t
}

func (r *rt) delete(id uint64) {
	r.Lock()
	defer r.Unlock()

	table, ok := r.tables[id]
	if !ok {
		return
	}
	r.deleteTableLocked(table)
	delete(r.tables, id)
}

func (r *rt) get(
	id uint64,
) []pb.TableShard {
	r.RLock()
	defer r.RUnlock()

	table, ok := r.tables[id]
	if !ok || !table.allocated {
		return nil
	}

	shards := make([]pb.TableShard, 0, len(table.shards))
	shards = append(shards, table.shards...)
	return shards
}

func (r *rt) deleteTableLocked(t *table) {
	getLogger().Info("remove table shards",
		zap.Uint64("table", t.id),
		tableShardsField("shards", t.metadata),
		tableShardSliceField("binds", t.shards))

	for _, shard := range t.shards {
		for _, replica := range shard.Replicas {
			if replica.CN == "" {
				continue
			}
			r.addOpLocked(
				replica.CN,
				newDeleteReplicaOp(shard, replica),
			)
		}
	}
}

func (r *rt) newCN(id string) *cn {
	return &cn{
		id:    id,
		state: pb.CNState_Up,
	}
}

func (r *rt) getAvailableCNsLocked(
	t *table,
	filters ...filter,
) []*cn {
	var cns []*cn
	for _, cn := range r.cns {
		if cn.available(t.metadata.TenantID, r.env) {
			cns = append(cns, cn)
		}
	}
	sort.Slice(cns, func(i, j int) bool {
		return cns[i].id < cns[j].id
	})
	for _, f := range filters {
		cns = f.filter(r, cns)
		if len(cns) == 0 {
			return nil
		}
	}
	return cns
}

func (r *rt) hasNotRunningShardLocked(
	cn string,
) bool {
	for _, t := range r.tables {
		for _, s := range t.shards {
			for _, r := range s.Replicas {
				if r.CN == cn &&
					r.State != pb.ReplicaState_Running {
					return true
				}
			}
		}
	}
	return false
}

func (r *rt) addOpLocked(
	cn string,
	ops ...pb.Operator,
) {
	if c, ok := r.cns[cn]; ok {
		c.addOps(ops...)
	}
}

func (r *rt) getDownCNsLocked(downCNs map[string]struct{}) {
	for _, cn := range r.cns {
		if !cn.isDown() && r.env.HasCN(cn.id) {
			continue
		}

		cn.down()
		delete(r.cns, cn.id)
		downCNs[cn.id] = struct{}{}
	}
}

func (t *table) newShardReplicas(
	count int,
	initVersion uint64,
) []pb.ShardReplica {
	replicas := make([]pb.ShardReplica, 0, count)
	for i := 0; i < count; i++ {
		replicas = append(
			replicas,
			pb.ShardReplica{
				ReplicaID: t.nextReplicaID(),
				State:     pb.ReplicaState_Allocating,
				Version:   initVersion,
			},
		)
	}
	return replicas
}

type table struct {
	id        uint64
	metadata  pb.ShardsMetadata
	shards    []pb.TableShard
	allocated bool
	replicaID uint64
}

func newTable(
	id uint64,
	metadata pb.ShardsMetadata,
	initReplicaVersion uint64,
) *table {
	t := &table{
		metadata:  metadata,
		id:        id,
		allocated: false,
	}
	if metadata.MaxReplicaCount == 0 {
		metadata.MaxReplicaCount = 1
	}

	n := metadata.ShardsCount * metadata.MaxReplicaCount
	t.shards = make([]pb.TableShard, 0, n)
	for i := uint32(0); i < metadata.ShardsCount; i++ {
		for j := uint32(0); j < metadata.MaxReplicaCount; j++ {
			t.shards = append(t.shards,
				pb.TableShard{
					Policy:   metadata.Policy,
					TableID:  id,
					Version:  metadata.Version,
					ShardID:  uint64(i + 1),
					Replicas: t.newShardReplicas(int(metadata.MaxReplicaCount), initReplicaVersion),
				})
		}
	}
	if metadata.Policy == pb.Policy_Partition {
		ids := metadata.PhysicalShardIDs
		if len(ids) != len(t.shards) {
			panic("partition and shard count not match")
		}
		for i, id := range ids {
			t.shards[i].ShardID = id
		}
	}
	return t
}

func (t *table) allocate(
	cn string,
	shardIdx int,
	replicaIdx int,
) {
	t.shards[shardIdx].Replicas[replicaIdx].CN = cn
	t.shards[shardIdx].Replicas[replicaIdx].State = pb.ReplicaState_Allocated
	t.shards[shardIdx].Replicas[replicaIdx].Version++
}

func (t *table) needAllocate() bool {
	if !t.allocated {
		return true
	}
	for _, s := range t.shards {
		for _, r := range s.Replicas {
			if r.State == pb.ReplicaState_Tombstone {
				return true
			}
		}
	}
	return false
}

func (t *table) getReplicaCount(cn string) int {
	count := 0
	for _, s := range t.shards {
		for _, r := range s.Replicas {
			if r.CN == cn {
				count++
			}
		}
	}
	return count
}

func (t *table) valid(
	target pb.TableShard,
	replica pb.ShardReplica,
) bool {
	for _, current := range t.shards {
		if current.ShardID != target.ShardID {
			continue
		}
		if current.Version > target.Version {
			return false
		}

		if target.Version > current.Version {
			panic("BUG: receive newer shard version than current")
		}

		for _, r := range current.Replicas {
			if r.ReplicaID != replica.ReplicaID {
				continue
			}

			if r.Version > replica.Version {
				return false
			}

			if replica.Version > r.Version {
				panic("BUG: receive newer shard replica version than current")
			}
		}
		return true
	}
	return false
}

func (t *table) moveLocked(
	from,
	to string,
) (pb.TableShard, pb.ShardReplica, pb.ShardReplica) {
	for i := range t.shards {
		for j := range t.shards[i].Replicas {
			if t.shards[i].Replicas[j].CN == from &&
				t.shards[i].Replicas[j].State == pb.ReplicaState_Running {
				old := t.shards[i].Replicas[j]
				old.State = pb.ReplicaState_Tombstone

				t.shards[i].Replicas[j].CN = to
				t.shards[i].Replicas[j].Version++
				t.shards[i].Replicas[j].State = pb.ReplicaState_Allocated
				return t.shards[i], old, t.shards[i].Replicas[j]
			}
		}
	}
	panic("cannot find running shard replica")
}

func (t *table) allocateCompleted(
	shard pb.TableShard,
	replica pb.ShardReplica,
) {
	i, j, ok := t.findReplica(shard, replica)
	if ok && t.shards[i].Replicas[j].State == pb.ReplicaState_Allocated {
		t.shards[i].Replicas[j].State = pb.ReplicaState_Running
	}
}

func (t *table) findReplica(
	shard pb.TableShard,
	replica pb.ShardReplica,
) (int, int, bool) {
	for i, s := range t.shards {
		if !shard.Same(s) {
			continue
		}

		for j, r := range s.Replicas {
			if !replica.Same(r) {
				continue
			}
			return i, j, true
		}
		break
	}
	return 0, 0, false
}

func (t *table) nextReplicaID() uint64 {
	t.replicaID++
	return t.replicaID
}

type cn struct {
	id            string
	state         pb.CNState
	incompleteOps []pb.Operator
	notifyOps     []pb.Operator
}

func (c *cn) available(
	tenantID uint32,
	env Env,
) bool {
	if c.state == pb.CNState_Down {
		return false
	}
	return env.Available(tenantID, c.id)
}

func (c *cn) isDown() bool {
	return c.state == pb.CNState_Down
}

func (c *cn) down() {
	c.state = pb.CNState_Down
	c.incompleteOps = c.incompleteOps[:0]
}

func (c *cn) addOps(
	ops ...pb.Operator,
) {
	for _, op := range ops {
		if !c.hasSame(op) {
			c.incompleteOps = append(c.incompleteOps, op)
		}
	}
}

func (c *cn) hasSame(
	op pb.Operator,
) bool {
	for _, old := range c.incompleteOps {
		if old.TableID == op.TableID &&
			old.Type == op.Type {
			switch op.Type {
			case pb.OpType_DeleteReplica, pb.OpType_AddReplica:
				if old.TableShard.Same(op.TableShard) &&
					old.Replica.Same(op.Replica) {
					return true
				}
			default:
				return true
			}
		}
	}
	return false
}

func (c *cn) closeCompletedOp(
	shards []pb.TableShard,
	apply func(pb.TableShard, pb.ShardReplica),
) {
	has := func(
		s pb.TableShard,
		r pb.ShardReplica,
	) bool {
		for _, shard := range shards {
			if s.Same(shard) {
				for _, replica := range shard.Replicas {
					if r.Same(replica) {
						return true
					}
				}
				return false
			}
		}
		return false
	}

	ops := c.incompleteOps[:0]
	c.notifyOps = c.notifyOps[:0]
	for _, op := range c.incompleteOps {
		switch op.Type {
		case pb.OpType_DeleteReplica:
			if !has(op.TableShard, op.Replica) {
				continue
			}
			ops = append(ops, op)
			c.notifyOps = append(c.notifyOps, op)
		case pb.OpType_AddReplica:
			if has(op.TableShard, op.Replica) {
				apply(op.TableShard, op.Replica)
				continue
			}
			ops = append(ops, op)
			c.notifyOps = append(c.notifyOps, op)
		}
	}
	c.incompleteOps = ops
}
