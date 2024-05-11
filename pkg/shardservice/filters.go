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
	"time"
)

type freezeFilter struct {
	freeze        map[string]time.Time
	maxFreezeTime time.Duration
}

func newFreezeFilter(maxFreezeTime time.Duration) *freezeFilter {
	return &freezeFilter{
		freeze:        make(map[string]time.Time),
		maxFreezeTime: maxFreezeTime,
	}
}

func (f *freezeFilter) filter(
	r *rt,
	cns []*cn,
) []*cn {
	if len(f.freeze) == 0 {
		return cns
	}
	values := cns[:0]
	for _, c := range cns {
		if t, ok := f.freeze[c.id]; !ok ||
			time.Since(t) > f.maxFreezeTime {
			values = append(values, c)
			delete(f.freeze, c.id)
		}
	}
	return values
}

type stateFilter struct {
}

func newStateFilter() *stateFilter {
	return &stateFilter{}
}

func (f *stateFilter) filter(r *rt, cns []*cn) []*cn {
	if len(cns) == 0 {
		return cns
	}
	values := cns[:0]
	for _, c := range cns {
		if !r.hasNotRunningShardLocked(c.id) {
			values = append(values, c)
		}
	}
	return values
}
