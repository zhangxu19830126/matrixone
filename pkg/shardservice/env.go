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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

type env struct {
	selectCNLabel string
	cluster       clusterservice.MOCluster
}

func NewEnv(selectCNLabel string) Env {
	return &env{
		selectCNLabel: selectCNLabel,
		cluster:       clusterservice.GetMOCluster(),
	}
}

func (e *env) HasCN(serviceID string) bool {
	_, ok := e.getCN(serviceID)
	return ok
}

func (e *env) Available(
	accountID uint64,
	serviceID string,
) bool {
	cn, ok := e.getCN(serviceID)
	if !ok {
		return false
	}

	if e.selectCNLabel == "" {
		return true
	}

	values, ok := cn.Labels[e.selectCNLabel]
	if !ok {
		return false
	}
	value := fmt.Sprintf("%d", accountID)

	for _, v := range values.Labels {
		if v == value {
			return true
		}
	}
	return false
}

func (e *env) getCN(serviceID string) (metadata.CNService, bool) {
	var cn metadata.CNService
	ok := false
	e.cluster.GetCNServiceWithoutWorkingState(
		clusterservice.NewServiceIDSelector(serviceID),
		func(c metadata.CNService) bool {
			ok = true
			cn = c
			return false
		},
	)
	return cn, ok
}
