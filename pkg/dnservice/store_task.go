// Copyright 2021 - 2022 Matrix Origin
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

package dnservice

import (
	"context"
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"go.uber.org/zap"
)

func (s *store) initTaskHolder() {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.serviceHolder != nil {
		return
	}

	s.task.serviceHolder = taskservice.NewTaskServiceHolder(s.logger, func() (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		details, err := s.hakeeperClient.GetClusterDetails(ctx)
		if err != nil {
			return "", err
		}
		if len(details.CNStores) == 0 {
			return "", moerr.NewInvalidState("no cn in the cluster")
		}

		n := rand.Intn(len(details.CNStores))
		return details.CNStores[n].SQLAddress, nil
	})
}

func (s *store) createTaskService(command *logservicepb.CreateTaskService) {
	s.task.Lock()
	defer s.task.Unlock()
	if s.task.serviceCreated {
		return
	}
	if err := s.task.serviceHolder.Create(*command); err != nil {
		s.logger.Error("create task service failed",
			zap.Error(err))
		return
	}
	s.task.serviceCreated = true
}

func (s *store) taskServiceCreated() bool {
	s.task.RLock()
	defer s.task.RUnlock()
	return s.task.serviceCreated
}
