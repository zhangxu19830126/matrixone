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

package bootstrap

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// Service is used to bootstrap and upgrade mo cluster.
//
// In bootstrap, it will create some internal databases and tables at the time of MO
// initialization according to a specific logic. It provides the necessary dependencies
// for other components to be launched later.
//
// In upgrade, it will upgrade the metadata between two MO versions. When there is a
// need to modify the original data (e.g. create a system table, modify the system table
// structure, etc.) between two MO versions, then an upgrade process is required. This
// process is used to ensure that each tenant's data is properly upgraded and updated.
//
// Note that, this service is not used to bootstrap logservice and dn. The internal
// databases and tables as below:
// 1. mo_indexes in mo_catalog
// 2. task infrastructure database
type Service interface {
	// Bootstrap try to bootstrap and upgrade mo cluster
	Bootstrap(ctx context.Context) error
}

// Locker locker is used to get lock to bootstrap. Only one cn can get lock to bootstrap.
// Other cns need to wait bootstrap completed.
type Locker interface {
	// Get return true means get lock
	Get(ctx context.Context, key string) (bool, error)
}

// UpgradeHandle every version that needs to be upgraded with cluster metadata needs to
// have an UpgradeHandle implementation!
type UpgradeHandle interface {
	// Metadata version metadata
	Metadata() versions.Version
	// Prepare prepare upgrade. This upgrade will be executed before cluster and tenant upgrade.
	Prepare(ctx context.Context, txn executor.TxnExecutor) error
	// ClusterNeedUpgrade handle upgrade cluster metadata. This upgrade will be executed before
	// tenant upgrade.
	HandleClusterUpgrade(ctx context.Context, txn executor.TxnExecutor) error
	// HandleTenantUpgrade handle upgrade a special tenant.
	HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error
}
