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
)

// Bootstrapper is used to create some internal databases and tables at the time
// of MO initialization according to a specific logic. It provides the necessary
// dependencies for other components to be launched later.
type Bootstrapper interface {
	// Bootstrap creates some internal databases and tables at the time of MO
	Bootstrap(ctx context.Context) error
}
