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

package morpc

import (
	"time"
)

const (
	internalTimeout = time.Second * 10
	oneWayTimeout   = time.Second * 600
)

// Timeout return true if the message is timeout
func (m RPCMessage) Timeout() bool {
	select {
	case <-m.Ctx.Done():
		return true
	default:
		return time.Now().After(m.timeoutAt)
	}
}

func (m RPCMessage) GetTimeout() time.Duration {
	return time.Until(m.timeoutAt)
}
