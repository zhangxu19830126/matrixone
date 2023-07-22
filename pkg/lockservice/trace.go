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

package lockservice

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	traceID atomic.Uint64
	l       sync.Mutex
	traces  = make(map[string]*tracings)
)

func printTraces() {
	l.Lock()
	defer l.Unlock()
	for k, v := range traces {
		fmt.Printf("trace: %s: [%s]\n", k, v.String())
	}
}

func addTrace(txnID []byte, action string) func() {
	id := traceID.Add(1)
	key := hex.EncodeToString(txnID)
	l.Lock()
	defer l.Unlock()
	v, ok := traces[key]
	if !ok {
		v = &tracings{}
		traces[key] = v
	}

	v.calls = append(v.calls, call{id: id, action: action})
	return func() {
		l.Lock()
		defer l.Unlock()
		v, ok := traces[key]
		if ok {
			newCalls := v.calls[:0]
			for _, c := range v.calls {
				if c.id != id {
					newCalls = append(newCalls, c)
				}
			}
			v.calls = newCalls
			if len(newCalls) == 0 {
				delete(traces, key)
			}
		}
	}
}

type tracings struct {
	calls []call
}

func (t *tracings) String() string {
	var buf bytes.Buffer
	for _, c := range t.calls {
		buf.WriteString(c.action)
		buf.WriteString("->")
	}
	return buf.String()
}

type call struct {
	id     uint64
	action string
}
