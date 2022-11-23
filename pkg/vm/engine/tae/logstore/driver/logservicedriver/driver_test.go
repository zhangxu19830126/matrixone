// Copyright 2021 Matrix Origin
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

package logservicedriver

import (
	// "sync"

	"testing"
	// "github.com/lni/vfs"
	// "github.com/matrixorigin/matrixone/pkg/logservice"
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	// "github.com/panjf2000/ants/v2"
	// // "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	// "github.com/stretchr/testify/assert"
)

// func initTest(t *testing.T) (*logservice.Service, *logservice.ClientConfig) {
// 	fs := vfs.NewStrictMem()
// 	service, ccfg, err := logservice.NewTestService(fs)
// 	assert.NoError(t, err)
// 	return service, &ccfg
// }

// func restartDriver(t *testing.T, d *LogServiceDriver) *LogServiceDriver {
// 	assert.NoError(t, d.Close())
// 	t.Log("Addr:")
// 	// preAddr:=d.addr
// 	for lsn, intervals := range d.addr {
// 		t.Logf("%d %v", lsn, intervals)
// 	}
// 	// preLsns:=d.validLsn
// 	t.Logf("Valid lsn: %v", d.validLsn)
// 	t.Logf("Driver Lsn %d, Syncing %d, Synced %d", d.driverLsn, d.syncing, d.synced)
// 	t.Logf("Truncated %d", d.truncating)
// 	t.Logf("LSTruncated %d", d.truncatedLogserviceLsn)
// 	d = NewLogServiceDriver(d.config)
// 	tempLsn := uint64(0)
// 	err := d.Replay(func(e *entry.Entry) {
// 		if e.Lsn <= tempLsn {
// 			panic("logic err")
// 		}
// 		tempLsn = e.Lsn
// 	})
// 	assert.NoError(t, err)
// 	t.Log("Addr:")
// 	for lsn, intervals := range d.addr {
// 		t.Logf("%d %v", lsn, intervals)
// 	}
// 	// assert.Equal(t,len(preAddr),len(d.addr))
// 	// for lsn,intervals := range preAddr{
// 	// 	replayedInterval,ok:=d.addr[lsn]
// 	// 	assert.True(t,ok)
// 	// 	assert.Equal(t,intervals.Intervals[0].Start,replayedInterval.Intervals[0].Start)
// 	// 	assert.Equal(t,intervals.Intervals[0].End,replayedInterval.Intervals[0].End)
// 	// }
// 	t.Logf("Valid lsn: %v", d.validLsn)
// 	// assert.Equal(t,preLsns.GetCardinality(),d.validLsn.GetCardinality())
// 	t.Logf("Driver Lsn %d, Syncing %d, Synced %d", d.driverLsn, d.syncing, d.synced)
// 	t.Logf("Truncated %d", d.truncating)
// 	t.Logf("LSTruncated %d", d.truncatedLogserviceLsn)
// 	return d
// }

func TestAppendRead(t *testing.T) {
	// service, ccfg := initTest(t)
	// defer service.Close()

	// cfg := NewTestConfig(ccfg)
	// driver := NewLogServiceDriver(cfg)

	// entryCount := 100
	// wg := &sync.WaitGroup{}
	// worker, _ := ants.NewPool(10)
	// entries := make([]*entry.Entry, entryCount)
	// appendfn := func(i int) func() {
	// 	return func() {
	// 		e := entry.MockEntry()
	// 		driver.Append(e)
	// 		entries[i] = e
	// 		wg.Done()
	// 	}
	// }

	// reanfn := func(i int) func() {
	// 	return func() {
	// 		e := entries[i]
	// 		e.WaitDone()
	// 		e2, err := driver.Read(e.Lsn)
	// 		assert.NoError(t, err)
	// 		assert.Equal(t, e2.Lsn, e.Lsn)
	// 		_, lsn1 := e.Entry.GetLsn()
	// 		_, lsn2 := e2.Entry.GetLsn()
	// 		assert.Equal(t, lsn1, lsn2)
	// 		wg.Done()
	// 		e2.Entry.Free()
	// 	}
	// }

	// for i := 0; i < entryCount; i++ {
	// 	wg.Add(1)
	// 	worker.Submit(appendfn(i))
	// }
	// wg.Wait()

	// for i := 0; i < entryCount; i++ {
	// 	wg.Add(1)
	// 	worker.Submit(reanfn(i))
	// }
	// wg.Wait()

	// driver = restartDriver(t, driver)

	// for i := 0; i < entryCount; i++ {
	// 	wg.Add(1)
	// 	worker.Submit(reanfn(i))
	// }
	// wg.Wait()

	// for _, e := range entries {
	// 	e.Entry.Free()
	// }

	// driver.Close()
}

func TestTruncate(t *testing.T) {
	// service, ccfg := initTest(t)
	// defer service.Close()

	// cfg := NewTestConfig(ccfg)
	// driver := NewLogServiceDriver(cfg)

	// entryCount := 10
	// entries := make([]*entry.Entry, entryCount)
	// wg := &sync.WaitGroup{}
	// worker, _ := ants.NewPool(20)
	// truncatefn := func(i int, dr *LogServiceDriver) func() {
	// 	return func() {
	// 		e := entries[i]
	// 		assert.NoError(t, e.WaitDone())
	// 		assert.NoError(t, dr.Truncate(e.Lsn))
	// 		testutils.WaitExpect(4000, func() bool {
	// 			trucated, err := dr.GetTruncated()
	// 			assert.NoError(t, err)
	// 			return trucated >= e.Lsn
	// 		})
	// 		truncated, err := dr.GetTruncated()
	// 		assert.NoError(t, err)
	// 		assert.GreaterOrEqual(t, truncated, e.Lsn)
	// 		wg.Done()
	// 	}
	// }
	// appendfn := func(i int, dr *LogServiceDriver) func() {
	// 	return func() {
	// 		e := entry.MockEntry()
	// 		dr.Append(e)
	// 		entries[i] = e
	// 		worker.Submit(truncatefn(i, dr))
	// 	}
	// }

	// for i := 0; i < entryCount; i++ {
	// 	wg.Add(1)
	// 	worker.Submit(appendfn(i, driver))
	// }

	// wg.Wait()

	// // driver = restartDriver(t, driver)

	// // for i := 0; i < entryCount; i++ {
	// // 	wg.Add(1)
	// // 	worker.Submit(appendfn(i, driver))
	// // }

	// assert.NoError(t, driver.Close())
}

// func Test1(t *testing.T) {
// 	errs := make(map[string]string)
// 	stack := make(map[string]string)
// 	data, _ := os.ReadFile("/Users/zhangxu/Downloads/manually_integrate_test_reports/failed_1/w.log")
// 	for _, line := range strings.Split(string(data), "\n") {
// 		if line == "" {
// 			continue
// 		}

// 		id := line[143:179]
// 		msg := line[84:97]
// 		if _, ok := errs[id]; ok {
// 			continue
// 		}

// 		if _, ok := stack[id]; ok {
// 			if msg != "read response" {
// 				errs[id] = id
// 				t.Log(id)
// 				continue
// 			}
// 			delete(stack, id)
// 		} else {
// 			if msg != "write request" {
// 				errs[id] = id
// 				t.Log(id)
// 				continue
// 			}
// 			stack[id] = msg
// 		}
// 	}
// 	assert.Fail(t, "")
// }

// func Test2(t *testing.T) {
// 	v := make([]byte, 100)
// 	v[0] = 29

// 	s := (*reflect.SliceHeader)(unsafe.Pointer(&v))
// 	t.Log(s.Cap)
// 	t.Log(s.Len)

// 	v2 := v[5:]
// 	s2 := (*reflect.SliceHeader)(unsafe.Pointer(&v2))
// 	t.Log(s2.Cap)
// 	t.Log(s2.Len)

// 	p := (*int)(unsafe.Pointer(uintptr(unsafe.Pointer(&v2[0])) - unsafe.Sizeof(v2[0])))
// 	assert.Equal(t, 30, *p)

// 	assert.Equal(t, fmt.Sprintf("%p", v), fmt.Sprintf("%p", &v))
// }
