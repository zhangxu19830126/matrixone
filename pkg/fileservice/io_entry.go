// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"bytes"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/mocache"
)

func (e *IOEntry) read(r io.Reader, path string, offset uint64,
	c *mocache.Cache, skipMemWrite bool) error {
	var err error

	if e.Size < 0 {
		var data []byte
		data, err = io.ReadAll(r)
		if err != nil {
			return err
		}
		e.Data = data
		e.Size = int64(len(data))
	} else {
		if int64(len(e.Data)) < e.Size {
			e.Data = make([]byte, e.Size)
		}
		var n int
		n, err = io.ReadFull(r, e.Data)
		if err != nil {
			return err
		}
		if int64(n) != e.Size {
			return moerr.NewUnexpectedEOFNoCtx(path)
		}
	}
	return e.set(path, offset, c, skipMemWrite)
}

func (e *IOEntry) set(path string, offset uint64, c *mocache.Cache, skipMemWrite bool) error {
	if c == nil || skipMemWrite {
		return e.setCachedDataWithKey(path, offset, DefaultCacheDataAllocator)
	}
	if err := e.setCachedDataWithKey(path, offset, c); err != nil {
		if e.CachedData != nil {
			mocache.Free(e.CachedData.GetValue())
		}
		return err
	}
	if e.CachedData != nil {
		e.CachedData = c.Set(path, offset, e.CachedData.GetValue())
	}
	return nil
}

func (e *IOEntry) setCachedDataWithKey(path string, offset uint64, alloc CacheDataAllocator) error {
	if e.ToCacheData == nil {
		return nil
	}
	if len(e.Data) == 0 {
		return nil
	}
	bs, err := e.ToCacheData(bytes.NewReader(e.Data), e.Data, path, offset, alloc)
	if err != nil {
		return err
	}
	e.CachedData = bs
	return nil
}

func (e *IOEntry) setCachedData() error {
	if e.ToCacheData == nil {
		return nil
	}
	if len(e.Data) == 0 {
		return nil
	}
	bs, err := e.ToCacheData(bytes.NewReader(e.Data), e.Data, "", 0, DefaultCacheDataAllocator)
	if err != nil {
		return err
	}
	e.CachedData = bs
	return nil
}

func (e *IOEntry) ReadFromOSFile(file *os.File) error {
	r := io.LimitReader(file, e.Size)

	if len(e.Data) < int(e.Size) {
		e.Data = make([]byte, e.Size)
	}

	n, err := io.ReadFull(r, e.Data)
	if err != nil {
		return err
	}
	if n != int(e.Size) {
		return io.ErrUnexpectedEOF
	}

	if e.WriterForRead != nil {
		if _, err := e.WriterForRead.Write(e.Data); err != nil {
			return err
		}
	}
	if e.ReadCloserForRead != nil {
		*e.ReadCloserForRead = io.NopCloser(bytes.NewReader(e.Data))
	}
	if err := e.setCachedData(); err != nil {
		return err
	}

	e.done = true

	return nil
}

func CacheOriginalData(r io.Reader, data []byte, _ string, _ uint64, allocator CacheDataAllocator) (cacheData mocache.CacheData, err error) {
	if len(data) == 0 {
		data, err = io.ReadAll(r)
		if err != nil {
			return
		}
	}
	cacheData = allocator.Alloc(len(data))
	copy(cacheData.Get(), data)
	return
}
