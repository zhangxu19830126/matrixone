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

package objectio

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/mocache"
)

type CacheConstructor = func(r io.Reader, buf []byte, path string, offset uint64, allocator fileservice.CacheDataAllocator) (mocache.CacheData, error)
type CacheConstructorFactory = func(size int64, algo uint8) CacheConstructor

// use this to replace all other constructors
func constructorFactory(size int64, algo uint8) CacheConstructor {
	return func(reader io.Reader, data []byte, path string, offset uint64, allocator fileservice.CacheDataAllocator) (cacheData mocache.CacheData, err error) {
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return
			}
		}

		// no compress
		if algo == compress.None {
			cacheData = allocator.AllocWithKey(path, offset, len(data))
			copy(cacheData.Get(), data)
			return cacheData, nil
		}

		// lz4 compress
		decompressed := allocator.AllocWithKey(path, offset, int(size))
		bs, err := compress.Decompress(data, decompressed.Get(), compress.Lz4)
		if err != nil {
			return
		}
		decompressed = decompressed.Truncate(len(bs))
		return decompressed, nil
	}
}

func Decode(buf []byte) (v any, err error) {
	header := DecodeIOEntryHeader(buf)
	codec := GetIOEntryCodec(*header)
	if codec.NoUnmarshal() {
		return buf[IOEntryHeaderSize:], nil
	}
	v, err = codec.Decode(buf[IOEntryHeaderSize:])
	if err != nil {
		return nil, err
	}
	return v, nil
}
