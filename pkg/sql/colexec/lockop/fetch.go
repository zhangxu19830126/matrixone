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

package lockop

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func getFetchRowsFunc(t types.Type) fetchRowsFunc {
	switch t.Oid {
	case types.T_bool:
		return fetchBoolRows
	case types.T_int8:
		return fetchInt8Rows
	case types.T_int16:
		return fetchInt16Rows
	case types.T_int32:
		return fetchInt32Rows
	case types.T_int64:
		return fetchInt64Rows
	case types.T_uint8:
		return fetchUint8Rows
	case types.T_uint16:
		return fetchUint16Rows
	case types.T_uint32:
		return fetchUint32Rows
	case types.T_uint64:
		return fetchUint64Rows
	case types.T_float32:
		return fetchFloat32Rows
	case types.T_float64:
		return fetchFloat64Rows
	case types.T_date:
		return fetchDateRows
	case types.T_time:
		return fetchTimeRows
	case types.T_datetime:
		return fetchDateTimeRows
	case types.T_timestamp:
		return fetchTimestampRows
	case types.T_decimal64:
		return fetchDecimal64Rows
	case types.T_decimal128:
		return fetchDecimal128Rows
	case types.T_uuid:
		return fetchUUIDRows
	case types.T_TS:
		return fetchTSRows
	case types.T_Rowid:
		return fetchRowIDRows
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return fetchBytesRows
	default:
		panic(fmt.Sprintf("not support for %s", t.String()))
	}
}

// switch vec.GetType().Oid {
// case types.T_bool:
// 	col := vector.MustFixedCol[bool](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_int8:
// 	col := vector.MustFixedCol[int8](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_int16:
// 	col := vector.MustFixedCol[int16](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_int32:
// 	col := vector.MustFixedCol[int32](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_int64:
// 	col := vector.MustFixedCol[int64](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_uint8:
// 	col := vector.MustFixedCol[uint8](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_uint16:
// 	col := vector.MustFixedCol[uint16](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_uint32:
// 	col := vector.MustFixedCol[uint32](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_uint64:
// 	col := vector.MustFixedCol[uint64](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_float32:
// 	col := vector.MustFixedCol[float32](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_float64:
// 	col := vector.MustFixedCol[float64](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_date:
// 	col := vector.MustFixedCol[types.Date](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_time:
// 	col := vector.MustFixedCol[types.Time](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_datetime:
// 	col := vector.MustFixedCol[types.Datetime](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_timestamp:
// 	col := vector.MustFixedCol[types.Timestamp](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_decimal64:
// 	col := vector.MustFixedCol[types.Decimal64](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_decimal128:
// 	col := vector.MustFixedCol[types.Decimal128](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_uuid:
// 	col := vector.MustFixedCol[types.Uuid](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_TS:
// 	col := vector.MustFixedCol[types.TS](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_Rowid:
// 	col := vector.MustFixedCol[types.Rowid](vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_json, types.T_text:
// 	col := vector.MustBytesCol(vec)
// 	for j := 0; j < vec.Length(); j++ {
// 		rows[j][i] = col[j]
// 	}
// }

func fetchBoolRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	cols := vector.MustFixedCol[int](vec)
	col2s := vector.MustFixedCol[types.Varlena](vec)
	data := vec.GetArea()
	col2s[0].GetByteSlice(data)
	for _, col := range cols {

	}

	return nil
}

func fetchInt8Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchInt16Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchInt32Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchInt64Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchUint8Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchUint16Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchUint32Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchUint64Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchFloat32Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchFloat64Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchDateRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchTimeRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchDateTimeRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchTimestampRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchDecimal64Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchDecimal128Rows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchUUIDRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchTSRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchRowIDRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}

func fetchBytesRows(
	vec *vector.Vector,
	rows [][]byte) [][]byte {
	return nil
}
