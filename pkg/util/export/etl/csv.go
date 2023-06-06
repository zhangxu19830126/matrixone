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

package etl

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

const initedSize = mpool.MB

var _ table.RowWriter = (*CSVWriter)(nil)

type CSVWriter struct {
	ctx    context.Context
	buf    *bytes.Buffer
	writer io.StringWriter
}

func NewCSVWriter(ctx context.Context, writer io.StringWriter) *CSVWriter {
	w := &CSVWriter{
		ctx:    ctx,
		buf:    bytes.NewBuffer(make([]byte, 0, initedSize)),
		writer: writer,
	}
	return w
}

func (w *CSVWriter) WriteRow(row *table.Row) error {
	return w.WriteStrings(row.ToStrings())
}

func (w *CSVWriter) WriteStrings(record []string) error {
	writer := csv.NewWriter(w.buf)
	err := writer.Write(record)
	if err != nil {
		return moerr.ConvertGoError(w.ctx, err)
	}
	writer.Flush()
	return nil
}

func (w *CSVWriter) GetContent() string {
	return w.buf.String()
}

func (w *CSVWriter) FlushAndClose() (int, error) {
	if len(w.buf.String()) == 0 {
		return 0, nil
	}
	n, err := w.writer.WriteString(util.UnsafeBytesToString(w.buf.Bytes()))
	if err != nil {
		return 0, err
	}
	w.writer = nil
	w.buf = nil
	return n, nil
}

type FSWriter struct {
	ctx context.Context         // New args
	fs  fileservice.FileService // New args
	// filepath
	filepath string // see WithFilePath or auto generated by NewFSWriter

	mux sync.Mutex

	offset int // see Write, should not have size bigger than 2GB
}

type FSWriterOption func(*FSWriter)

func (f FSWriterOption) Apply(w *FSWriter) {
	f(w)
}

func NewFSWriter(ctx context.Context, fs fileservice.FileService, opts ...FSWriterOption) *FSWriter {
	w := &FSWriter{
		ctx: ctx,
		fs:  fs,
	}
	for _, o := range opts {
		o.Apply(w)
	}
	if len(w.filepath) == 0 {
		panic("filepath is Empty")
	}
	return w
}

func WithFilePath(filepath string) FSWriterOption {
	return FSWriterOption(func(w *FSWriter) {
		w.filepath = filepath
	})
}

// Write implement io.Writer, Please execute in series
func (w *FSWriter) Write(p []byte) (n int, err error) {
	w.mux.Lock()
	defer w.mux.Unlock()
	n = len(p)
	mkdirTried := false
mkdirRetry:
	if err = w.fs.Write(w.ctx, fileservice.IOVector{
		// like: etl:store/system/filename.csv
		FilePath: w.filepath,
		Entries: []fileservice.IOEntry{
			{
				Offset: int64(w.offset),
				Size:   int64(n),
				Data:   p,
			},
		},
	}); err == nil {
		w.offset += n
	} else if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) && !mkdirTried {
		mkdirTried = true
		goto mkdirRetry
	}
	// XXX Why call this?
	// _ = errors.WithContext(w.ctx, err)
	return
}

// WriteString implement io.StringWriter
func (w *FSWriter) WriteString(s string) (n int, err error) {
	var b = util.UnsafeStringToBytes(s)
	return w.Write(b)
}
