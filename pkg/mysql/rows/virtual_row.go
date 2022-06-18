/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rows

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	perrors "github.com/pkg/errors"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/bufferpool"
	"github.com/arana-db/arana/pkg/util/bytesconv"
)

var (
	_ VirtualRow = (*binaryVirtualRow)(nil)
	_ VirtualRow = (*textVirtualRow)(nil)
)

var errScanDiffSize = errors.New("cannot scan to dest with different length")

// IsScanDiffSizeErr returns true if target error is caused by scanning values with different size.
func IsScanDiffSizeErr(err error) bool {
	return perrors.Is(err, errScanDiffSize)
}

// VirtualRow represents virtual row which is created manually.
type VirtualRow interface {
	proto.KeyedRow
	// Values returns all values of current row.
	Values() []proto.Value
}

type baseVirtualRow struct {
	lengthOnce sync.Once
	length     int64

	fields []proto.Field
	cells  []proto.Value
}

func (b *baseVirtualRow) Get(name string) (proto.Value, error) {
	idx := -1
	for i, it := range b.fields {
		if it.Name() == name {
			idx = i
			break
		}
	}

	if idx == -1 {
		return nil, perrors.Errorf("no such field '%s' found", name)
	}

	return b.cells[idx], nil
}

func (b *baseVirtualRow) Scan(dest []proto.Value) error {
	if len(dest) != len(b.cells) {
		return perrors.WithStack(errScanDiffSize)
	}
	copy(dest, b.cells)
	return nil
}

func (b *baseVirtualRow) Values() []proto.Value {
	return b.cells
}

type binaryVirtualRow baseVirtualRow

func (vi *binaryVirtualRow) Get(name string) (proto.Value, error) {
	return (*baseVirtualRow)(vi).Get(name)
}

func (vi *binaryVirtualRow) Fields() []proto.Field {
	return vi.fields
}

func (vi *binaryVirtualRow) Values() []proto.Value {
	return (*baseVirtualRow)(vi).Values()
}

func (vi *binaryVirtualRow) IsBinary() bool {
	return true
}

func (vi *binaryVirtualRow) WriteTo(w io.Writer) (n int64, err error) {
	// https://dev.mysql.com/doc/internals/en/null-bitmap.html

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	const offset = 2
	nullBitmapLen := (len(vi.cells) + 7 + offset) >> 3

	b := bufferpool.Get()
	defer bufferpool.Put(b)

	b.Grow(1 + nullBitmapLen)

	b.WriteByte(0x00)
	for i := 0; i < nullBitmapLen; i++ {
		b.WriteByte(0x00)
	}

	bw := (*BinaryValueWriter)(b)

	var next proto.Value
	for i := 0; i < len(vi.cells); i++ {
		next = vi.cells[i]
		if next == nil {
			var (
				bytePos = (i+2)/8 + 1
				bitPos  = (i + 2) % 8
			)
			b.Bytes()[bytePos] |= 1 << bitPos
		}

		switch val := next.(type) {
		case uint64:
			_, err = bw.WriteUint64(val)
		case int64:
			_, err = bw.WriteUint64(uint64(val))
		case uint32:
			_, err = bw.WriteUint32(val)
		case int32:
			_, err = bw.WriteUint32(uint32(val))
		case uint16:
			_, err = bw.WriteUint16(val)
		case int16:
			_, err = bw.WriteUint16(uint16(val))
		case string:
			_, err = bw.WriteString(val)
		case float32:
			_, err = bw.WriteFloat32(val)
		case float64:
			_, err = bw.WriteFloat64(val)
		case time.Time:
			_, err = bw.WriteDateTime(val)
		case time.Duration:
			_, err = bw.WriteDuration(val)
		case *gxbig.Decimal:
			_, err = bw.WriteString(val.String())
		default:
			err = perrors.Errorf("unknown value type %T", val)
		}

		if err != nil {
			return
		}
	}

	return b.WriteTo(w)
}

func (vi *binaryVirtualRow) Length() int {
	vi.lengthOnce.Do(func() {
		n, _ := vi.WriteTo(io.Discard)
		vi.length = n
	})
	return int(vi.length)
}

func (vi *binaryVirtualRow) Scan(dest []proto.Value) error {
	return (*baseVirtualRow)(vi).Scan(dest)
}

type textVirtualRow baseVirtualRow

func (t *textVirtualRow) Get(name string) (proto.Value, error) {
	return (*baseVirtualRow)(t).Get(name)
}

func (t *textVirtualRow) Fields() []proto.Field {
	return t.fields
}

func (t *textVirtualRow) WriteTo(w io.Writer) (n int64, err error) {
	bf := bufferpool.Get()
	defer bufferpool.Put(bf)

	for i := 0; i < len(t.fields); i++ {
		var (
			field = t.fields[i].(*mysql.Field)
			cell  = t.cells[i]
		)

		if cell == nil {
			if err = bf.WriteByte(0xfb); err != nil {
				err = perrors.WithStack(err)
				return
			}
			continue
		}

		switch field.FieldType() {
		case consts.FieldTypeTimestamp, consts.FieldTypeDateTime, consts.FieldTypeDate, consts.FieldTypeNewDate:
			var b []byte
			if b, err = mysql.AppendDateTime(b, t.cells[i].(time.Time)); err != nil {
				err = perrors.WithStack(err)
				return
			}
			if _, err = (*BinaryValueWriter)(bf).WriteString(bytesconv.BytesToString(b)); err != nil {
				err = perrors.WithStack(err)
			}
		default:
			var s string
			switch val := cell.(type) {
			case fmt.Stringer:
				s = val.String()
			default:
				s = fmt.Sprint(cell)
			}
			if _, err = (*BinaryValueWriter)(bf).WriteString(s); err != nil {
				err = perrors.WithStack(err)
				return
			}
		}
	}

	n, err = bf.WriteTo(w)

	return
}

func (t *textVirtualRow) IsBinary() bool {
	return false
}

func (t *textVirtualRow) Length() int {
	t.lengthOnce.Do(func() {
		t.length, _ = t.WriteTo(io.Discard)
	})
	return int(t.length)
}

func (t *textVirtualRow) Scan(dest []proto.Value) error {
	return (*baseVirtualRow)(t).Scan(dest)
}

func (t *textVirtualRow) Values() []proto.Value {
	return (*baseVirtualRow)(t).Values()
}

// NewBinaryVirtualRow creates a virtual row with binary-protocol.
func NewBinaryVirtualRow(fields []proto.Field, cells []proto.Value) VirtualRow {
	return (*binaryVirtualRow)(newBaseVirtualRow(fields, cells))
}

// NewTextVirtualRow creates a virtual row with text-protocol.
func NewTextVirtualRow(fields []proto.Field, cells []proto.Value) VirtualRow {
	return (*textVirtualRow)(newBaseVirtualRow(fields, cells))
}

func newBaseVirtualRow(fields []proto.Field, cells []proto.Value) *baseVirtualRow {
	if len(fields) != len(cells) {
		panic(fmt.Sprintf("the lengths of fields and cells are doesn't match!"))
	}
	return &baseVirtualRow{
		fields: fields,
		cells:  cells,
	}
}
