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
	"bytes"
	"math"
	"time"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/util/bytesconv"
)

const _day = time.Hour * 24

type ValueWriter interface {
	WriteString(s string) (int64, error)
	WriteUint64(v uint64) (int64, error)
	WriteUint32(v uint32) (int64, error)
	WriteUint16(v uint16) (int64, error)
	WriteUint8(v uint8) (int64, error)
	WriteFloat64(f float64) (int64, error)
	WriteFloat32(f float32) (int64, error)
	WriteDate(t time.Time) (int64, error)
	WriteDateTime(t time.Time) (int64, error)
	WriteDuration(d time.Duration) (int64, error)
}

var _ ValueWriter = (*BinaryValueWriter)(nil)

type BinaryValueWriter bytes.Buffer

func (bw *BinaryValueWriter) Bytes() []byte {
	return (*bytes.Buffer)(bw).Bytes()
}

func (bw *BinaryValueWriter) Reset() {
	(*bytes.Buffer)(bw).Reset()
}

func (bw *BinaryValueWriter) WriteDuration(d time.Duration) (n int64, err error) {
	b := bw.buffer()
	if d == 0 {
		if err = b.WriteByte(0); err != nil {
			return
		}
		n++
		return
	}

	var (
		length uint8
		neg    byte
	)

	if d < 0 {
		neg = 1
		d *= -1
	}

	var (
		days    = uint32(d / _day)
		hours   = uint8(d % _day / time.Hour)
		minutes = uint8(d % time.Hour / time.Minute)
		secs    = uint8(d % time.Minute / time.Second)
		msec    = uint32(d % time.Millisecond / time.Microsecond)
	)

	if msec == 0 {
		length = 8
	} else {
		length = 12
	}

	if err = b.WriteByte(length); err != nil {
		return
	}
	if err = b.WriteByte(neg); err != nil {
		return
	}
	if _, err = bw.WriteUint32(days); err != nil {
		return
	}
	if err = b.WriteByte(hours); err != nil {
		return
	}
	if err = b.WriteByte(minutes); err != nil {
		return
	}
	if err = b.WriteByte(secs); err != nil {
		return
	}

	if length == 12 {
		if _, err = bw.WriteUint32(msec); err != nil {
			return
		}
	}

	n = int64(length) + 1

	return
}

func (bw *BinaryValueWriter) WriteDate(t time.Time) (int64, error) {
	if t.IsZero() {
		return bw.writeTimeN(t, 0)
	}
	return bw.writeTimeN(t, 4)
}

func (bw *BinaryValueWriter) WriteDateTime(t time.Time) (int64, error) {
	if t.IsZero() {
		return bw.writeTimeN(t, 0)
	}

	if t.Nanosecond()/int(time.Microsecond) == 0 {
		return bw.writeTimeN(t, 7)
	}

	return bw.writeTimeN(t, 11)
}

func (bw *BinaryValueWriter) writeTimeN(t time.Time, l int) (n int64, err error) {
	var (
		year   = uint16(t.Year())
		month  = uint8(t.Month())
		day    = uint8(t.Day())
		hour   = uint8(t.Hour())
		minute = uint8(t.Minute())
		sec    = uint8(t.Second())
	)

	if _, err = bw.WriteUint8(byte(l)); err != nil {
		return
	}

	switch l {
	case 0:
	case 4:
		if _, err = bw.WriteUint16(year); err != nil {
			return
		}
		if _, err = bw.WriteUint8(month); err != nil {
			return
		}
		if _, err = bw.WriteUint8(day); err != nil {
			return
		}
	case 7:
		if _, err = bw.WriteUint16(year); err != nil {
			return
		}
		if _, err = bw.WriteUint8(month); err != nil {
			return
		}
		if _, err = bw.WriteUint8(day); err != nil {
			return
		}
		if _, err = bw.WriteUint8(hour); err != nil {
			return
		}
		if _, err = bw.WriteUint8(minute); err != nil {
			return
		}
		if _, err = bw.WriteUint8(sec); err != nil {
			return
		}
	case 11:
		if _, err = bw.WriteUint16(year); err != nil {
			return
		}
		if _, err = bw.WriteUint8(month); err != nil {
			return
		}
		if _, err = bw.WriteUint8(day); err != nil {
			return
		}
		if _, err = bw.WriteUint8(hour); err != nil {
			return
		}
		if _, err = bw.WriteUint8(minute); err != nil {
			return
		}
		if _, err = bw.WriteUint8(sec); err != nil {
			return
		}
		msec := uint32(int64(t.Nanosecond()) / int64(time.Microsecond))
		if _, err = bw.WriteUint32(msec); err != nil {
			return
		}
	default:
		err = errors.Errorf("illegal time length %d", l)
		return
	}

	n = int64(l) + 1
	return
}

func (bw *BinaryValueWriter) WriteFloat32(f float32) (int64, error) {
	n := math.Float32bits(f)
	return bw.WriteUint32(n)
}

func (bw *BinaryValueWriter) WriteFloat64(f float64) (int64, error) {
	n := math.Float64bits(f)
	return bw.WriteUint64(n)
}

func (bw *BinaryValueWriter) WriteUint8(v uint8) (int64, error) {
	if err := bw.buffer().WriteByte(v); err != nil {
		return 0, err
	}
	return 1, nil
}

func (bw *BinaryValueWriter) WriteString(s string) (int64, error) {
	return bw.writeLenEncString(s)
}

func (bw *BinaryValueWriter) WriteUint16(v uint16) (n int64, err error) {
	if err = bw.buffer().WriteByte(byte(v)); err != nil {
		return
	}
	n++
	if err = bw.buffer().WriteByte(byte(v >> 8)); err != nil {
		return
	}
	n++
	return
}

func (bw *BinaryValueWriter) WriteUint32(v uint32) (n int64, err error) {
	b := bw.buffer()
	if err = b.WriteByte(byte(v)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	n += 4
	return
}

func (bw *BinaryValueWriter) WriteUint64(v uint64) (n int64, err error) {
	b := bw.buffer()
	if err = b.WriteByte(byte(v)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 32)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 40)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 48)); err != nil {
		return
	}
	if err = b.WriteByte(byte(v >> 56)); err != nil {
		return
	}
	n += 8
	return
}

func (bw *BinaryValueWriter) writeLenEncString(s string) (n int64, err error) {
	var wrote int64

	if wrote, err = bw.writeLenEncInt(uint64(len(s))); err != nil {
		return
	}
	n += wrote

	if wrote, err = bw.writeEOFString(s); err != nil {
		return
	}
	n += wrote

	return
}

func (bw *BinaryValueWriter) writeEOFString(s string) (n int64, err error) {
	var wrote int
	if wrote, err = bw.buffer().Write(bytesconv.StringToBytes(s)); err != nil {
		return
	}
	n += int64(wrote)
	return
}

func (bw *BinaryValueWriter) writeLenEncInt(i uint64) (n int64, err error) {
	b := bw.buffer()
	switch {
	case i < 251:
		if err = b.WriteByte(byte(i)); err == nil {
			n++
		}
	case i < 1<<16:
		if err = b.WriteByte(0xfc); err != nil {
			return
		}
		if err = b.WriteByte(byte(i)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 8)); err != nil {
			return
		}
		n += 3
	case i < 1<<24:
		if err = b.WriteByte(0xfd); err != nil {
			return
		}
		if err = b.WriteByte(byte(i)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 8)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 16)); err != nil {
			return
		}
		n += 4
	default:
		if err = b.WriteByte(0xfe); err != nil {
			return
		}
		if err = b.WriteByte(byte(i)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 8)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 16)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 24)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 32)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 40)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 48)); err != nil {
			return
		}
		if err = b.WriteByte(byte(i >> 56)); err != nil {
			return
		}
		n += 9
	}

	return
}

func (bw *BinaryValueWriter) buffer() *bytes.Buffer {
	return (*bytes.Buffer)(bw)
}
