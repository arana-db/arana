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

package function

import (
	"context"
	"fmt"
)

import (
	"github.com/blang/semver"

	"github.com/docker/go-units"

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

const FuncFormatBytes = "FORMAT_BYTES"

var _ proto.VersionedFunc = (*formatBytesFunc)(nil)

var (
	_decimal1024                 = decimal.NewFromInt(1024)
	_formatBytesFuncVersionRange = semver.MustParseRange(">=8.0.16")
	_binaryAbbrs                 = []string{"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"}
	_zeroBytesStr                = proto.NewValueString(fmt.Sprintf("0 %s", _binaryAbbrs[0]))
)

func init() {
	// https://dev.mysql.com/doc/refman/8.0/en/performance-schema-functions.html#function_format-bytes
	proto.RegisterFunc(FuncFormatBytes, formatBytesFunc{})
}

type formatBytesFunc struct{}

func (ff formatBytesFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	d, err := val.Decimal()
	if err != nil || d.IsZero() {
		return _zeroBytesStr, nil
	}

	return proto.NewValueString(ff.formatUnit(d)), nil
}

func (ff formatBytesFunc) NumInput() int {
	return 1
}

func (ff formatBytesFunc) Versions() semver.Range {
	return _formatBytesFuncVersionRange
}

func (ff formatBytesFunc) formatUnit(size decimal.Decimal) string {
	size = size.Truncate(0)

	if size.Abs().LessThan(_decimal1024) {
		return units.CustomSize("%v %s", size.InexactFloat64(), 1024.0, _binaryAbbrs)
	}
	return units.CustomSize("%.2f %s", size.InexactFloat64(), 1024.0, _binaryAbbrs)
}
