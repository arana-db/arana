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

package math

import (
	"fmt"
)

import gxbig "github.com/dubbogo/gost/math/big"

func ToDecimal(value interface{}) *gxbig.Decimal {
	var d *gxbig.Decimal
	switch val := value.(type) {
	case *gxbig.Decimal:
		d = val
	case float64:
		d, _ = gxbig.NewDecFromFloat(val)
	case float32:
		d, _ = gxbig.NewDecFromFloat(float64(val))
	case int:
		d = gxbig.NewDecFromInt(int64(val))
	case int64:
		d = gxbig.NewDecFromInt(val)
	case int32:
		d = gxbig.NewDecFromInt(int64(val))
	case int16:
		d = gxbig.NewDecFromInt(int64(val))
	case int8:
		d = gxbig.NewDecFromInt(int64(val))
	case uint8:
		d = gxbig.NewDecFromUint(uint64(val))
	case uint16:
		d = gxbig.NewDecFromUint(uint64(val))
	case uint32:
		d = gxbig.NewDecFromUint(uint64(val))
	case uint64:
		d = gxbig.NewDecFromUint(val)
	case uint:
		d = gxbig.NewDecFromUint(uint64(val))
	default:
		var err error
		if d, err = gxbig.NewDecFromString(fmt.Sprint(val)); err == nil {
			return d
		}
		d, _ = gxbig.NewDecFromString("0.0")
	}
	return d
}

func IsZero(d *gxbig.Decimal) bool {
	if d != nil {
		return d.IsZero()
	}
	return false
}
