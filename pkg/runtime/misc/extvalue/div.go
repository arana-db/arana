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

package extvalue

import (
	"context"
	"errors"
	"math"
)

import (
	"github.com/shopspring/decimal"
)

type favContextKey string

type Div struct {
	ops int32
	// divScale is number of continuous division operations; this value will be available of all layers
	divScale int32
	// leftmostScale is a length of scale of the leftmost value in continuous division operation
	leftmostScale               int32
	curIntermediatePrecisionInc int
}

func NewDiv() Div {
	d := Div{0, 0, 0, 0}
	return d
}

// '9 scales' are added for every non-integer divider(right side).
const divIntermediatePrecisionInc = 9
const ctxPrecisionKey = favContextKey("key")

func isIntOr1(val decimal.Decimal) bool {
	if val.Equal(decimal.NewFromInt(1)) {
		return true
	}
	if val.Equal(decimal.NewFromInt(-1)) {
		return true
	}
	if val.Equal(decimal.NewFromInt(val.IntPart())) {
		return true
	}
	return false
}

func (d *Div) DoDiv(ctx context.Context, lval, rval decimal.Decimal) (decimal.Decimal, error) {
	if rval.Equal(decimal.NewFromInt(0)) {
		return decimal.NewFromInt(0), errors.New("division by 0")
	}

	v, ok := ctx.Value(ctxPrecisionKey).(int32)

	if !ok {
		return decimal.Decimal{}, errors.New("context key Precision value is nil")
	}

	if d.curIntermediatePrecisionInc == 0 {
		// if the first dividend / the leftmost value is non int value,
		// then curIntermediatePrecisionInc gets additional increment per every 9 scales
		if !isIntOr1(lval) {
			d.curIntermediatePrecisionInc = int(math.Ceil(float64(lval.Exponent()*-1) / float64(divIntermediatePrecisionInc)))
		} else {

			divRes := lval.DivRound(rval, v+2)

			return divRes.Truncate(v), nil
		}
	}

	storedScale := int32(d.curIntermediatePrecisionInc * divIntermediatePrecisionInc)
	l := lval.Truncate(storedScale)
	r := rval.Truncate(storedScale)

	// give it buffer of 2 additional scale to avoid the result to be rounded
	divRes := l.DivRound(r, storedScale+2)

	// round
	return divRes.RoundBank(v), nil
}
