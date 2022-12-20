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
	"regexp"
	"strconv"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncCastDate is  https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastDate = "CAST_DATE"

var _ proto.Func = (*castDateFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastDate, castDateFunc{})
}

type castDateFunc struct{}

func (a castDateFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	// expr
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	dateArgs := fmt.Sprint(val)
	if strings.Compare(dateArgs, "") == 0 {
		return a.DefaultDateValue(), nil
	}

	// format - YY-MM-DD, YYYY-MM-DD
	pat := "^\\d{1,4}[~!@#$%^&*_+\\-=:;,.|?/]{1}\\d{1,2}[~!@#$%^&*_+\\-=:;,.|?/]{1}\\d{1,2}$"
	match, err := regexp.MatchString(pat, dateArgs)
	if match && err == nil {
		dateLen := len(dateArgs)
		dateDayStr := string(dateArgs[dateLen-1 : dateLen])
		dateLeft := string(dateArgs[0 : dateLen-2])
		if a.IsDigitalValid(string(dateArgs[dateLen-2])) {
			dateDayStr = string(dateArgs[dateLen-2 : dateLen])
			dateLeft = string(dateArgs[0 : dateLen-3])
		}
		dateArgs = dateLeft

		dateLen = len(dateArgs)
		dateMonthStr := string(dateArgs[dateLen-1 : dateLen])
		dateLeft = string(dateArgs[0 : dateLen-2])
		if a.IsDigitalValid(string(dateArgs[dateLen-2])) {
			dateMonthStr = string(dateArgs[dateLen-2 : dateLen])
			dateLeft = string(dateArgs[0 : dateLen-3])
		}
		dateYearStr := dateLeft

		dateYear, _ := strconv.Atoi(dateYearStr)
		dateYear = a.amend4DigtalYear(dateYear)
		dateMonth, _ := strconv.Atoi(dateMonthStr)
		dateDay, _ := strconv.Atoi(dateDayStr)

		if a.IsYearValid(dateYear) && a.IsMonthValid(dateMonth) && a.IsDayValid(dateYear, dateMonth, dateDay) {
			dateStr := a.DateOutput(dateYear, dateMonth, dateDay)
			return dateStr, nil
		} else {
			return a.DefaultDateValue(), nil
		}
	}
	// format - YYYYMMDD, YYMMDD
	pat = "^\\d{5,8}$"
	match, err = regexp.MatchString(pat, dateArgs)
	if match && err == nil {
		dateLen := len(dateArgs)
		dateDayStr := string(dateArgs[dateLen-2 : dateLen])
		dateLeft := string(dateArgs[0 : dateLen-2])
		dateArgs = dateLeft

		dateLen = len(dateArgs)
		dateMonthStr := string(dateArgs[dateLen-2 : dateLen])
		dateLeft = string(dateArgs[0 : dateLen-2])
		dateYearStr := dateLeft

		dateYear, _ := strconv.Atoi(dateYearStr)
		dateYear = a.amend4DigtalYear(dateYear)
		dateMonth, _ := strconv.Atoi(dateMonthStr)
		dateDay, _ := strconv.Atoi(dateDayStr)

		if a.IsYearValid(dateYear) && a.IsMonthValid(dateMonth) && a.IsDayValid(dateYear, dateMonth, dateDay) {
			dateStr := a.DateOutput(dateYear, dateMonth, dateDay)
			return dateStr, nil
		} else {
			return a.DefaultDateValue(), nil
		}
	}

	return a.DefaultDateValue(), nil
}

func (a castDateFunc) NumInput() int {
	return 1
}

func (a castDateFunc) DateOutput(year, month, day int) proto.Value {
	dayStr := fmt.Sprint(day)
	if day < 10 {
		dayStr = "0" + dayStr
	}
	monthStr := fmt.Sprint(month)
	if month < 10 {
		monthStr = "0" + monthStr
	}
	yearStr := fmt.Sprint(year)
	if year >= 100 && year <= 999 {
		yearStr = "0" + yearStr
	}

	dateStr := yearStr + "-" + monthStr + "-" + dayStr
	return proto.NewValueString(dateStr)
}

func (a castDateFunc) amend4DigtalYear(year int) int {
	if year >= 0 && year <= 69 {
		year += 2000
	}
	if year >= 70 && year <= 99 {
		year += 1900
	}
	return year
}

func (a castDateFunc) IsDigitalValid(data string) bool {
	if len(data) == 1 && data >= "0" && data <= "9" {
		return true
	}
	return false
}

func (a castDateFunc) IsYearValid(year int) bool {
	if year >= 100 && year <= 9999 {
		return true
	}
	return false
}

func (a castDateFunc) IsMonthValid(month int) bool {
	if month >= 1 && month <= 12 {
		return true
	}
	return false
}

func (a castDateFunc) IsDayValid(year, month, day int) bool {
	if month == 1 || month == 3 || month == 5 || month == 7 ||
		month == 8 || month == 10 || month == 12 {
		if day >= 1 && day <= 31 {
			return true
		}
		return false
	}
	if month == 4 || month == 6 || month == 9 || month == 11 {
		if day >= 1 && day <= 30 {
			return true
		}
		return false
	}
	if month == 2 {
		if (year%100 == 0 && year%400 == 0) ||
			(year%100 > 0 && year%4 == 0) {
			if day >= 1 && day <= 29 {
				return true
			}
			return false
		} else {
			if day >= 1 && day <= 28 {
				return true
			}
			return false
		}
	}
	return false
}

func (a castDateFunc) MaxDateValue() proto.Value {
	return proto.NewValueString("9999-12-31")
}

func (a castDateFunc) MinDateValue() proto.Value {
	return proto.NewValueString("0000-01-01")
}

func (a castDateFunc) DefaultDateValue() proto.Value {
	return proto.NewValueString("0000-00-00")
}
