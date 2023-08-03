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

var (
	DateSep          = `[~!@#$%^&*_+=:;,.|/?\(\)\[\]\{\}\-\\]+`
	_dateReplace     = regexp.MustCompile(DateSep)
	_dateMatchString = regexp.MustCompile(fmt.Sprintf(`^\d{1,4}%s\d{1,2}%s\d{1,2}$`, DateSep, DateSep))
	_dateMatchInt    = regexp.MustCompile(`^\d{5,8}$`)
)

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
	match := _dateMatchString.MatchString(dateArgs)
	if match {
		dateArgsReplace := _dateReplace.ReplaceAllStringFunc(dateArgs, func(s string) string { return "-" })
		dateYear, dateMonth, dateDay := a.splitDateWithSep(dateArgsReplace)

		if a.IsYearValid(dateYear) && a.IsMonthValid(dateMonth) && a.IsDayValid(dateYear, dateMonth, dateDay) {
			dateStr := a.DateOutput(dateYear, dateMonth, dateDay)
			return dateStr, nil
		}
	}
	// format - YYYYMMDD, YYMMDD
	match = _dateMatchInt.MatchString(dateArgs)
	if match {
		dateYear, dateMonth, dateDay := a.splitDateWithoutSep(dateArgs)

		if a.IsYearValid(dateYear) && a.IsMonthValid(dateMonth) && a.IsDayValid(dateYear, dateMonth, dateDay) {
			dateStr := a.DateOutput(dateYear, dateMonth, dateDay)
			return dateStr, nil
		}
	}

	return a.DefaultDateValue(), nil
}

func (a castDateFunc) NumInput() int {
	return 1
}

func (a castDateFunc) DateOutput(year, month, day int) proto.Value {
	var sb strings.Builder

	yearStr := strconv.FormatInt(int64(year), 10)
	if year >= 100 && year <= 999 {
		sb.WriteString("0")
	}
	sb.WriteString(yearStr)
	sb.WriteString("-")
	monthStr := strconv.FormatInt(int64(month), 10)
	if month < 10 {
		sb.WriteString("0")
	}
	sb.WriteString(monthStr)
	sb.WriteString("-")
	dayStr := strconv.FormatInt(int64(day), 10)
	if day < 10 {
		sb.WriteString("0")
	}
	sb.WriteString(dayStr)

	return proto.NewValueString(sb.String())
}

func (a castDateFunc) splitDateWithSep(dateArgs string) (year, month, day int) {
	dateLen := len(dateArgs)
	dateDayStr := dateArgs[dateLen-1 : dateLen]
	dateLeft := dateArgs[0 : dateLen-2]
	if a.IsDigitalValid(string(dateArgs[dateLen-2])) {
		dateDayStr = dateArgs[dateLen-2 : dateLen]
		dateLeft = dateArgs[0 : dateLen-3]
	}
	dateArgs = dateLeft

	dateLen = len(dateArgs)
	dateMonthStr := dateArgs[dateLen-1 : dateLen]
	dateLeft = dateArgs[0 : dateLen-2]
	if a.IsDigitalValid(string(dateArgs[dateLen-2])) {
		dateMonthStr = dateArgs[dateLen-2 : dateLen]
		dateLeft = dateArgs[0 : dateLen-3]
	}
	dateYearStr := dateLeft

	dateYear, _ := strconv.Atoi(dateYearStr)
	dateYear = a.amend4DigitalYear(dateYear)
	dateMonth, _ := strconv.Atoi(dateMonthStr)
	dateDay, _ := strconv.Atoi(dateDayStr)

	return dateYear, dateMonth, dateDay
}

func (a castDateFunc) splitDateWithoutSep(dateArgs string) (year, month, day int) {
	dateLen := len(dateArgs)
	dateDayStr := dateArgs[dateLen-2 : dateLen]
	dateLeft := dateArgs[0 : dateLen-2]
	dateArgs = dateLeft

	dateLen = len(dateArgs)
	dateMonthStr := dateArgs[dateLen-2 : dateLen]
	dateLeft = dateArgs[0 : dateLen-2]
	dateYearStr := dateLeft

	dateYear, _ := strconv.Atoi(dateYearStr)
	dateYear = a.amend4DigitalYear(dateYear)
	dateMonth, _ := strconv.Atoi(dateMonthStr)
	dateDay, _ := strconv.Atoi(dateDayStr)

	return dateYear, dateMonth, dateDay
}

func (a castDateFunc) amend4DigitalYear(year int) int {
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

func (a castDateFunc) DefaultDateValue() proto.Value {
	return proto.NewValueString("0000-00-00")
}
