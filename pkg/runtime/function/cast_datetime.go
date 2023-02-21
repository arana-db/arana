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
	"time"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncCastDatetime is  https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastDatetime = "CAST_DATETIME"

var (
	DatetimeSep               = `[~!@#$%^&*_+=:;,|/?\(\)\[\]\{\}\-\\]+`
	_datetimeReplace          = regexp.MustCompile(DatetimeSep)
	_datetimeMatchUpperString = regexp.MustCompile(fmt.Sprintf(`^\d{1,4}%s\d{1,2}%s\d{1,2}$`, DatetimeSep, DatetimeSep))
	_datetimeMatchLowerString = regexp.MustCompile(fmt.Sprintf(`^\d{1,2}%s\d{1,2}%s\d{1,2}$`, DatetimeSep, DatetimeSep))
	_datetimeMatchInt         = regexp.MustCompile(`^\d{11,14}$`)
)

var _ proto.Func = (*castDatetimeFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastDatetime, castDatetimeFunc{})
}

type castDatetimeFunc struct{}

var (
	castDate castDateFunc
	castTime castTimeFunc
)

func (a castDatetimeFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	// expr
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	datetimeArgs := fmt.Sprint(val)
	if strings.Compare(datetimeArgs, "") == 0 {
		return a.DefaultDatetimeValue(), nil
	}

	// fractional seconds
	frac := false
	if strings.Contains(datetimeArgs, ".") {
		datetimeArr := strings.Split(datetimeArgs, ".")
		if len(datetimeArr) >= 2 && len(datetimeArr[1]) >= 1 {
			datetimeFrac, _ := strconv.Atoi(string(datetimeArr[1][0]))
			if datetimeFrac >= 5 {
				frac = true
			}
		}
		datetimeArgs = datetimeArr[0]
	}

	if strings.Contains(datetimeArgs, " ") || strings.Contains(datetimeArgs, "T") {
		// format - YYYY-MM-DD hh:mm:ss.ms or YYYY-MM-DDThh:mm:ss.ms
		var datetimeArr []string
		if strings.Contains(datetimeArgs, " ") {
			datetimeArr = strings.Split(datetimeArgs, " ")
		} else if strings.Contains(datetimeArgs, "T") {
			datetimeArr = strings.Split(datetimeArgs, "T")
		} else {
			return a.DefaultDatetimeValue(), nil
		}

		match := _datetimeMatchUpperString.MatchString(datetimeArr[0])
		if !match {
			return a.DefaultDatetimeValue(), nil
		}
		datetimeArrReplace := _datetimeReplace.ReplaceAllStringFunc(datetimeArr[0], func(s string) string { return "-" })
		year, month, day := castDate.splitDateWithSep(datetimeArrReplace)
		year = castDate.amend4DigtalYear(year)

		match = _datetimeMatchLowerString.MatchString(datetimeArr[1])
		if !match {
			return a.DefaultDatetimeValue(), nil
		}
		datetimeArrReplace = _datetimeReplace.ReplaceAllStringFunc(datetimeArr[1], func(s string) string { return "-" })
		hour, minutes, second := a.splitDatetimeWithSep(datetimeArrReplace)

		if castDate.IsYearValid(year) && castDate.IsMonthValid(month) && castDate.IsDayValid(year, month, day) &&
			a.IsHourValid(hour) && castTime.IsMinutesValid(minutes) && castTime.IsSecondValid(second) {
			return a.DatetimeOutput(year, month, day, hour, minutes, second, frac), nil
		} else {
			return a.DefaultDatetimeValue(), nil
		}
	} else {
		// format - YYYYMMDDhhmmss.ms
		match := _datetimeMatchInt.MatchString(datetimeArgs)
		if !match {
			return a.DefaultDatetimeValue(), nil
		}

		datetimeLen := len(datetimeArgs)
		dateArgs := datetimeArgs[0 : datetimeLen-6]
		year, month, day := castDate.splitDateWithoutSep(dateArgs)
		year = castDate.amend4DigtalYear(year)

		timeArgs := datetimeArgs[datetimeLen-6 : datetimeLen]
		hour, minutes, second := a.splitDatetimeWithoutSep(timeArgs)

		if castDate.IsYearValid(year) && castDate.IsMonthValid(month) && castDate.IsDayValid(year, month, day) &&
			a.IsHourValid(hour) && castTime.IsMinutesValid(minutes) && castTime.IsSecondValid(second) {
			return a.DatetimeOutput(year, month, day, hour, minutes, second, frac), nil
		} else {
			return a.DefaultDatetimeValue(), nil
		}
	}
}

func (a castDatetimeFunc) NumInput() int {
	return 1
}

func (a castDatetimeFunc) DatetimeOutput(year, month, day, hour, minutes, second int, frac bool) proto.Value {
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
	sb.WriteString(" ")

	hourStr := strconv.FormatInt(int64(hour), 10)
	if hour < 10 {
		sb.WriteString("0")
	}
	sb.WriteString(hourStr)
	sb.WriteString(":")
	minutesStr := strconv.FormatInt(int64(minutes), 10)
	if minutes < 10 {
		sb.WriteString("0")
	}
	sb.WriteString(minutesStr)
	sb.WriteString(":")
	secondStr := strconv.FormatInt(int64(second), 10)
	if second < 10 {
		sb.WriteString("0")
	}
	sb.WriteString(secondStr)

	datetimeRet, _ := time.Parse("2006-01-02 15:04:05", sb.String())

	if frac {
		datetimeRet = datetimeRet.Add(1 * time.Second)
	}

	return proto.NewValueString(datetimeRet.Format("2006-01-02 15:04:05"))
}

func (a castDatetimeFunc) splitDatetimeWithSep(timeArgs string) (hour, minutes, second int) {
	timeLen := len(timeArgs)
	timeSecondStr := timeArgs[timeLen-1 : timeLen]
	timeLeft := timeArgs[0 : timeLen-2]
	if castDate.IsDigitalValid(string(timeArgs[timeLen-2])) {
		timeSecondStr = timeArgs[timeLen-2 : timeLen]
		timeLeft = timeArgs[0 : timeLen-3]
	}
	timeArgs = timeLeft

	timeLen = len(timeArgs)
	timeMinutesStr := timeArgs[timeLen-1 : timeLen]
	timeLeft = timeArgs[0 : timeLen-2]
	if castDate.IsDigitalValid(string(timeArgs[timeLen-2])) {
		timeMinutesStr = timeArgs[timeLen-2 : timeLen]
		timeLeft = timeArgs[0 : timeLen-3]
	}
	timeHourStr := timeLeft

	timeHour, _ := strconv.Atoi(timeHourStr)
	timeMinutes, _ := strconv.Atoi(timeMinutesStr)
	timeSecond, _ := strconv.Atoi(timeSecondStr)

	return timeHour, timeMinutes, timeSecond
}

func (a castDatetimeFunc) splitDatetimeWithoutSep(timeArgs string) (hour, minutes, second int) {
	timeLen := len(timeArgs)
	timeSecondStr := timeArgs[timeLen-2 : timeLen]
	timeLeft := timeArgs[0 : timeLen-2]
	timeArgs = timeLeft

	timeLen = len(timeArgs)
	timeMinutesStr := timeArgs[timeLen-2 : timeLen]
	timeLeft = timeArgs[0 : timeLen-2]
	timeHourStr := timeLeft

	timeHour, _ := strconv.Atoi(timeHourStr)
	timeMinutes, _ := strconv.Atoi(timeMinutesStr)
	timeSecond, _ := strconv.Atoi(timeSecondStr)

	return timeHour, timeMinutes, timeSecond
}

func (a castDatetimeFunc) IsHourValid(hour int) bool {
	if hour >= 0 && hour <= 23 {
		return true
	}
	return false
}

func (a castDatetimeFunc) DefaultDatetimeValue() proto.Value {
	return proto.NewValueString("0000-00-00 00:00:00")
}
