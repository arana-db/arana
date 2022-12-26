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

// FuncCastDatetime is  https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastDatetime = "CAST_DATETIME"

var _ proto.Func = (*castDatetimeFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastDatetime, castDatetimeFunc{})
}

type castDatetimeFunc struct{}

var castDate castDateFunc
var castTime castTimeFunc

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

		pat := "^\\d{1,4}[~!@#$%^&*_+\\-=:;,|?/]{1}\\d{1,2}[~!@#$%^&*_+\\-=:;,|?/]{1}\\d{1,2}$"
		match, err := regexp.MatchString(pat, datetimeArr[0])
		if !match || err != nil {
			return a.DefaultDatetimeValue(), nil
		}
		year, month, day := castDate.splitDateWithSep(datetimeArr[0])
		year = castDate.amend4DigtalYear(year)

		pat = "^\\d{1,2}[~!@#$%^&*_+\\-=:;,|?/]{1}\\d{1,2}[~!@#$%^&*_+\\-=:;,|?/]{1}\\d{1,2}$"
		match, err = regexp.MatchString(pat, datetimeArr[1])
		if !match || err != nil {
			return a.DefaultDatetimeValue(), nil
		}
		hour, minutes, second := a.splitDatetimeWithSep(datetimeArr[1])

		if castDate.IsYearValid(year) && castDate.IsMonthValid(month) && castDate.IsDayValid(year, month, day) &&
			a.IsHourValid(hour) && castTime.IsMinutesValid(minutes) && castTime.IsSecondValid(second) {
			return a.DatetimeOutput(year, month, day, hour, minutes, second, frac), nil
		} else {
			return a.DefaultDatetimeValue(), nil
		}
	} else {
		// format - YYYYMMDDhhmmss.ms
		pat := "^\\d{11,14}$"
		match, err := regexp.MatchString(pat, datetimeArgs)
		if !match || err != nil {
			return a.DefaultDatetimeValue(), nil
		}

		datetimeLen := len(datetimeArgs)
		dateArgs := string(datetimeArgs[0 : datetimeLen-6])
		year, month, day := castDate.splitDateWithoutSep(dateArgs)
		year = castDate.amend4DigtalYear(year)

		timeArgs := string(datetimeArgs[datetimeLen-6 : datetimeLen])
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
	if frac {
		second += 1
		if second >= 60 {
			minutes += 1
			second = 0
		}
		if minutes >= 60 {
			hour += 1
			minutes = 0
		}
		if hour >= 24 {
			day += 1
			hour = 0
		}

		if month == 1 || month == 3 || month == 5 || month == 7 ||
			month == 8 || month == 10 || month == 12 {
			if day >= 32 {
				month += 1
				day = 1
			}
		}
		if month == 4 || month == 6 || month == 9 || month == 11 {
			if day >= 31 {
				month += 1
				day = 1
			}
		}
		if month == 2 {
			if (year%100 == 0 && year%400 == 0) ||
				(year%100 > 0 && year%4 == 0) {
				if day >= 30 {
					month += 1
					day = 1
				}
			} else {
				if day >= 29 {
					month += 1
					day = 1
				}
			}
		}
		if month >= 13 {
			year += 1
			month = 1
		}
		if year >= 10000 {
			return a.DefaultDatetimeValue()
		}
	}

	secondStr := fmt.Sprint(second)
	if second < 10 {
		secondStr = "0" + secondStr
	}
	minutesStr := fmt.Sprint(minutes)
	if minutes < 10 {
		minutesStr = "0" + minutesStr
	}
	hourStr := fmt.Sprint(hour)
	if hour < 10 {
		hourStr = "0" + hourStr
	}
	timeStr := hourStr + ":" + minutesStr + ":" + secondStr

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

	return proto.NewValueString(dateStr + " " + timeStr)
}

func (a castDatetimeFunc) splitDatetimeWithSep(timeArgs string) (hour, minutes, second int) {
	timeLen := len(timeArgs)
	timeSecondStr := string(timeArgs[timeLen-1 : timeLen])
	timeLeft := string(timeArgs[0 : timeLen-2])
	if castDate.IsDigitalValid(string(timeArgs[timeLen-2])) {
		timeSecondStr = string(timeArgs[timeLen-2 : timeLen])
		timeLeft = string(timeArgs[0 : timeLen-3])
	}
	timeArgs = timeLeft

	timeLen = len(timeArgs)
	timeMinutesStr := string(timeArgs[timeLen-1 : timeLen])
	timeLeft = string(timeArgs[0 : timeLen-2])
	if castDate.IsDigitalValid(string(timeArgs[timeLen-2])) {
		timeMinutesStr = string(timeArgs[timeLen-2 : timeLen])
		timeLeft = string(timeArgs[0 : timeLen-3])
	}
	timeHourStr := timeLeft

	timeHour, _ := strconv.Atoi(timeHourStr)
	timeMinutes, _ := strconv.Atoi(timeMinutesStr)
	timeSecond, _ := strconv.Atoi(timeSecondStr)

	return timeHour, timeMinutes, timeSecond
}

func (a castDatetimeFunc) splitDatetimeWithoutSep(timeArgs string) (hour, minutes, second int) {
	timeLen := len(timeArgs)
	timeSecondStr := string(timeArgs[timeLen-2 : timeLen])
	timeLeft := string(timeArgs[0 : timeLen-2])
	timeArgs = timeLeft

	timeLen = len(timeArgs)
	timeMinutesStr := string(timeArgs[timeLen-2 : timeLen])
	timeLeft = string(timeArgs[0 : timeLen-2])
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
