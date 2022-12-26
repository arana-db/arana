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

// FuncCastTime is  https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastTime = "CAST_TIME"

var _ proto.Func = (*castTimeFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastTime, castTimeFunc{})
}

type castTimeFunc struct{}

func (a castTimeFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	// expr
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	timeArgs := fmt.Sprint(val)
	if strings.Compare(timeArgs, "") == 0 {
		return a.DefaultTimeValue(), nil
	}

	// negative flag
	nega := false
	if strings.Compare(string(timeArgs[0]), "-") == 0 {
		nega = true
		timeArgs = string(timeArgs[1:])
	}
	if strings.Compare(string(timeArgs[0]), "+") == 0 {
		timeArgs = string(timeArgs[1:])
	}

	// fractional seconds
	frac := false
	if strings.Contains(timeArgs, ".") {
		timeArr := strings.Split(timeArgs, ".")
		if len(timeArr) >= 2 && len(timeArr[1]) >= 1 {
			timeFrac, _ := strconv.Atoi(string(timeArr[1][0]))
			if timeFrac >= 5 {
				frac = true
			}
		}
		timeArgs = timeArr[0]
	}

	if strings.Contains(timeArgs, " ") {
		// format - D hhh:mm:ss.ms，D hhh:mm.ms，D hhh.ms
		pat := "^\\d{1,2} \\d{1,3}(:\\d{1,2}){0,2}$"
		match, err := regexp.MatchString(pat, timeArgs)
		if !match || err != nil {
			return a.DefaultTimeValue(), nil
		}

		timeArrLeft := strings.Split(timeArgs, " ")
		timeDay, _ := strconv.Atoi(timeArrLeft[0])
		timeHour, timeMinutes, timeSecond := a.splitTimeWithSep(timeArrLeft[1])
		timeHour += timeDay * 24

		if !a.IsHourValid(timeHour) {
			return a.MaxTimeValue(), nil
		}
		if a.IsMinutesValid(timeMinutes) && a.IsSecondValid(timeSecond) {
			timeStr := a.TimeOutput(timeHour, timeMinutes, timeSecond, nega, frac)
			return timeStr, nil
		}
	} else if strings.Contains(timeArgs, ":") {
		// format - hhh:mm:ss.ms，hhh:mm.ms
		pat := "^\\d{1,3}(:\\d{1,2}){1,2}$"
		match, err := regexp.MatchString(pat, timeArgs)
		if !match || err != nil {
			return a.DefaultTimeValue(), nil
		}

		timeHour, timeMinutes, timeSecond := a.splitTimeWithSep(timeArgs)

		if !a.IsHourValid(timeHour) {
			return a.MaxTimeValue(), nil
		}
		if a.IsMinutesValid(timeMinutes) && a.IsSecondValid(timeSecond) {
			timeStr := a.TimeOutput(timeHour, timeMinutes, timeSecond, nega, frac)
			return timeStr, nil
		}
	} else {
		// format - hhhmmss.ms，mmss.ms，ss.ms
		pat := "^\\d{1,7}$"
		match, err := regexp.MatchString(pat, timeArgs)
		if !match || err != nil {
			return a.DefaultTimeValue(), nil
		}

		timeHour, timeMinutes, timeSecond := a.splitTimeWithoutSep(timeArgs)

		if !a.IsHourValid(timeHour) {
			return a.MaxTimeValue(), nil
		}
		if a.IsMinutesValid(timeMinutes) && a.IsSecondValid(timeSecond) {
			timeStr := a.TimeOutput(timeHour, timeMinutes, timeSecond, nega, frac)
			return timeStr, nil
		}
	}

	return a.DefaultTimeValue(), nil
}

func (a castTimeFunc) NumInput() int {
	return 1
}

func (a castTimeFunc) TimeOutput(hour, minutes, second int, nega, frac bool) proto.Value {
	if hour == 838 && minutes == 59 && second == 59 {
		timeStr := a.MaxTimeValue()
		if nega {
			timeStr = a.MinTimeValue()
		}
		return timeStr
	}

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
	if nega {
		timeStr = "-" + timeStr
	}
	return proto.NewValueString(timeStr)
}

func (a castTimeFunc) splitTimeWithSep(timeArgs string) (hour, minutes, second int) {
	timeArr := strings.Split(timeArgs, ":")
	timeHour := 0
	if len(timeArr) >= 1 {
		timeHour, _ = strconv.Atoi(timeArr[0])
	}
	timeMinutes := 0
	if len(timeArr) >= 2 {
		timeMinutes, _ = strconv.Atoi(timeArr[1])
	}
	timeSecond := 0
	if len(timeArr) >= 3 {
		timeSecond, _ = strconv.Atoi(timeArr[2])
	}

	return timeHour, timeMinutes, timeSecond
}

func (a castTimeFunc) splitTimeWithoutSep(timeArgs string) (hour, minutes, second int) {
	timeInt, _ := strconv.Atoi(timeArgs)
	timeSecond := timeInt % 100
	timeLeft := timeInt / 100
	timeMinutes := timeLeft % 100
	timeHour := timeLeft / 100

	return timeHour, timeMinutes, timeSecond
}

func (a castTimeFunc) IsDayValid(day int) bool {
	if day >= 0 && day <= 34 {
		return true
	}
	return false
}

func (a castTimeFunc) IsHourValid(hour int) bool {
	if hour >= 0 && hour <= 838 {
		return true
	}
	return false
}

func (a castTimeFunc) IsMinutesValid(minutes int) bool {
	if minutes >= 0 && minutes <= 59 {
		return true
	}
	return false
}

func (a castTimeFunc) IsSecondValid(second int) bool {
	if second >= 0 && second <= 59 {
		return true
	}
	return false
}

func (a castTimeFunc) MaxTimeValue() proto.Value {
	return proto.NewValueString("838:59:59")
}

func (a castTimeFunc) MinTimeValue() proto.Value {
	return proto.NewValueString("-838:59:59")
}

func (a castTimeFunc) DefaultTimeValue() proto.Value {
	return proto.NewValueString("00:00:00")
}
