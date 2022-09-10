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

package exception

import (
	"fmt"
)

const (
	CodeServerError Code = 1000 + iota
	CodeNotFound
	CodeInvalidParams
	CodeAuth
	CodeUnknownError
)

var _httpStatus = map[Code]int{
	CodeNotFound:      404,
	CodeInvalidParams: 400,
	CodeAuth:          401,
	CodeServerError:   500,
}

type Code uint16

func (ec Code) HttpStatus() int {
	hc, ok := _httpStatus[ec]
	if ok {
		return hc
	}
	return 500
}

type APIException struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

func (ae APIException) Error() string {
	return fmt.Sprintf("ERR-%05d: %s", ae.Code, ae.Message)
}

func New(code Code, format string, args ...interface{}) APIException {
	return APIException{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

func Wrap(code Code, err error) APIException {
	return APIException{
		Code:    code,
		Message: err.Error(),
	}
}
