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

package router

import (
	"fmt"
	"regexp"
	"sync"
)

import (
	"github.com/pkg/errors"
)

const _minPasswordLength = 8

var (
	_normalNameRegexp     *regexp.Regexp
	_normalNameRegexpOnce sync.Once
)

var (
	_passwordRegexp     *regexp.Regexp
	_passwordRegexpOnce sync.Once
)

func validateNormalName(name string) bool {
	_normalNameRegexpOnce.Do(func() {
		_normalNameRegexp = regexp.MustCompile("^[_a-zA-Z][0-9a-zA-Z_-]*$")
	})
	return _normalNameRegexp.MatchString(name)
}

func validateTenantName(name string) error {
	if !validateNormalName(name) {
		return errors.Errorf("invalid tenant name '%s'", name)
	}
	return nil
}

func validatePassword(password string) bool {
	_passwordRegexpOnce.Do(func() {
		// FIXME: consider whether to allow '@'?
		_passwordRegexp = regexp.MustCompile(fmt.Sprintf("^[a-zA-Z0-9$#!%%*&^_-]{%d,}", _minPasswordLength))
	})
	return _passwordRegexp.MatchString(password)
}
