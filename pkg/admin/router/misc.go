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
	"regexp"
	"sync"
)

import (
	"github.com/pkg/errors"
)

var (
	_tenantNameRegexp     *regexp.Regexp
	_tenantNameRegexpOnce sync.Once
)

func validateTenantName(name string) error {
	_tenantNameRegexpOnce.Do(func() {
		_tenantNameRegexp = regexp.MustCompile("[a-zA-Z][a-zA-Z0-9-_]+")
	})
	if _tenantNameRegexp.MatchString(name) {
		return nil
	}
	return errors.Errorf("invalid tenant name '%s'", name)
}
