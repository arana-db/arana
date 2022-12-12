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

const DefaultBase = 16

func Code(num1, base, num2 int64) (int64, error) {
	t := num1 << base
	if t < 0 {
		return 0, fmt.Errorf("integer operation result is out of range")
	}
	return t + num2, nil
}

func Decode(num, base int64) (int64, int64) {
	t := int64(1<<base - 1)

	return num >> base, num & t
}
