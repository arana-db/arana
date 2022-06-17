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

package snowflake

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func Test_snowflakeSequence_Acquire(t *testing.T) {

	seq := &snowflakeSequence{
		mu:         sync.Mutex{},
		epoch:      time.Time{},
		lastTime:   0,
		step:       0,
		workdId:    1,
		currentVal: 0,
	}

	val, err := seq.Acquire(context.Background())

	assert.NoError(t, err, fmt.Sprintf("acquire err : %v", err))

	curVal := seq.CurrentVal()

	assert.Equal(t, val, curVal, fmt.Sprintf("acquire val: %d, cur val: %d", val, curVal))
}
