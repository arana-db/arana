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

package sequence

import (
	"context"
	"fmt"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	_ "github.com/arana-db/arana/pkg/sequence/snowflake"
	"github.com/arana-db/arana/testdata"
)

func Test_sequenceManager(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := testdata.NewMockVConn(ctrl)

	ret := resultx.New(resultx.WithLastInsertID(1))

	conn.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(ret, nil)

	manager := newSequenceManager()

	name := proto.BuildAutoIncrementName("student")

	seq, err := manager.CreateSequence(context.Background(), "conn", "conn", proto.SequenceConfig{
		Name: name,
		Type: "snowflake",
		Option: map[string]string{
			"": "",
		},
	})

	assert.NoError(t, err, fmt.Sprintf("create sequence err: %v", err))

	assert.NotNil(t, seq)

	s, err := manager.GetSequence(context.Background(), "conn", "conn", name)

	assert.NoError(t, err, fmt.Sprintf("create sequence err: %v", err))

	assert.NotNil(t, s)

	assert.Equal(t, seq, s)
}
