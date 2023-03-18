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

package gtid

import (
	"fmt"
	"sync"
)

import (
	"github.com/bwmarrin/snowflake"
)

import (
	"github.com/arana-db/arana/pkg/util/identity"
	"github.com/arana-db/arana/pkg/util/rand2"
)

var (
	nodeId     string
	once       sync.Once
	seqBuilder *snowflake.Node
)

// ID Gtid
type ID struct {
	NodeID string
	Seq    int64
}

// NewID generates next Gtid
func NewID() ID {
	once.Do(func() {
		nodeId = identity.GetNodeIdentity()
		seqBuilder, _ = snowflake.NewNode(rand2.Int63n(1024))
	})

	return ID{
		NodeID: nodeId,
		Seq:    seqBuilder.Generate().Int64(),
	}
}

// String ID to string
func (i ID) String() string {
	return fmt.Sprintf("%s-%d", i.NodeID, i.Seq)
}
