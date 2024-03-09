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

package transaction

import (
	"github.com/arana-db/arana/pkg/runtime"
)

// Global TrxLog arana tx log
type GlobalTrxLog struct {
	TrxID           string
	Tenant          string
	ServerID        int32
	Status          runtime.TxState
	ExpectedEndTime int64
	StartTime       int64
	BranchTrxLogs   []BranchTrxLog
}

// Branch TrxLog arana tx log
type BranchTrxLog struct {
	TrxID         string
	BranchID      string
	ParticipantID string
	Tenant        string
	ServerID      int32
	Status        runtime.TxState
	StartTime     int64
}

// TrxParticipant join target trx all node info
type TrxParticipant struct {
	NodeID     string
	RemoteAddr string
	Schema     string
}

type dBOperation string

const (
	Like           dBOperation = "LIKE"
	In             dBOperation = "IN"
	Equal          dBOperation = "="
	NotEqual       dBOperation = "<>"
	LessThan       dBOperation = "<"
	LessEqualThan  dBOperation = "<="
	GreatThan      dBOperation = ">"
	GrateEqualThan dBOperation = ">="
)

// Condition sql query where condition
type Condition struct {
	FiledName string
	Operation dBOperation
	Value     interface{}
}
