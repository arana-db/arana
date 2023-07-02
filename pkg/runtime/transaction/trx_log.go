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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
)

var (
	_allowFilterAttributes = map[string]struct{}{
		"log_id":      {},
		"trx_id":      {},
		"tenant":      {},
		"server_id":   {},
		"status":      {},
		"participant": {},
		"start_time":  {},
		"update_time": {},
	}
	_initTxLogOnce   sync.Once
	_txLogCleanTimer *time.Timer
)

const (
	// TODO 启用 mysql 的二级分区功能，解决清理 tx log 的问题
	_initTxLog = `
CREATE TABLE IF NOT EXISTS __arana_trx_log
(
    log_id      bigint(20) auto_increment COMMENT 'primary key',
    txr_id      varchar(255)     NOT NULL COMMENT 'transaction uniq id',
    tenant      varchar(255)     NOT NULL COMMENT 'tenant info',
    server_id   int(10) UNSIGNED NOT NULL COMMENT 'arana server node id',
    status      int(10)          NOT NULL COMMENT 'transaction status, preparing:2,prepared:3,committing:4,committed:5,aborting:6,rollback:7,finish:8,rolledBack:9',
    participant varchar(500) COMMENT 'transaction participants, content is mysql node info',
    start_time  timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (log_id),
    UNIQUE KEY (txr_id)
) ENGINE = InnoDB
  CHARSET = utf8
`
	insSql    = "REPLACE INTO __arana_trx_log(trx_id, tenant, server_id, status, participant, start_time, update_time) VALUES (?,?,?,?,?,sysdate(),sysdate())"
	delSql    = "DELETE FROM __arana_trx_log WHERE trx_id = ?"
	selectSql = "SELECT trx_id, tenant, server_id, status, participant, start_time, update_time FROM __arana_trx_log WHERE 1=1 %s ORDER BY update_time LIMIT ? OFFSET ?"
)

// TxLogManager Transaction log management
type TxLogManager struct {
	sysDB proto.DB
}

// init executes create __arana_tx_log table action
func (gm *TxLogManager) Init(delay time.Duration) error {
	var err error
	_initTxLogOnce.Do(func() {
		ctx := context.Background()
		res, _, err := gm.sysDB.Call(ctx, _initTxLog)
		if err != nil {
			return
		}
		_, _ = res.RowsAffected()
		_txLogCleanTimer = time.AfterFunc(delay, gm.runCleanTxLogTask)
	})
	return err
}

// AddOrUpdateTxLog Add or update transaction log
func (gm *TxLogManager) AddOrUpdateTxLog(l TrxLog) error {
	participants, err := json.Marshal(l.Participants)
	if err != nil {
		return err
	}
	trxIdVal, _ := proto.NewValue(l.TrxID)
	tenantVal, _ := proto.NewValue(l.Tenant)
	serverIdVal, _ := proto.NewValue(l.ServerID)
	stateVal, _ := proto.NewValue(int32(l.State))
	participantsVal, _ := proto.NewValue(string(participants))
	args := []proto.Value{
		trxIdVal,
		tenantVal,
		serverIdVal,
		stateVal,
		participantsVal,
	}
	_, _, err = gm.sysDB.Call(context.Background(), insSql, args...)
	return err
}

// DeleteTxLog Delete transaction log
func (gm *TxLogManager) DeleteTxLog(l TrxLog) error {
	trxIdVal, _ := proto.NewValue(l.TrxID)
	args := []proto.Value{
		trxIdVal,
	}
	_, _, err := gm.sysDB.Call(context.Background(), delSql, args...)
	return err
}

// ScanTxLog Scanning transaction
func (gm *TxLogManager) ScanTxLog(pageNo, pageSize uint64, conditions []Condition) (uint32, []TrxLog, error) {
	var (
		whereBuilder []string
		args         []proto.Value
		logs         []TrxLog
		num          uint32
		dest         []proto.Value
		log          TrxLog
		participants []TrxParticipant
		serverId     int64
		state        int64
	)

	for i := range conditions {
		condition := conditions[i]
		if _, ok := _allowFilterAttributes[condition.FiledName]; !ok {
			return 0, nil, fmt.Errorf("ScanTxLog filter attribute=%s not allowed", condition.FiledName)
		}
		whereBuilder = append(whereBuilder, fmt.Sprintf("%s %s ?", condition.FiledName, condition.Operation))
		val, _ := proto.NewValue(condition.Value)
		args = append(args, val)
	}

	limit := proto.NewValueUint64(pageSize)
	offset := proto.NewValueUint64((pageNo - 1) * pageSize)

	args = append(args, limit, offset)
	conditionSelectSql := fmt.Sprintf(selectSql, strings.Join(whereBuilder, " "))
	rows, _, err := gm.sysDB.Call(context.Background(), conditionSelectSql, args...)
	if err != nil {
		return 0, nil, err
	}
	dataset, _ := rows.Dataset()
	for {
		row, err := dataset.Next()
		if err != nil {
			return 0, nil, err
		}
		if row == nil {
			break
		}
		if err := row.Scan(dest[:]); err != nil {
			return 0, nil, err
		}
		log.TrxID = dest[0].String()
		log.Tenant = dest[1].String()
		serverId, _ = dest[2].Int64()
		log.ServerID = int32(serverId)
		state, _ = dest[3].Int64()
		log.State = runtime.TxState(int32(state))

		if err := json.Unmarshal([]byte(dest[4].String()), &participants); err != nil {
			return 0, nil, err
		}
		log.Participants = participants
		logs = append(logs, log)
		num++
	}
	return num, logs, nil
}

// runCleanTxLogTask execute the transaction log cleanup action, and clean up the __arana_tx_log secondary
// partition table according to the day level or hour level.
// the execution of this task requires distributed task preemption based on the metadata DB
func (gm *TxLogManager) runCleanTxLogTask() {
	var (
		pageNo     uint64
		pageSize   uint64 = 50
		conditions        = []Condition{
			{
				FiledName: "status",
				Operation: Equal,
				Value:     runtime.TrxFinish,
			},
		}
	)
	var txLogs []TrxLog
	for {
		total, logs, err := gm.ScanTxLog(pageNo, pageSize, conditions)
		if err != nil {
			break
		}
		txLogs = append(txLogs, logs...)
		if len(txLogs) >= int(total) {
			break
		}
	}
	for _, l := range txLogs {
		gm.DeleteTxLog(l)
	}
}
