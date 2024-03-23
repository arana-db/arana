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
	"fmt"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/proto"
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
	_initGlobalTxLog = `
		CREATE TABLE __arana_global_trx_log (
		  log_id bigint NOT NULL AUTO_INCREMENT COMMENT 'primary key',
		  txr_id varchar(255) NOT NULL COMMENT 'transaction uniq id',
		  tenant varchar(255) NOT NULL COMMENT 'tenant info',
		  server_id int unsigned NOT NULL COMMENT 'arana server node id',
		  status int NOT NULL COMMENT 'transaction status: started:1,preparing:2,prepared:3,committing:4,committed:5,rollbacking:6,rollbacked:7,failed:8',
		  start_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'transaction start time',
		  expected_end_time datetime NOT NULL COMMENT 'global transaction expected end time',
		  update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		  PRIMARY KEY (log_id),
		  UNIQUE KEY txr_id (txr_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3
	`
	insertGlobalSql = "INSERT INTO __arana_global_trx_log (txr_id, tenant, server_id, status, start_time, expected_end_time) VALUES (?, ?, ?, ?, ?, ?);"
	deleteGlobalSql = "DELETE FROM __arana_global_trx_log WHERE trx_id = ?"
	selectGlobalSql = "SELECT log_id, txr_id, tenant, server_id, status, start_time, expected_end_time, update_time FROM __arana_trx_log WHERE 1=1 %s ORDER BY expected_end_time LIMIT ? OFFSET ?"
)

const (
	_initBranchTxLog = `
		CREATE TABLE __arana_branch_trx_log (
		  log_id bigint NOT NULL AUTO_INCREMENT COMMENT 'primary key',
		  txr_id varchar(255) NOT NULL COMMENT 'transaction uniq id',
		  branch_id varchar(255) NOT NULL COMMENT 'branch transaction key',
		  participant_id int unsigned NOT NULL COMMENT 'transaction participants, content is mysql node info',
		  status int NOT NULL COMMENT 'transaction status: started:1,preparing:2,prepared:3,committing:4,committed:5,rollbacking:6,rollbacked:7,failed:8',
		  start_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'branch transaction start time',
		  update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		  PRIMARY KEY (log_id),
		  UNIQUE KEY txr_branch_id (txr_id, branch_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3
	`
	insertBranchSql = "INSERT INTO __arana_branch_trx_log (txr_id, branch_id, participant_id, status, start_time) VALUES (?, ?, ?, ?, ?);"
	deleteBranchSql = "DELETE FROM __arana_global_trx_log WHERE trx_id = ? and branch_id=?"
	selectBranchSql = "SELECT log_id, txr_id, branch_id, participant_id, status, start_time, update_time FROM __arana_branch_trx_log WHERE 1=1 %s ORDER BY expected_end_time LIMIT ? OFFSET ?"
)

// TxLogManager Transaction log management
// TODO
type TxLogManager struct {
	sysDB proto.DB
}

// init executes create __arana_tx_log table action
func (gm *TxLogManager) Init(delay time.Duration) error {
	var err error
	_initTxLogOnce.Do(func() {
		ctx := context.Background()
		res, _, err := gm.sysDB.Call(ctx, _initGlobalTxLog)
		if err != nil {
			return
		}
		_, _ = res.RowsAffected()
		_txLogCleanTimer = time.AfterFunc(delay, gm.runCleanGlobalTxLogTask)

		res, _, err = gm.sysDB.Call(ctx, _initBranchTxLog)
		if err != nil {
			return
		}
		_, _ = res.RowsAffected()
		_txLogCleanTimer = time.AfterFunc(delay, gm.runCleanBranchTxLogTask)
	})
	return err
}

// AddOrUpdateTxLog Add or update global transaction log
func (gm *TxLogManager) AddOrUpdateGlobalTxLog(l GlobalTrxLog) error {
	trxIdVal, _ := proto.NewValue(l.TrxID)
	tenantVal, _ := proto.NewValue(l.Tenant)
	serverIdVal, _ := proto.NewValue(l.ServerID)
	statusVal, _ := proto.NewValue(int32(l.Status))
	startTimeVal, _ := proto.NewValue(l.StartTime)
	expectedEndTimeVal, _ := proto.NewValue(l.ExpectedEndTime)
	args := []proto.Value{
		trxIdVal,
		tenantVal,
		serverIdVal,
		statusVal,
		startTimeVal,
		expectedEndTimeVal,
	}
	_, _, err := gm.sysDB.Call(context.Background(), insertGlobalSql, args...)
	return err
}

// AddOrUpdateTxLog Add or update branch transaction log
func (gm *TxLogManager) AddOrUpdateBranchTxLog(l BranchTrxLog) error {
	panic("implement me")
}

// DeleteTxLog Delete transaction log
func (gm *TxLogManager) DeleteGlobalTxLog(l GlobalTrxLog) error {
	trxIdVal, _ := proto.NewValue(l.TrxID)
	args := []proto.Value{
		trxIdVal,
	}
	_, _, err := gm.sysDB.Call(context.Background(), deleteGlobalSql, args...)
	return err
}

// TODO
func (gm *TxLogManager) DeleteBranchTxLog(l BranchTrxLog) error {
	panic("implement me")
}

// Global ScanTxLog Scanning transaction
func (gm *TxLogManager) ScanGlobalTxLog(pageNo, pageSize uint64, conditions []Condition) (uint32, []GlobalTrxLog, error) {
	var (
		whereBuilder    []string
		args            []proto.Value
		logs            []GlobalTrxLog
		num             uint32
		dest            []proto.Value
		serverId        int64
		expectedEndTime int64
		startTime       int64
		state           int64
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
	conditionSelectSql := fmt.Sprintf(selectGlobalSql, strings.Join(whereBuilder, " "))
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
		var log GlobalTrxLog
		if err = row.Scan(dest[:]); err != nil {
			return 0, nil, err
		}
		log.TrxID = dest[0].String()
		log.Tenant = dest[1].String()
		serverId, _ = dest[2].Int64()
		log.ServerID = int32(serverId)
		state, _ = dest[3].Int64()
		log.Status = rcontext.TxState(state)
		expectedEndTime, _ = dest[4].Int64()
		log.ExpectedEndTime = time.UnixMilli(expectedEndTime)
		startTime, _ = dest[5].Int64()
		log.StartTime = time.UnixMilli(startTime)
		logs = append(logs, log)
		num++
	}
	return num, logs, nil
}

// Branch ScanTxLog Scanning transaction
// TODO
func (gm *TxLogManager) ScanBranchTxLog(pageNo, pageSize uint64, conditions []Condition) (uint32, []BranchTrxLog, error) {
	panic("implement me")
}

// runCleanTxLogTask execute the transaction log cleanup action, and clean up the __arana_tx_log secondary
// partition table according to the day level or hour level.
// the execution of this task requires distributed task preemption based on the metadata DB
func (gm *TxLogManager) runCleanGlobalTxLogTask() {
	var (
		pageNo     uint64
		pageSize   uint64 = 50
		conditions        = []Condition{
			{
				FiledName: "status",
				Operation: In,
				Value:     []int32{int32(rcontext.TrxRolledBacked), int32(rcontext.TrxCommitted), int32(rcontext.TrxAborted)},
			},
		}
	)
	var txLogs []GlobalTrxLog
	for {
		total, logs, err := gm.ScanGlobalTxLog(pageNo, pageSize, conditions)
		if err != nil {
			break
		}
		txLogs = append(txLogs, logs...)
		if len(txLogs) >= int(total) {
			break
		}
	}
	for _, l := range txLogs {
		gm.DeleteGlobalTxLog(l)
	}
}

// TODO
func (gm *TxLogManager) runCleanBranchTxLogTask() {
	panic("implement me")
}
