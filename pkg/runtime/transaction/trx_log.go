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
)

// TxLogManager Transaction log management
type TxLogManager struct {
	sysDB proto.DB
}

// init executes create __arana_tx_log table action
func (gm *TxLogManager) init() error {
	ctx := context.Background()
	res, _, err := gm.sysDB.Call(ctx, _initTxLog)
	if err != nil {
		return err
	}
	_, _ = res.RowsAffected()
	return nil
}

// AddOrUpdateTxLog Add or update transaction log
func (gm *TxLogManager) AddOrUpdateTxLog(l TrxLog) error {
	insSql := `
REPLACE INTO
    __arana_trx_log(trx_id, tenant, server_id, status, participant, start_time, update_time)
VALUES
    (?,?,?,?,?,sysdate(),sysdate())
`
	participants, err := json.Marshal(l.Participants)
	if err != nil {
		return err
	}
	trxIdVal, _ := proto.NewValue(l.TrxID)
	tenantVal, _ := proto.NewValue(l.Tenant)
	serverIdVal, _ := proto.NewValue(l.ServerID)
	stateVal, _ := proto.NewValue(l.State)
	participantsVal, _ := proto.NewValue(participants)
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
	delSql := "DELETE FROM __arana_trx_log WHERE trx_id = ?"
	trxIdVal, _ := proto.NewValue(l.TrxID)
	args := []proto.Value{
		trxIdVal,
	}
	_, _, err := gm.sysDB.Call(context.Background(), delSql, args...)
	return err
}

// ScanTxLog Scanning transaction
func (gm *TxLogManager) ScanTxLog(pageNo, pageSize uint64, conditions []Condition) (uint32, []TrxLog, error) {
	selectSql := `
SELECT
    trx_id, tenant, server_id, status, participant, start_time, update_time
FROM
    __arana_trx_log
WHERE
    1=1 %s LIMIT ? OFFSET ? ORDER BY update_time
`

	var (
		whereBuilder []string
		args         []proto.Value
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

	selectSql = fmt.Sprintf(selectSql, strings.Join(whereBuilder, " "))

	_, _, err := gm.sysDB.Call(context.Background(), selectSql, args...)
	if err != nil {
		return 0, nil, err
	}

	// TODO convert sql.Rows to []TrxLog

	return 0, nil, nil
}

// runCleanTxLogTask execute the transaction log cleanup action, and clean up the __arana_tx_log secondary
// partition table according to the day level or hour level.
// the execution of this task requires distributed task preemption based on the metadata DB
func (gm *TxLogManager) runCleanTxLogTask() {

}
