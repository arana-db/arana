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

const (
	_initGtidLog = `
CREATE TABLE IF NOT EXISTS __arana_tx_log (
	log_id bigint(20) auto_increment,
	gtid varchar(255) NOT NULL,
	server_id int(10) UNSIGNED NOT NULL,
	state int(10) NOT NULL, // enum('prepare', 'commit', 'abort'), 0=prepare,1=commit,2=abort
	participant varchar(500), //
	start_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
	update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (log_id),
	UNIQUE KEY (gtid)
  ) ENGINE = InnoDB CHARSET = utf8
  // PARTITION // 是否启用 mysql 的二级分区功能，解决清理 gtid log 的问题
`
)

// TxLog Transaction log
type TxLog struct {
	Gtid        string
	ServerID    int32
	State       int32
	Participant string
}

// TxLogManager Transaction log management
type TxLogManager struct {
}

// AddOrUpdateTxLog Add or update transaction log
func (gm *TxLogManager) AddOrUpdateTxLog(l TxLog) error {
	return nil
}

// DeleteTxLog Delete transaction log
func (gm *TxLogManager) DeleteTxLog(l TxLog) error {
	return nil
}

// ScanTxLog Scanning transaction
func (gm *TxLogManager) ScanTxLog(pageNo, pageSize uint32) (uint32, []TxLog, error) {
	return 0, nil, nil
}
