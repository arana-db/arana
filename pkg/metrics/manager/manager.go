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

package manager

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/metrics/stats"
	"github.com/arana-db/arana/pkg/metrics/stats/prometheus"
	"github.com/arana-db/arana/pkg/util/log"
)

const (
	SQLExecTimeSize = 5000
)

const (
	statsLabelCluster       = "Cluster"
	statsLabelOperation     = "Operation"
	statsLabelNamespace     = "Namespace"
	statsLabelFingerprint   = "Fingerprint"
	statsLabelFlowDirection = "FlowDirection"
	statsLabelSlice         = "Slice"
	statsLabelIPAddr        = "IPAddr"
	statsLabelRole          = "role"
)

// SQLResponse record one namespace SQL response like P99/P95
type SQLResponse struct {
	ns                      string
	sqlExecTimeRecordSwitch bool
	sqlExecTimeChan         chan string
	sqlTimeList             []float64
	response99Max           map[string]float64 // map[backendAddr]P99MaxValue
	response99Avg           map[string]float64 // map[backendAddr]P99AvgValue
	response95Max           map[string]float64 // map[backendAddr]P95MaxValue
	response95Avg           map[string]float64 // map[backendAddr]P95AvgValue
}

func NewSQLResponse(name string) *SQLResponse {
	return &SQLResponse{
		ns:                      name,
		sqlExecTimeRecordSwitch: false,
		sqlExecTimeChan:         make(chan string, SQLExecTimeSize),
		sqlTimeList:             []float64{},
		response99Max:           make(map[string]float64),
		response99Avg:           make(map[string]float64),
		response95Max:           make(map[string]float64),
		response95Avg:           make(map[string]float64),
	}

}

// StatisticManager statistics manager
type StatisticManager struct {
	bootstrapPath string
	options       *config.BootOptions

	clusterName string
	startTime   int64

	statsType     string // 监控后端类型
	handlers      map[string]http.Handler
	generalLogger log.Logger

	sqlTimings     *stats.MultiTimings            // SQL耗时统计
	sqlErrorCounts *stats.CountersWithMultiLabels // SQL错误数统计

	flowCounts       *stats.CountersWithMultiLabels // 业务流量统计
	sessionCounts    *stats.GaugesWithMultiLabels   // 前端会话数统计
	clientConnecions sync.Map                       // 等同于sessionCounts, 用于限制前端连接

	backendSQLTimings               *stats.MultiTimings            // 后端SQL耗时统计
	backendSQLFingerprintSlowCounts *stats.CountersWithMultiLabels // 后端慢SQL指纹数量统计
	backendSQLErrorCounts           *stats.CountersWithMultiLabels // 后端SQL错误数统计
	backendConnectPoolIdleCounts    *stats.GaugesWithMultiLabels   //后端空闲连接数统计
	backendConnectPoolInUseCounts   *stats.GaugesWithMultiLabels   //后端正在使用连接数统计
	backendConnectPoolWaitCounts    *stats.GaugesWithMultiLabels   //后端等待队列统计
	backendInstanceCounts           *stats.GaugesWithMultiLabels   //后端实例状态统计
	uptimeCounts                    *stats.GaugesWithMultiLabels   // 启动时间记录
	backendSQLResponse99MaxCounts   *stats.GaugesWithMultiLabels   // 后端 SQL 耗时 P99 最大响应时间
	backendSQLResponse99AvgCounts   *stats.GaugesWithMultiLabels   // 后端 SQL 耗时 P99 平均响应时间
	backendSQLResponse95MaxCounts   *stats.GaugesWithMultiLabels   // 后端 SQL 耗时 P95 最大响应时间
	backendSQLResponse95AvgCounts   *stats.GaugesWithMultiLabels   // 后端 SQL 耗时 P95 平均响应时间

	SQLResponsePercentile map[string]*SQLResponse // 用于记录 P99/P95 Max/AVG 响应时间
	slowSQLTime           int64
	closeChan             chan bool
}

// Init init StatisticManager
func (s *StatisticManager) Init() error {
	s.startTime = time.Now().Unix()
	s.closeChan = make(chan bool, 0)
	s.handlers = make(map[string]http.Handler)
	bootOptions, err := config.LoadBootOptions(s.bootstrapPath)
	if err != nil {
		return err
	}
	if bootOptions.Stats == nil {
		return fmt.Errorf("bootOptions.Stats is nil")
	}
	if err := s.initBackend(bootOptions.Stats); err != nil {
		return err
	}

	s.sqlTimings = stats.NewMultiTimings("SqlTimings",
		"arana proxy sql sqlTimings", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	s.sqlErrorCounts = stats.NewCountersWithMultiLabels("SqlErrorCounts",
		"arana proxy sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})

	s.flowCounts = stats.NewCountersWithMultiLabels("FlowCounts",
		"arana proxy flow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFlowDirection})
	s.sessionCounts = stats.NewGaugesWithMultiLabels("SessionCounts",
		"arana proxy session counts", []string{statsLabelCluster, statsLabelNamespace})
	s.backendSQLTimings = stats.NewMultiTimings("BackendSqlTimings",
		"arana proxy backend sql sqlTimings", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	s.backendSQLFingerprintSlowCounts = stats.NewCountersWithMultiLabels("BackendSqlFingerprintSlowCounts",
		"arana proxy backend sql fingerprint slow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	s.backendSQLErrorCounts = stats.NewCountersWithMultiLabels("BackendSqlErrorCounts",
		"arana proxy backend sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	s.backendConnectPoolIdleCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolIdleCounts",
		"arana proxy backend idle connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})
	s.backendConnectPoolInUseCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolInUseCounts",
		"arana proxy backend in-use connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})
	s.backendConnectPoolWaitCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolWaitCounts",
		"arana proxy backend wait connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})
	s.backendInstanceCounts = stats.NewGaugesWithMultiLabels("backendInstanceCounts",
		"arana proxy backend DB status counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr, statsLabelRole})
	s.backendSQLResponse99MaxCounts = stats.NewGaugesWithMultiLabels("backendSQLResponse99MaxCounts",
		"arana proxy backend sql sqlTimings P99 max", []string{statsLabelCluster, statsLabelNamespace, statsLabelIPAddr})
	s.backendSQLResponse99AvgCounts = stats.NewGaugesWithMultiLabels("backendSQLResponse99AvgCounts",
		"arana proxy backend sql sqlTimings P99 avg", []string{statsLabelCluster, statsLabelNamespace, statsLabelIPAddr})
	s.backendSQLResponse95MaxCounts = stats.NewGaugesWithMultiLabels("backendSQLResponse95MaxCounts",
		"arana proxy backend sql sqlTimings P95 max", []string{statsLabelCluster, statsLabelNamespace, statsLabelIPAddr})
	s.backendSQLResponse95AvgCounts = stats.NewGaugesWithMultiLabels("backendSQLResponse95AvgCounts",
		"arana proxy backend sql sqlTimings P95 avg", []string{statsLabelCluster, statsLabelNamespace, statsLabelIPAddr})
	s.uptimeCounts = stats.NewGaugesWithMultiLabels("UptimeCounts",
		"arana proxy uptime counts", []string{statsLabelCluster})
	s.clientConnecions = sync.Map{}
	s.startClearTask()
	return nil
}

// Close close proxy stats
func (s *StatisticManager) Close() {
	close(s.closeChan)
}

// GetHandlers return specific handler of stats
func (s *StatisticManager) GetHandlers() map[string]http.Handler {
	return s.handlers
}

func (s *StatisticManager) initBackend(cfg *config.StatsConfig) error {
	prometheus.Init(cfg.Service)
	s.handlers = prometheus.GetHandlers()
	return nil
}

// clear data to prevent
func (s *StatisticManager) startClearTask() {
	go func() {
		t := time.NewTicker(time.Hour)
		for {
			select {
			case <-s.closeChan:
				return
			case <-t.C:
				s.clearLargeCounters()
			}
		}
	}()
}

func (s *StatisticManager) clearLargeCounters() {
	s.sqlErrorCounts.ResetAll()

	s.backendSQLErrorCounts.ResetAll()
	s.backendSQLFingerprintSlowCounts.ResetAll()
}

func (s *StatisticManager) recordSessionErrorSQLFingerprint(namespace string, operation string, md5 string) {
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.sqlErrorCounts.Add(operationStatsKey, 1)
}

func (s *StatisticManager) recordSessionSQLTiming(namespace string, operation string, startTime time.Time) {
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.sqlTimings.Record(operationStatsKey, startTime)
}

// millisecond duration
func (s *StatisticManager) isBackendSlowSQL(startTime time.Time) bool {
	duration := time.Since(startTime).Nanoseconds() / int64(time.Millisecond)
	return duration > s.slowSQLTime || s.slowSQLTime == 0
}

func (s *StatisticManager) recordBackendSlowSQLFingerprint(namespace string, md5 string) {
	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
	s.backendSQLFingerprintSlowCounts.Add(fingerprintStatsKey, 1)
}

func (s *StatisticManager) recordBackendErrorSQLFingerprint(namespace string, operation string, md5 string) {
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.backendSQLErrorCounts.Add(operationStatsKey, 1)
}

func (s *StatisticManager) recordBackendSQLTiming(namespace string, operation string, sliceName, backendAddr string, startTime time.Time) {
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.backendSQLTimings.Record(operationStatsKey, startTime)

	if !s.SQLResponsePercentile[namespace].sqlExecTimeRecordSwitch {
		return
	}
	execTime := float64(time.Since(startTime).Milliseconds())
	select {
	case s.SQLResponsePercentile[namespace].sqlExecTimeChan <- fmt.Sprintf(sliceName + "__" + backendAddr + "__" + fmt.Sprintf("%f", execTime)):
	case <-time.After(time.Millisecond):
		s.SQLResponsePercentile[namespace].sqlExecTimeRecordSwitch = false
	}
}

// IncrSessionCount incr session count
func (s *StatisticManager) IncrSessionCount(namespace string) {
	statsKey := []string{s.clusterName, namespace}
	s.sessionCounts.Add(statsKey, 1)
}

func (s *StatisticManager) IncrConnectionCount(namespace string) {
	if value, ok := s.clientConnecions.Load(namespace); !ok {
		s.clientConnecions.Store(namespace, atomic.NewInt32(1))
	} else {
		lastNum := value.(*atomic.Int32)
		lastNum.Inc()
	}
}

// DescSessionCount decr session count
func (s *StatisticManager) DescSessionCount(namespace string) {
	statsKey := []string{s.clusterName, namespace}
	s.sessionCounts.Add(statsKey, -1)
}

func (s *StatisticManager) DescConnectionCount(namespace string) {
	if value, ok := s.clientConnecions.Load(namespace); !ok {
		log.Warn("namespace: '%v' maxClientConnections should in map", namespace)
	} else {
		lastNum := value.(*atomic.Int32)
		lastNum.Dec()
	}
}

// AddReadFlowCount add read flow count
func (s *StatisticManager) AddReadFlowCount(namespace string, byteCount int) {
	statsKey := []string{s.clusterName, namespace, "read"}
	s.flowCounts.Add(statsKey, int64(byteCount))
}

// AddWriteFlowCount add write flow count
func (s *StatisticManager) AddWriteFlowCount(namespace string, byteCount int) {
	statsKey := []string{s.clusterName, namespace, "write"}
	s.flowCounts.Add(statsKey, int64(byteCount))
}

// record idle connect count
func (s *StatisticManager) recordConnectPoolIdleCount(namespace string, slice string, addr string, count int64) {
	statsKey := []string{s.clusterName, namespace, slice, addr}
	s.backendConnectPoolIdleCounts.Set(statsKey, count)
}

// record in-use connect count
func (s *StatisticManager) recordConnectPoolInuseCount(namespace string, slice string, addr string, count int64) {
	statsKey := []string{s.clusterName, namespace, slice, addr}
	s.backendConnectPoolInUseCounts.Set(statsKey, count)
}

// record wait queue length
func (s *StatisticManager) recordConnectPoolWaitCount(namespace string, slice string, addr string, count int64) {
	statsKey := []string{s.clusterName, namespace, slice, addr}
	s.backendConnectPoolWaitCounts.Set(statsKey, count)
}

// record wait queue length
func (s *StatisticManager) recordInstanceCount(namespace string, slice string, addr string, count int64, role string) {
	statsKey := []string{s.clusterName, namespace, slice, addr, role}
	s.backendInstanceCounts.Set(statsKey, count)
}

// record wait queue length
func (s *StatisticManager) recordBackendSQLTimingP99Max(namespace, backendAddr string, count int64) {
	statsKey := []string{s.clusterName, namespace, backendAddr}
	s.backendSQLResponse99MaxCounts.Set(statsKey, count)
}

func (s *StatisticManager) recordBackendSQLTimingP99Avg(namespace, backendAddr string, count int64) {
	statsKey := []string{s.clusterName, namespace, backendAddr}
	s.backendSQLResponse99AvgCounts.Set(statsKey, count)
}

func (s *StatisticManager) recordBackendSQLTimingP95Max(namespace, backendAddr string, count int64) {
	statsKey := []string{s.clusterName, namespace, backendAddr}
	s.backendSQLResponse95MaxCounts.Set(statsKey, count)
}

func (s *StatisticManager) recordBackendSQLTimingP95Avg(namespace, backendAddr string, count int64) {
	statsKey := []string{s.clusterName, namespace, backendAddr}
	s.backendSQLResponse95AvgCounts.Set(statsKey, count)
}

// AddUptimeCount add uptime count
func (s *StatisticManager) AddUptimeCount(count int64) {
	statsKey := []string{s.clusterName}
	s.uptimeCounts.Set(statsKey, count)
}

func (s *StatisticManager) CalcAvgSQLTimes() {
	for ns, sQLResponse := range s.SQLResponsePercentile {
		sqlTimes := make([]float64, 0)
		quit := false
		backendAddr := ""
		for !quit {
			select {
			case tmp := <-sQLResponse.sqlExecTimeChan:
				if len(sqlTimes) >= SQLExecTimeSize {
					quit = true
				}
				sqlTimeSplit := strings.Split(tmp, "__")
				if len(sqlTimeSplit) < 3 {
					log.Warnf("sql time format error.get:%s", tmp)
					quit = true
				}
				backendAddr = sqlTimeSplit[1]
				etime, _ := strconv.ParseFloat(sqlTimeSplit[2], 64)
				sqlTimes = append(sqlTimes, etime)
			case <-time.After(time.Millisecond):
				quit = true
			}
			sort.Float64s(sqlTimes)

			sum := 0.0
			p99sum := 0.0
			p95sum := 0.0
			if len(sqlTimes) != 0 {
				s.SQLResponsePercentile[ns].response99Max[backendAddr] = sqlTimes[(len(sqlTimes)-1)*99/100]
				s.SQLResponsePercentile[ns].response95Max[backendAddr] = sqlTimes[(len(sqlTimes)-1)*95/100]
			}
			for k, _ := range sqlTimes {
				sum += sqlTimes[k]
				if k < len(sqlTimes)*95/100 {
					p95sum += sqlTimes[k]
				}
				if k < len(sqlTimes)*99/100 {
					p99sum += sqlTimes[k]
				}
			}
			if len(sqlTimes)*99/100 > 0 {
				s.SQLResponsePercentile[ns].response99Avg[backendAddr] = p99sum / float64(len(sqlTimes)*99/100)
			}
			if len(sqlTimes)*95/100 > 0 {
				s.SQLResponsePercentile[ns].response95Avg[backendAddr] = p95sum / float64(len(sqlTimes)*95/100)
			}

			s.SQLResponsePercentile[ns].sqlExecTimeRecordSwitch = true
		}
	}
}
