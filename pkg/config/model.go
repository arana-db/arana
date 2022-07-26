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

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

import (
	"github.com/go-playground/validator/v10"

	"github.com/pkg/errors"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"gopkg.in/yaml.v3"
)

type (
	DataRevision interface {
		Revision() string
	}

	// Configuration represents an Arana configuration.
	Configuration struct {
		Kind       string                 `yaml:"kind" json:"kind,omitempty"`
		APIVersion string                 `yaml:"apiVersion" json:"apiVersion,omitempty"`
		Metadata   map[string]interface{} `yaml:"metadata" json:"metadata"`
		Data       *Data                  `validate:"required,structonly" yaml:"data" json:"data"`
	}

	// DataSourceType is the data source type
	DataSourceType string

	// SocketAddress specify either a logical or physical address and port, which are
	// used to tell server where to bind/listen, connect to upstream and find
	// management servers
	SocketAddress struct {
		Address string `default:"0.0.0.0" yaml:"address" json:"address"`
		Port    int    `default:"13306" yaml:"port" json:"port"`
	}

	Data struct {
		Listeners          []*Listener          `validate:"required,dive" yaml:"listeners" json:"listeners"`
		Tenants            []*Tenant            `validate:"required,dive" yaml:"tenants" json:"tenants"`
		DataSourceClusters []*DataSourceCluster `validate:"required,dive" yaml:"clusters" json:"clusters"`
		ShardingRule       *ShardingRule        `validate:"required,dive" yaml:"sharding_rule,omitempty" json:"sharding_rule,omitempty"`
		ShadowRule         *ShadowRule          `yaml:"shadow_rule,omitempty" json:"shadow_rule,omitempty"`
	}

	Tenant struct {
		Name  string  `validate:"required" yaml:"name" json:"name"`
		Users []*User `validate:"required" yaml:"users" json:"users"`
	}

	DataSourceCluster struct {
		Name        string         `yaml:"name" json:"name"`
		Type        DataSourceType `yaml:"type" json:"type"`
		SqlMaxLimit int            `default:"-1" yaml:"sql_max_limit" json:"sql_max_limit,omitempty"`
		Tenant      string         `yaml:"tenant" json:"tenant"`
		Parameters  ParametersMap  `yaml:"parameters" json:"parameters"`
		Groups      []*Group       `yaml:"groups" json:"groups"`
	}

	Group struct {
		Name  string  `yaml:"name" json:"name"`
		Nodes []*Node `yaml:"nodes" json:"nodes"`
	}

	Node struct {
		Name       string                 `validate:"required" yaml:"name" json:"name"`
		Host       string                 `validate:"required" yaml:"host" json:"host"`
		Port       int                    `validate:"required" yaml:"port" json:"port"`
		Username   string                 `validate:"required" yaml:"username" json:"username"`
		Password   string                 `validate:"required" yaml:"password" json:"password"`
		Database   string                 `validate:"required" yaml:"database" json:"database"`
		Parameters ParametersMap          `yaml:"parameters" json:"parameters"`
		ConnProps  map[string]interface{} `yaml:"conn_props" json:"conn_props,omitempty"`
		Weight     string                 `default:"r10w10" yaml:"weight" json:"weight"`
		Labels     map[string]string      `yaml:"labels" json:"labels,omitempty"`
	}

	ShardingRule struct {
		Tables []*Table `yaml:"tables" json:"tables"`
	}

	ShadowRule struct {
		ShadowTables []*ShadowTable `yaml:"tables" json:"tables"`
	}

	ShadowTable struct {
		Name       string       `yaml:"name" json:"name"`
		Enable     bool         `yaml:"enable" json:"enable"`
		GroupNode  string       `yaml:"group_node" json:"group_node"`
		MatchRules []*MatchRule `yaml:"match_rules" json:"match_rules"`
	}

	MatchRule struct {
		Operation  []string         `yaml:"operation" json:"operation"`
		MatchType  string           `yaml:"match_type" json:"match_type"`
		Attributes []*RuleAttribute `yaml:"attributes" json:"attributes"`
	}

	RuleAttribute struct {
		Column string `yaml:"column" json:"column"`
		Value  string `yaml:"value,omitempty" json:"value,omitempty"`
		Regex  string `yaml:"regex,omitempty" json:"regex,omitempty"`
	}

	Prop struct {
		Operation string `yaml:"operation" json:"operation"`
		Column    string `yaml:"column" json:"column"`
		Value     string `yaml:"value" json:"value"`
		Regex     string `yaml:"regex" json:"regex"`
	}

	Listener struct {
		ProtocolType  string         `yaml:"protocol_type" json:"protocol_type"`
		SocketAddress *SocketAddress `yaml:"socket_address" json:"socket_address"`
		ServerVersion string         `yaml:"server_version" json:"server_version"`
	}

	User struct {
		Username string `yaml:"username" json:"username"`
		Password string `yaml:"password" json:"password"`
	}

	Table struct {
		Name           string            `validate:"required" yaml:"name" json:"name"`
		Sequence       *Sequence         `yaml:"sequence" json:"sequence"`
		AllowFullScan  bool              `yaml:"allow_full_scan" json:"allow_full_scan,omitempty"`
		DbRules        []*Rule           `yaml:"db_rules" json:"db_rules"`
		TblRules       []*Rule           `yaml:"tbl_rules" json:"tbl_rules"`
		Topology       *Topology         `yaml:"topology" json:"topology"`
		ShadowTopology *Topology         `yaml:"shadow_topology" json:"shadow_topology"`
		Attributes     map[string]string `yaml:"attributes" json:"attributes"`
	}

	Sequence struct {
		Type   string            `yaml:"type" json:"type"`
		Option map[string]string `yaml:"option" json:"option"`
	}

	Rule struct {
		Column string `validate:"required" yaml:"column" json:"column"`
		Type   string `validate:"required" yaml:"type" json:"type"`
		Expr   string `validate:"required" yaml:"expr" json:"expr"`
		Step   int    `yaml:"step" json:"step"`
	}

	Topology struct {
		DbPattern  string `validate:"required" yaml:"db_pattern" json:"db_pattern"`
		TblPattern string `validate:"required" yaml:"tbl_pattern" json:"tbl_pattern"`
	}
)

type ParametersMap map[string]string

func (pm *ParametersMap) Merge(parametersMap ParametersMap) {
	for key, val := range parametersMap {
		if _, ok := (*pm)[key]; !ok {
			(*pm)[key] = val
		}
	}
}

func (pm *ParametersMap) String() string {
	sBuff := strings.Builder{}
	for k, v := range *pm {
		sBuff.WriteString(pm.LowerCaseFirstLetter(pm.Camel(k)))
		sBuff.WriteString("=")
		sBuff.WriteString(v)
		sBuff.WriteString("&")
	}
	return strings.TrimRight(sBuff.String(), "&")
}

// Camel underline to camel
func (pm *ParametersMap) Camel(name string) string {
	name = strings.Replace(name, "_", " ", -1)
	name = cases.Title(language.English, cases.NoLower).String(name)
	return strings.Replace(name, " ", "", -1)
}

// LowerCaseFirstLetter lowercase letter
func (pm *ParametersMap) LowerCaseFirstLetter(str string) string {
	return string(unicode.ToLower(rune(str[0]))) + str[1:]
}

// Decoder decodes configuration.
type Decoder struct {
	reader io.Reader
}

func (d *Decoder) Decode(v interface{}) error {
	if err := yaml.NewDecoder(d.reader).Decode(v); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// NewDecoder creates a Decoder from a reader.
func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{reader: reader}
}

// Load loads the configuration from file path.
func Load(path string) (*Configuration, error) {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Open(path); err != nil {
		return nil, errors.Wrap(err, "failed to load configuration file")
	}
	defer func() {
		_ = f.Close()
	}()

	var cfg Configuration
	if err = NewDecoder(f).Decode(&cfg); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal config")
	}
	return &cfg, nil
}

var _weightRegexp = regexp.MustCompile(`^[rR]([0-9]+)[wW]([0-9]+)$`)

func (d *Node) GetReadAndWriteWeight() (int, int, error) {
	items := _weightRegexp.FindStringSubmatch(d.Weight)
	if len(items) != 3 {
		return 0, 0, errors.New("weight config should be r10w10")
	}
	readWeight, err := strconv.Atoi(items[1])
	if err != nil {
		return 0, 0, err
	}
	writeWeight, err := strconv.Atoi(items[2])
	if err != nil {
		return 0, 0, err
	}

	return readWeight, writeWeight, nil
}

func (d *Node) String() string {
	b, _ := json.Marshal(d)
	return string(b)
}

func (t *ProtocolType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *ProtocolType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return errors.Errorf("unrecognized protocol type: %q", text)
	}
	return nil
}

func (t *ProtocolType) unmarshalText(text []byte) bool {
	protocolType := string(text)
	switch protocolType {
	case "mysql":
		*t = MySQL
	case "http":
		*t = Http
	default:
		return false
	}
	return true
}

// Validate validates the input configuration.
func Validate(cfg *Configuration) error {
	v := validator.New()
	return v.Struct(cfg)
}

// GetConnPropCapacity parses the capacity of backend connection pool, return default value if failed.
func GetConnPropCapacity(connProps map[string]interface{}, defaultValue int) int {
	capacity, ok := connProps["capacity"]
	if !ok {
		return defaultValue
	}
	n, _ := strconv.Atoi(fmt.Sprint(capacity))
	if n < 1 {
		return defaultValue
	}
	return n
}

// GetConnPropMaxCapacity parses the max capacity of backend connection pool, return default value if failed.
func GetConnPropMaxCapacity(connProps map[string]interface{}, defaultValue int) int {
	var (
		maxCapacity interface{}
		ok          bool
	)

	if maxCapacity, ok = connProps["max_capacity"]; !ok {
		if maxCapacity, ok = connProps["maxCapacity"]; !ok {
			return defaultValue
		}
	}
	n, _ := strconv.Atoi(fmt.Sprint(maxCapacity))
	if n < 1 {
		return defaultValue
	}
	return n
}

// GetConnPropIdleTime parses the idle time of backend connection pool, return default value if failed.
func GetConnPropIdleTime(connProps map[string]interface{}, defaultValue time.Duration) time.Duration {
	var (
		idleTime interface{}
		ok       bool
	)

	if idleTime, ok = connProps["idle_time"]; !ok {
		if idleTime, ok = connProps["idleTime"]; !ok {
			return defaultValue
		}
	}

	s := fmt.Sprint(idleTime)
	d, _ := time.ParseDuration(s)
	if d > 0 {
		return d
	}

	n, _ := strconv.Atoi(s)
	if n < 1 {
		return defaultValue
	}

	return time.Duration(n) * time.Second
}

type (
	Clusters []*DataSourceCluster
	Tenants  []*Tenant
	Nodes    []*Node
	Groups   []*Group
	Users    []*User
	Rules    []*Rule
)

func (r Rules) Compare(o Rules) bool {
	if len(r) == 0 && len(o) == 0 {
		return true
	}

	if len(r) != len(o) {
		return false
	}

	newT := make([]*Rule, 0, 4)
	updateT := make([]*Rule, 0, 4)
	deleteT := make([]*Rule, 0, 4)

	newTmp := map[string]*Rule{}
	oldTmp := map[string]*Rule{}

	for i := range r {
		newTmp[r[i].Column] = r[i]
	}
	for i := range o {
		oldTmp[o[i].Column] = o[i]
	}

	for i := range r {
		if _, ok := oldTmp[o[i].Column]; !ok {
			newT = append(newT, o[i])
		}
	}

	for i := range o {
		val, ok := newTmp[o[i].Column]
		if !ok {
			deleteT = append(deleteT, o[i])
			continue
		}

		if !reflect.DeepEqual(val, o[i]) {
			updateT = append(updateT, val)
			continue
		}
	}

	return len(newT) == 0 && len(updateT) == 0 && len(deleteT) == 0
}

func (t *Table) Compare(o *Table) bool {
	if len(t.DbRules) != len(o.DbRules) {
		return false
	}

	if len(t.TblRules) != len(o.TblRules) {
		return false
	}

	if !Rules(t.DbRules).Compare(o.DbRules) {
		return false
	}
	if !Rules(t.TblRules).Compare(o.TblRules) {
		return false
	}

	if !reflect.DeepEqual(t.Topology, o.Topology) || !reflect.DeepEqual(t.ShadowTopology, o.ShadowTopology) {
		return false
	}

	if t.AllowFullScan == o.AllowFullScan {
		return false
	}

	if !reflect.DeepEqual(t.Attributes, o.Attributes) {
		return false
	}

	return true
}

func (s *ShardingRule) Diff(old *ShardingRule) *ShardingRuleEvent {
	newT := make([]*Table, 0, 4)
	updateT := make([]*Table, 0, 4)
	deleteT := make([]*Table, 0, 4)

	newTmp := map[string]*Table{}
	oldTmp := map[string]*Table{}

	for i := range s.Tables {
		newTmp[s.Tables[i].Name] = s.Tables[i]
	}
	for i := range old.Tables {
		oldTmp[old.Tables[i].Name] = old.Tables[i]
	}

	for i := range s.Tables {
		if _, ok := oldTmp[s.Tables[i].Name]; !ok {
			newT = append(newT, s.Tables[i])
		}
	}

	for i := range old.Tables {
		val, ok := newTmp[old.Tables[i].Name]
		if !ok {
			deleteT = append(deleteT, old.Tables[i])
			continue
		}

		if !reflect.DeepEqual(val, old.Tables[i]) {
			updateT = append(updateT, val)
			continue
		}
	}

	return &ShardingRuleEvent{
		AddTables:    newT,
		UpdateTables: updateT,
		DeleteTables: deleteT,
	}
}
