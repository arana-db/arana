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

package test

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

import (
	"github.com/pkg/errors"

	"go.uber.org/zap"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

const (
	RowsAffected = "rowsAffected"
	LastInsertId = "lastInsertId"
	ValueInt     = "valueInt"
	ValueString  = "valueString"
	File         = "file"
)

type (
	// Message expected message or actual message for integration test
	Message struct {
		Kind     string   `yaml:"kind"` // dataset, expected
		MetaData MetaData `yaml:"metadata"`
		Data     []*Data  `yaml:"data"`
	}

	MetaData struct {
		Tables []*Table `json:"tables"`
	}

	Data struct {
		Name  string     `json:"name"`
		Value [][]string `json:"value"`
	}

	Table struct {
		Name    string   `yaml:"name"`
		Columns []Column `yaml:"columns"`
	}

	Column struct {
		Name string `yaml:"name"`
		Type string `yaml:"type"`
	}
)

type (
	Cases struct {
		Kind           string  `yaml:"kind"` // dataset, expected
		ExecCases      []*Case `yaml:"exec_cases"`
		DeleteCases    []*Case `yaml:"delete_cases"`
		QueryRowsCases []*Case `yaml:"query_rows_cases"`
		QueryRowCases  []*Case `yaml:"query_row_cases"`
	}

	Case struct {
		SQL            string    `yaml:"sql"`
		Parameters     string    `yaml:"parameters,omitempty"`
		Type           string    `yaml:"type"`
		Sense          []string  `yaml:"sense"`
		ExpectedResult *Expected `yaml:"expected"`
	}

	Expected struct {
		ResultType string `yaml:"type"`
		Value      string `yaml:"value"`
	}
)

func getDataFrom(rows *sql.Rows) ([][]string, []string, error) {
	pr := func(t interface{}) (r string) {
		r = "\\N"
		switch v := t.(type) {
		case *sql.NullBool:
			if v.Valid {
				r = strconv.FormatBool(v.Bool)
			}
		case *sql.NullString:
			if v.Valid {
				r = v.String
			}
		case *sql.NullInt64:
			if v.Valid {
				r = fmt.Sprintf("%6d", v.Int64)
			}
		case *sql.NullFloat64:
			if v.Valid {
				r = fmt.Sprintf("%.2f", v.Float64)
			}
		case *time.Time:
			if v.Year() > 1900 {
				r = v.Format("_2 Jan 2006")
			}
		default:
			r = fmt.Sprintf("%#v", t)
		}
		return
	}

	c, _ := rows.Columns()
	n := len(c)
	field := make([]interface{}, 0, n)
	for i := 0; i < n; i++ {
		field = append(field, new(sql.NullString))
	}

	var converts [][]string

	for rows.Next() {
		if err := rows.Scan(field...); err != nil {
			return nil, nil, err
		}
		row := make([]string, 0, n)
		for i := 0; i < n; i++ {
			col := pr(field[i])
			row = append(row, col)
		}
		converts = append(converts, row)
	}
	return converts, c, nil
}

func (e *Expected) CompareRows(rows *sql.Rows, actual *Message) error {
	data, columns, err := getDataFrom(rows)
	if err != nil {
		return err
	}

	actualMap := make(map[string][]map[string]string, 10) // table:key:value
	for _, v := range actual.Data {
		tb := actualMap[v.Name]
		if tb == nil {
			actualMap[v.Name] = make([]map[string]string, 0, 1000)
		}
		for _, rows := range v.Value {
			val := make(map[string]string, len(rows))
			for i, row := range rows {
				val[actual.MetaData.Tables[0].Columns[i].Name] = row
				actualMap[v.Name] = append(actualMap[v.Name], val)
			}
		}
	}
	return CompareWithActualSet(columns, data, actualMap)
}

func CompareWithActualSet(columns []string, driverSet [][]string, exceptSet map[string][]map[string]string) error {
	for _, rows := range driverSet {
		foundFlag := false
		// for loop actual data
		for _, actualRows := range exceptSet {
			for _, actualRow := range actualRows {
				if actualRow[columns[0]] == rows[0] {
					foundFlag = true
					for i := 1; i < len(rows); i++ {
						if actualRow[columns[i]] != rows[i] {
							foundFlag = false
						}
					}
				}
			}

			if foundFlag {
				break
			}
		}

		if !foundFlag {
			return errors.New("record not found")
		}
	}

	return nil
}

func (e *Expected) CompareRow(result interface{}) error {
	switch e.ResultType {
	case ValueInt:
		var cnt int
		row, ok := result.(*sql.Row)
		if !ok {
			return errors.New("type error")
		}
		if err := row.Scan(&cnt); err != nil {
			return err
		}
		if e.Value != fmt.Sprint(cnt) {
			return errors.New("not equal")
		}
	case RowsAffected:
		rowsAfferted, _ := result.(sql.Result).RowsAffected()
		if fmt.Sprint(rowsAfferted) != e.Value {
			return errors.New("not equal")
		}
	case LastInsertId:
		lastInsertId, _ := result.(sql.Result).LastInsertId()
		if fmt.Sprint(lastInsertId) != e.Value {
			return errors.New("not equal")
		}

	}

	return nil
}

// LoadYamlConfig load yaml config from path to val, val should be a pointer
func LoadYamlConfig(path string, val interface{}) error {
	if err := validInputIsPtr(val); err != nil {
		log.Fatal("valid conf failed", zap.Error(err))
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err = yaml.NewDecoder(bufio.NewReader(f)).Decode(val); err != nil {
		return err
	}
	return nil
}

func validInputIsPtr(conf interface{}) error {
	tp := reflect.TypeOf(conf)
	if tp.Kind() != reflect.Ptr {
		return errors.New("conf should be pointer")
	}
	return nil
}

func GetValueByType(param string) (interface{}, error) {
	if strings.TrimSpace(param) == "" {
		return nil, nil
	}
	kv := strings.Split(param, ":")
	if len(kv) < 2 {
		return nil, errors.New("invalid params")
	}
	switch strings.ToLower(kv[1]) {
	case "string":
		return kv[0], nil
	case "int":
		return strconv.ParseInt(kv[0], 10, 64)
	case "float":
		return strconv.ParseFloat(kv[0], 64)
	}

	return kv[0], nil
}
