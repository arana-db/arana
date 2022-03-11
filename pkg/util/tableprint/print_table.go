//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package utils

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

import (
	"github.com/olekukonko/tablewriter"
)

// PrintTable prints all rows as table format.
func PrintTable(rows *sql.Rows) ([][]string, error) {
	return writeTable(os.Stdout, rows, true)
}

// WriteTable writes table into writer.
func WriteTable(w io.Writer, rows *sql.Rows) error {
	_, err := writeTable(w, rows, false)
	return err
}

// WriteTableColor writes colorful table into writer.
func WriteTableColor(w io.Writer, rows *sql.Rows) error {
	_, err := writeTable(w, rows, true)
	return err
}

func writeTable(w io.Writer, rows *sql.Rows, color bool) ([][]string, error) {
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

	header := make([]string, 0, n)
	// print labels
	for i := 0; i < n; i++ {
		var h string
		if len(c[i]) > 1 && c[i][1] == ':' {
			h = c[i][2:]
		} else {
			h = c[i]
		}

		if color {
			h = fmt.Sprintf("\033[32m%s\033[0m", h)
		}

		header = append(header, h)
	}

	table := tablewriter.NewWriter(w)
	table.SetHeader(header)
	table.SetAutoFormatHeaders(false)

	// print data
	field := make([]interface{}, 0, n)
	for i := 0; i < n; i++ {
		field = append(field, new(sql.NullString))
	}

	var converts [][]string

	for rows.Next() {
		if err := rows.Scan(field...); err != nil {
			return nil, err
		}
		row := make([]string, 0, n)
		for i := 0; i < n; i++ {
			col := pr(field[i])
			row = append(row, col)
		}

		converts = append(converts, row)

		table.Append(row)
	}

	table.Render()

	return converts, nil
}
