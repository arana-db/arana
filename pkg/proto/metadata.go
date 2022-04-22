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

package proto

import (
	"strings"
)

type TableMetadata struct {
	Name              string
	Columns           map[string]*ColumnMetadata
	Indexes           map[string]*IndexMetadata
	ColumnNames       []string
	PrimaryKeyColumns []string
}

func NewTableMetadata(name string, columnMetadataList []*ColumnMetadata, indexMetadataList []*IndexMetadata) *TableMetadata {
	tma := &TableMetadata{
		Name:              name,
		Columns:           make(map[string]*ColumnMetadata, 0),
		Indexes:           make(map[string]*IndexMetadata, 0),
		ColumnNames:       make([]string, len(columnMetadataList)),
		PrimaryKeyColumns: make([]string, 0),
	}
	for i, columnMetadata := range columnMetadataList {
		columnName := strings.ToLower(columnMetadata.Name)
		tma.ColumnNames[i] = columnName
		tma.Columns[columnName] = columnMetadata
		if columnMetadata.PrimaryKey {
			tma.PrimaryKeyColumns = append(tma.PrimaryKeyColumns, columnName)
		}
	}
	for _, indexMetadata := range indexMetadataList {
		indexName := strings.ToLower(indexMetadata.Name)
		tma.Indexes[indexName] = indexMetadata
	}

	return tma
}

type ColumnMetadata struct {
	Name string
	// TODO int32
	DataType      string
	Ordinal       string
	PrimaryKey    bool
	Generated     bool
	CaseSensitive bool
}

type IndexMetadata struct {
	Name string
}
