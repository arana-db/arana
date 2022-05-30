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

package schema_manager

import (
	"context"
	"fmt"
	"io"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/log"
)

const (
	orderByOrdinalPosition   = " ORDER BY ORDINAL_POSITION"
	tableMetadataNoOrder     = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_KEY, EXTRA, COLLATION_NAME, ORDINAL_POSITION FROM information_schema.columns WHERE TABLE_SCHEMA=database()"
	tableMetadataSQL         = tableMetadataNoOrder + orderByOrdinalPosition
	tableMetadataSQLInTables = tableMetadataNoOrder + " AND TABLE_NAME IN (%s)" + orderByOrdinalPosition
	indexMetadataSQL         = "SELECT TABLE_NAME, INDEX_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA=database() AND TABLE_NAME IN (%s)"
)

type SimpleSchemaLoader struct{}

func (l *SimpleSchemaLoader) Load(ctx context.Context, conn proto.VConn, schema string, tables []string) map[string]*proto.TableMetadata {
	ctx = rcontext.WithRead(rcontext.WithDirect(ctx))
	var (
		tableMetadataMap  = make(map[string]*proto.TableMetadata, len(tables))
		indexMetadataMap  map[string][]*proto.IndexMetadata
		columnMetadataMap map[string][]*proto.ColumnMetadata
	)
	columnMetadataMap = l.LoadColumnMetadataMap(ctx, conn, schema, tables)
	if columnMetadataMap != nil {
		indexMetadataMap = l.LoadIndexMetadata(ctx, conn, schema, tables)
	}

	for tableName, columns := range columnMetadataMap {
		tableMetadataMap[tableName] = proto.NewTableMetadata(tableName, columns, indexMetadataMap[tableName])
	}

	return tableMetadataMap
}

func (l *SimpleSchemaLoader) LoadColumnMetadataMap(ctx context.Context, conn proto.VConn, schema string, tables []string) map[string][]*proto.ColumnMetadata {
	var (
		resultSet proto.Result
		ds        proto.Dataset
		err       error
	)

	if resultSet, err = conn.Query(ctx, schema, getColumnMetadataSQL(tables)); err != nil {
		log.Errorf("Load ColumnMetadata error when call db: %v", err)
		return nil
	}

	if ds, err = resultSet.Dataset(); err != nil {
		log.Errorf("Load ColumnMetadata error when call db: %v", err)
		return nil
	}

	result := make(map[string][]*proto.ColumnMetadata, 0)
	if ds == nil {
		log.Error("Load ColumnMetadata error because the result is nil")
		return nil
	}

	var (
		fields, _ = ds.Fields()
		row       proto.Row
		cells     = make([]proto.Value, len(fields))
	)

	for {
		row, err = ds.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil
		}

		if err = row.Scan(cells); err != nil {
			return nil
		}

		tableName := convertInterfaceToStrNullable(cells[0])
		columnName := convertInterfaceToStrNullable(cells[1])
		dataType := convertInterfaceToStrNullable(cells[2])
		columnKey := convertInterfaceToStrNullable(cells[3])
		extra := convertInterfaceToStrNullable(cells[4])
		collationName := convertInterfaceToStrNullable(cells[5])
		ordinalPosition := convertInterfaceToStrNullable(cells[6])
		result[tableName] = append(result[tableName], &proto.ColumnMetadata{
			Name:          columnName,
			DataType:      dataType,
			Ordinal:       ordinalPosition,
			PrimaryKey:    strings.EqualFold("PRI", columnKey),
			Generated:     strings.EqualFold("auto_increment", extra),
			CaseSensitive: columnKey != "" && !strings.HasSuffix(collationName, "_ci"),
		})
	}

	return result
}

func convertInterfaceToStrNullable(value proto.Value) string {
	if value == nil {
		return ""
	}

	switch val := value.(type) {
	case string:
		return val
	default:
		return fmt.Sprint(val)
	}
}

func (l *SimpleSchemaLoader) LoadIndexMetadata(ctx context.Context, conn proto.VConn, schema string, tables []string) map[string][]*proto.IndexMetadata {
	var (
		resultSet proto.Result
		err       error
		ds        proto.Dataset
	)

	if resultSet, err = conn.Query(ctx, schema, getIndexMetadataSQL(tables)); err != nil {
		return nil
	}

	if ds, err = resultSet.Dataset(); err != nil {
		return nil
	}

	var (
		fields, _ = ds.Fields()
		row       proto.Row
		values    = make([]proto.Value, len(fields))
		result    = make(map[string][]*proto.IndexMetadata, 0)
	)

	for {
		row, err = ds.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil
		}

		if err = row.Scan(values); err != nil {
			return nil
		}

		tableName := convertInterfaceToStrNullable(values[0])
		indexName := convertInterfaceToStrNullable(values[1])
		result[tableName] = append(result[tableName], &proto.IndexMetadata{Name: indexName})
	}

	return result
}

func getIndexMetadataSQL(tables []string) string {
	tableParamList := make([]string, 0, len(tables))
	for _, table := range tables {
		tableParamList = append(tableParamList, "'"+table+"'")
	}
	return fmt.Sprintf(indexMetadataSQL, strings.Join(tableParamList, ","))
}

func getColumnMetadataSQL(tables []string) string {
	if len(tables) == 0 {
		return tableMetadataSQL
	}
	tableParamList := make([]string, len(tables))
	for i, table := range tables {
		tableParamList[i] = "'" + table + "'"
	}
	// TODO use strings.Builder in the future
	return fmt.Sprintf(tableMetadataSQLInTables, strings.Join(tableParamList, ","))
}
