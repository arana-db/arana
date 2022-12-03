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

package schema

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

import (
	lru "github.com/hashicorp/golang-lru"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
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

func init() {
	proto.RegisterSchemaLoader(NewSimpleSchemaLoader())
}

type SimpleSchemaLoader struct {
	// key format is schema.table
	metadataCache *lru.Cache
	mutex         *sync.RWMutex
}

func NewSimpleSchemaLoader() *SimpleSchemaLoader {
	cache, err := lru.New(100)
	if err != nil {
		panic(err)
	}
	schemaLoader := &SimpleSchemaLoader{
		metadataCache: cache,
		mutex:         new(sync.RWMutex),
	}
	go schemaLoader.refresh()
	return schemaLoader
}

func (l *SimpleSchemaLoader) refresh() {
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		l.mutex.Lock()
		for l.metadataCache.Len() > 0 {
			l.metadataCache.RemoveOldest()
		}
		l.mutex.Unlock()
	}
}

func (l *SimpleSchemaLoader) Load(ctx context.Context, schema string, tables []string) (map[string]*proto.TableMetadata, error) {
	var (
		tableMetadataMap  = make(map[string]*proto.TableMetadata, len(tables))
		indexMetadataMap  map[string][]*proto.IndexMetadata
		columnMetadataMap map[string][]*proto.ColumnMetadata
		queryTables       = make([]string, 0, len(tables))
	)

	if len(schema) > 0 {
		for _, table := range tables {
			qualifiedTblName := schema + "." + table
			l.mutex.RLock()
			if tableMetadata, ok := l.metadataCache.Get(qualifiedTblName); ok {
				tableMetadataMap[table] = tableMetadata.(*proto.TableMetadata)
			} else {
				queryTables = append(queryTables, table)
			}
			l.mutex.RUnlock()
		}
	} else {
		copy(queryTables, tables)
	}

	if len(queryTables) == 0 {
		return tableMetadataMap, nil
	}

	ctx = rcontext.WithRead(rcontext.WithDirect(ctx))

	var err error
	if columnMetadataMap, err = l.LoadColumnMetadataMap(ctx, schema, queryTables); err != nil {
		return nil, errors.WithStack(err)
	}

	if columnMetadataMap != nil {
		if indexMetadataMap, err = l.LoadIndexMetadata(ctx, schema, queryTables); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	for tableName, columns := range columnMetadataMap {
		tableMetadataMap[tableName] = proto.NewTableMetadata(tableName, columns, indexMetadataMap[tableName])
		if len(schema) > 0 {
			l.mutex.Lock()
			l.metadataCache.Add(schema+"."+tableName, tableMetadataMap[tableName])
			l.mutex.Unlock()
		}
	}

	return tableMetadataMap, nil
}

func (l *SimpleSchemaLoader) LoadColumnMetadataMap(ctx context.Context, schema string, tables []string) (map[string][]*proto.ColumnMetadata, error) {
	conn, err := runtime.Load(schema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var (
		resultSet proto.Result
		ds        proto.Dataset
	)

	if resultSet, err = conn.Query(ctx, "", getColumnMetadataSQL(tables)); err != nil {
		log.Errorf("Load ColumnMetadata error when call db: %v", err)
		return nil, errors.WithStack(err)
	}

	if ds, err = resultSet.Dataset(); err != nil {
		log.Errorf("Load ColumnMetadata error when call db: %v", err)
		return nil, errors.WithStack(err)
	}

	result := make(map[string][]*proto.ColumnMetadata, 0)
	if ds == nil {
		log.Error("Load ColumnMetadata error because the result is nil")
		return nil, nil
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
			return nil, errors.WithStack(err)
		}

		if err = row.Scan(cells); err != nil {
			return nil, errors.WithStack(err)
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

	return result, nil
}

func convertInterfaceToStrNullable(value proto.Value) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func (l *SimpleSchemaLoader) LoadIndexMetadata(ctx context.Context, schema string, tables []string) (map[string][]*proto.IndexMetadata, error) {
	conn, err := runtime.Load(schema)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		resultSet proto.Result
		ds        proto.Dataset
	)

	if resultSet, err = conn.Query(ctx, "", getIndexMetadataSQL(tables)); err != nil {
		return nil, errors.WithStack(err)
	}

	if ds, err = resultSet.Dataset(); err != nil {
		return nil, errors.WithStack(err)
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
			return nil, errors.WithStack(err)
		}

		if err = row.Scan(values); err != nil {
			return nil, errors.WithStack(err)
		}

		tableName := convertInterfaceToStrNullable(values[0])
		indexName := convertInterfaceToStrNullable(values[1])
		result[tableName] = append(result[tableName], &proto.IndexMetadata{Name: indexName})
	}

	return result, nil
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
