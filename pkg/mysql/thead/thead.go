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

package thead

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
)

var (
	Topology = Thead{
		Col{Name: "id", FieldType: consts.FieldTypeLongLong},
		Col{Name: "group_name", FieldType: consts.FieldTypeVarString},
		Col{Name: "table_name", FieldType: consts.FieldTypeVarString},
	}
	Database = Thead{
		Col{Name: "Database", FieldType: consts.FieldTypeVarString},
	}
	Nodes = Thead{
		Col{Name: "node", FieldType: consts.FieldTypeVarString},
		Col{Name: "cluster", FieldType: consts.FieldTypeVarString},
		Col{Name: "database", FieldType: consts.FieldTypeVarString},
		Col{Name: "host", FieldType: consts.FieldTypeVarString},
		Col{Name: "port", FieldType: consts.FieldTypeLong},
		Col{Name: "user_name", FieldType: consts.FieldTypeVarString},
		Col{Name: "weight", FieldType: consts.FieldTypeVarString},
		Col{Name: "parameters", FieldType: consts.FieldTypeVarString},
	}
	Users = Thead{
		Col{Name: "user_name", FieldType: consts.FieldTypeVarString},
	}
	GroupSequence = Thead{
		Col{Name: "type", FieldType: consts.FieldTypeVarString},
		Col{Name: "seq_val", FieldType: consts.FieldTypeVarString},
		Col{Name: "step", FieldType: consts.FieldTypeVarString},
	}
	SnowflakeSequence = Thead{
		Col{Name: "type", FieldType: consts.FieldTypeVarString},
		Col{Name: "work_id", FieldType: consts.FieldTypeVarString},
		Col{Name: "node_id", FieldType: consts.FieldTypeVarString},
	}
	ShardingRule = Thead{
		Col{Name: "name", FieldType: consts.FieldTypeVarString},
		Col{Name: "sequence_type", FieldType: consts.FieldTypeVarString},
		Col{Name: "db_rules", FieldType: consts.FieldTypeVarString},
		Col{Name: "tbl_rules", FieldType: consts.FieldTypeVarString},
		Col{Name: "attributes", FieldType: consts.FieldTypeVarString},
	}

	DBRule = Thead{
		Col{Name: "table_name", FieldType: consts.FieldTypeVarString},
		Col{Name: "column", FieldType: consts.FieldTypeVarString},
		Col{Name: "type", FieldType: consts.FieldTypeVarString},
		Col{Name: "expr", FieldType: consts.FieldTypeVarString},
		Col{Name: "step", FieldType: consts.FieldTypeInt24},
	}

	TableRule = Thead{
		Col{Name: "table_name", FieldType: consts.FieldTypeVarString},
		Col{Name: "column", FieldType: consts.FieldTypeVarString},
		Col{Name: "type", FieldType: consts.FieldTypeVarString},
		Col{Name: "expr", FieldType: consts.FieldTypeVarString},
		Col{Name: "step", FieldType: consts.FieldTypeVarString},
	}

	OpenTables = Thead{
		Col{Name: "Database", FieldType: consts.FieldTypeVarString},
		Col{Name: "Table", FieldType: consts.FieldTypeVarString},
		Col{Name: "In_use", FieldType: consts.FieldTypeInt24},
		Col{Name: "Name_locked", FieldType: consts.FieldTypeInt24},
	}
)

type Col struct {
	Name      string
	FieldType consts.FieldType
}

type Thead []Col

func (t Thead) ToFields() []proto.Field {
	columns := make([]proto.Field, len(t))
	for i := 0; i < len(t); i++ {
		columns[i] = mysql.NewField(t[i].Name, t[i].FieldType)
	}
	return columns
}
