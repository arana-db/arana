# Sample data storage format in etcd

### /arana-db/config/data/dataSourceClusters

```json
[
    {
        "name":"employees",
        "type":"mysql",
        "sql_max_limit":-1,
        "tenant":"arana",
        "conn_props":{
            "capacity":10,
            "max_capacity":20,
            "idle_timeout":60
        },
        "groups":[
            {
                "name":"employees_0000",
                "nodes":[
                    {
                        "name":"arana-node-1",
                        "host":"arana-mysql",
                        "port":3306,
                        "username":"root",
                        "password":"123456",
                        "database":"employees",
                        "conn_props":{
                            "charset":"utf8mb4,utf8",
                            "loc":"Local",
                            "parseTime":"true",
                            "readTimeout":"1s",
                            "writeTimeout":"1s"
                        },
                        "weight":"r10w10",
                        "labels":{
                            "zone":"shanghai"
                        }
                    }
                ]
            }
        ]
    }
]
```

### /arana-db/config/data/filters

### /arana-db/config/data/listeners

```json
[
    {
        "protocol_type":"mysql",
        "socket_address":{
            "address":"0.0.0.0",
            "port":13306
        },
        "server_version":"5.7.0"
    }
]
```

### /arana-db/config/data/shardingRule

```json
{
    "tables":[
        {
            "name":"employees.student",
            "allow_full_scan":true,
            "db_rules":null,
            "tbl_rules":[
                {
                    "column":"uid",
                    "expr":"modShard(32)"
                }
            ],
            "topology":{
                "db_pattern":"employees_0000",
                "tbl_pattern":"student_${0000...0031}"
            },
            "shadow_topology":null,
            "attributes":{
                "sqlMaxLimit":"-1"
            }
        }
    ]
}
```

### /arana-db/config/data/tenants

```json
[
    {
        "name":"arana",
        "users":[
            {
                "username":"arana",
                "password":"123456"
            },
            {
                "username":"dksl",
                "password":"123456"
            }
        ]
    }
]
```

# /arana-db/config/metadata

{"name":"arana-config"}
