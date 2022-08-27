## 配置中心

### Root

```azure
ARANA_ROOT=/arana
```

### Tenants 
```yaml
# 路径 Key
${ARANA_ROOT}/v1/tenants/<tenant>
/arana/v1/tenants/arana

# 存储内容
- name: arana
  users:
    - username: root
      password: "123456"
    - username: arana
      password: "123456"
```

### Nodes
```yaml
# 路径 Key
${ARANA_ROOT}/v1/tenants/<tenant>/nodes/<node>
/arana/v1/tenants/arana/nodes/node0
/arana/v1/tenants/arana/nodes/node1

# 存储内容
- name: node0
  host: arana-mysql
  port: 3306
  username: root
  password: "123456"
  database: employees_0000
  weight: r10w10
  parameters:
```

### Data Source Clusters
```yaml
# 路径 Key
${ARANA_ROOT}/v1/tenants/<tenant>/clusters/<cluster>/groups/<group>/nodes/<node>
/arana/v1/tenants/arana/clusters/employees/groups/employees_0000/nodes/node0
/arana/v1/tenants/arana/clusters/employees/groups/employees_0000/nodes/node0_r_0

# 存储内容
- name: node0
  host: arana-mysql
  port: 3306
  username: root
  password: "123456"
  database: employees_0000
  weight: r10w10
  parameters:


# 路径 Key
${ARANA_ROOT}/v1/tenants/<tenant>/clusters/<cluster>/groups/<group>
/arana/v1/tenants/arana/clusters/employees/groups/employees_0000
/arana/v1/tenants/arana/clusters/employees/groups/employees_0001

# 存储内容
nodes:
  - node0
  - node1
```

### Sharding Rule
```yaml
# 路径 Key
${ARANA_ROOT}/v1/tenants/<tenant>/clusters/<cluster>/tables/<table>
/arana/v1/tenants/arana/clusters/employees/tables/student
/arana/v1/tenants/arana/clusters/employees/tables/order

# 存储内容
- name: student
  allow_full_scan: true
  sequence:
    type: snowflake
    option:
  db_rules:
    - column: uid
      type: scriptExpr
      expr: parseInt($value % 32 / 8)
  tbl_rules:
    - column: uid
      type: scriptExpr
      expr: $value % 32
      step: 32
  topology:
    db_pattern: employees_${0000..0003}
    tbl_pattern: student_${0000..0031}
  attributes:
    sqlMaxLimit: -1
```
