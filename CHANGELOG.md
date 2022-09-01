# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.0] - 2022-09-10
### New Feature
- Support MySQL protocol，`arana` can be used as a MySQL proxy.
- Support as a MySQL proxy for single Database and sharding Database.
- Support MySQL read write split according to weight config.
- Support `insert`, `update`, `delete`, `show` etc SQL statement.
- Support simple query SQL statement, for `limit x,y`, simple `order by` & `group by`, aggregate function.
- Support `etcd` as dynamic configuration center.
- Support `snowflake` as distributed primary key.
- Support single Database transaction.
- Support `MySQL` SQL parsing and generate abstract syntax tree.
- Support multiple sharding algorithms, such as `mod`, `hashMd5`, `hashCrc32`, `hashBKDR`.
- Support JavaScript expression and function expression for sharding algorithms.
