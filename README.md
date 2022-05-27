# arana
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/arana-db/arana/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/arana-db/arana/branch/master/graph/badge.svg)](https://codecov.io/gh/arana-db/arana)
[![Release](https://img.shields.io/github/v/release/arana-db/arana)](https://img.shields.io/github/v/release/arana-db/arana)
[![Docker Pulls](https://img.shields.io/docker/pulls/aranadb/arana)](https://img.shields.io/docker/pulls/aranadb/arana)

![](./docs/pics/arana-logo.png)

|                                             **Stargazers Over Time**                                              | **Contributors Over Time**                                                                                                                                                                                                                       |
|:-----------------------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|      [![Stargazers over time](https://starchart.cc/arana-db/arana.svg)](https://starchart.cc/arana-db/arana)      | [![Contributor over time](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=arana-db/arana)](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=arana-db/arana) |


## Introduction | [中文](https://github.com/arana-db/arana/blob/master/README_CN.md)

> `Arana` is a Cloud Native Database Proxy. It can be deployed as a DBMesh Sidecar. Arana provides transparent data access capabilities, 
> when using database, user doesn't need to care about the `sharding` details of database, they can use `arana-db` just like a single `MySQL` database.
> For other problems caused by sharding, such as `Distributed transaction`, `SQL Aduit`, `Multi Tenant`, `Arana` will provide a complete solution.
> through simple config, user can use these capabilities provided by `arana` directly.

## Architecture

## Features

| Feature | Complete |
| -- | -- |
| Single DB Proxy | √ |
| Read Write Splitting | √ |
| Sharding | √ |
| Multi Tenant | √ |
| Distributed Primary Key | √ |
| Distributed Transaction | WIP |
| Shadow Table | WIP |
| DB Mesh | WIP |
| Tracing | WIP |
| Metrics | WIP |
| SQL Audit | Roadmap |
| Data encrypt / decrypt | Roadmap |

## Getting started

Please reference this link [Getting Started](https://github.com/arana-db/arana/discussions/172)

```
arana start -c ${configFilePath}
```

### Prerequisites

+ Go 1.16+
+ MySQL Server 5.7+

## Design and implementation

## Roadmap

## Built With

- [TiDB](https://github.com/pingcap/tidb) - The SQL parser used

## Contact

<img src="https://raw.githubusercontent.com/arana-db/arana/master/docs/pics/dingtalk-group.png" width="200px"/>

## Contributing

## License
Arana software is licenced under the Apache License Version 2.0. See the [LICENSE](https://github.com/arana-db/arana/blob/master/LICENSE) file for details.
