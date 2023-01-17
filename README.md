# Arana

<div align=center>
    <img src="https://raw.githubusercontent.com/arana-db/arana/master/docs/pics/arana-main.png"/>
</div>

`Arana` is a Cloud Native Database Proxy. It can be deployed as a Database mesh sidecar. It provides transparent data
access capabilities,
when using `arana`, user doesn't need to care about the `sharding` details of database, they can use it just like a
single `MySQL` database.

## Overview

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/arana-db/arana/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/arana-db/arana/branch/master/graph/badge.svg)](https://codecov.io/gh/arana-db/arana)
[![Go Report Card](https://goreportcard.com/badge/github.com/arana-db/arana)](https://goreportcard.com/report/github.com/arana-db/arana)
[![Release](https://img.shields.io/github/v/release/arana-db/arana)](https://img.shields.io/github/v/release/arana-db/arana)
[![Docker Pulls](https://img.shields.io/docker/pulls/aranadb/arana)](https://img.shields.io/docker/pulls/aranadb/arana)

|                                             **Stargazers Over Time**                                              |                                                                                                            **Contributors Over Time**                                                                                                            |
|:-----------------------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|      [![Stargazers over time](https://starchart.cc/arana-db/arana.svg)](https://starchart.cc/arana-db/arana)      | [![Contributor over time](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=arana-db/arana)](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=arana-db/arana) |

## Introduction | [中文](https://github.com/arana-db/arana/blob/master/README_CN.md)

First, `Arana` is a Cloud Native Database Proxy. It provides transparent data access capabilities, when using `arana`,
user doesn't need to care about the `sharding` details of database, they can use it just like a single `MySQL` database.
`Arana` also provide abilities of `Multi Tenant`, `Distributed transaction`, `Shadow database`, `SQL Audit`
, `Data encrypt / decrypt`
and so on. Through simple config, user can use these abilities provided by `arana` directly.

Second, `Arana` can also be deployed as a Database mesh sidecar. As a Database mesh sidecar, arana switches data access
from
client mode to proxy mode, which greatly optimizes the startup speed of applications. It provides the ability to manage
database
traffic, it takes up very little container resources, doesn't affect the performance of application services in the
container， but
provides all the capabilities of proxy.

## Architecture

<img src="https://raw.githubusercontent.com/arana-db/arana/master/docs/pics/arana-architecture.png"/>

## Features

|       **Feature**       | **Complete** |
|:-----------------------:|:------------:|
|     Single DB Proxy     |      √       |
|  Read Write Splitting   |      √       |
|        Sharding         |      √       |
|      Multi Tenant       |      √       |
| Distributed Primary Key |      √       |
|      Shadow Table       |      √       |
| Distributed Transaction |     WIP      |
|      Database Mesh      |     WIP      |
|    Tracing / Metrics    |     WIP      |
|        SQL Audit        |     WIP      |
| Data encrypt / decrypt  |   Roadmap    |
|       SQL LIMITER       |   Roadmap    |

## Getting started

Please reference this link [Getting Started](https://github.com/arana-db/arana/discussions/172)

```
arana start -c ${configFilePath}
```

### Prerequisites

+ Go 1.18+
+ MySQL Server 5.7+

## Design and implementation

## Roadmap

## Built With

- [TiDB](https://github.com/pingcap/tidb) - The SQL parser used

## Contact

Arana Chinese Community Meeting Time: **Every Saturday At 9:00PM GMT+8**

<img src="https://raw.githubusercontent.com/arana-db/arana/master/docs/pics/dingtalk-group.jpeg" width="300px"/>

## Contributing

Thanks for your help improving the project! We are so happy to have you! We have a contributing guide to help you get
involved in the Arana project.

## Developer

Thanks to [all developers](https://github.com/arana-db/arana/graphs/contributors)!

<a href="https://github.com/arana-db/arana/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=arana-db/arana" />
</a>

## License

Arana software is licenced under the Apache License Version 2.0. See
the [LICENSE](https://github.com/arana-db/arana/blob/master/LICENSE) file for details.
