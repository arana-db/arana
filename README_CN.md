# Arana

<div align=center>
    <img src="https://raw.githubusercontent.com/arana-db/arana/master/docs/pics/arana-main.png"/>
</div>

`Arana` 定位于云原生数据库代理，它可以以 `sidecar` 模式部署为数据库服务网格。`Arana` 提供透明的数据访问能力，当用户在使用时，可以不用关心数据库的“分片”细节，像使用单机 `MySQL` 数据库一样使用 `Arana`。

## 概览

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/arana-db/arana/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/arana-db/arana/branch/master/graph/badge.svg)](https://codecov.io/gh/arana-db/arana)
[![Go Report Card](https://goreportcard.com/badge/github.com/arana-db/arana)](https://goreportcard.com/report/github.com/arana-db/arana)
[![Release](https://img.shields.io/github/v/release/arana-db/arana)](https://img.shields.io/github/v/release/arana-db/arana)
[![Docker Pulls](https://img.shields.io/docker/pulls/aranadb/arana)](https://img.shields.io/docker/pulls/aranadb/arana)

|                                             **Star 用户时间线**                                              |                                                                                                                   **贡献用户时间线**                                                                                                                    |
|:-------------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| [![Stargazers over time](https://starchart.cc/arana-db/arana.svg)](https://starchart.cc/arana-db/arana) | [![Contributor over time](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=arana-db/arana)](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=arana-db/arana) |

## 介绍 | [英文](https://github.com/arana-db/arana/blob/master/README.md)

首先，`Arana` 定位于云原生数据库代理，它提供透明的数据访问能力。在使用 `Arana` 时，用户不需要关心数据库的分片细节，可以像使用单机 `MySQL` 数据库一样使用 `Arana`。`Arana` 还提供**多租户**、**分布式事务**、**影子库**、**SQL审计**、**数据加密/解密**等能力，通过简单的配置，用户就可以直接使用 `Arana` 所提供的这些能力。

其次，`Arana` 可以以 `sidecar` 模式部署为数据库服务网格，作为一款数据库服务网格的 `sidecar`，`Arana` 可以将数据访问模式从客户端模式切换为代理模式，这种转变能够极大优化应用程序的启动速度，不仅占用容器资源极少，而且能够在不影响容器内应用服务的性能情况下提供了代理的所有能力。

## 架构

<img src="https://raw.githubusercontent.com/arana-db/arana/master/docs/pics/arana-architecture.png"/>

## 特性

|      **特性**       | **是否支持** |
|:-----------------:|:--------:|
|       单实例代理       |    √     |
|       读写分离        |    √     |
|        分片         |    √     |
|    多租户            |    √     |
|       分布式主键       |    √     |
|        影子表        |    √     |
| Tracing / Metrics |   √    |
|       分布式事务       |   WIP    |
|       数据库网格       |   WIP    |
|      SQL 审计       |   WIP    |
|      数据加密/解密      | Roadmap  |
|      SQL 限流       | Roadmap  |

## 快速启动

[快速启动](https://github.com/arana-db/arana/discussions/172)

```
arana start -c ${configFilePath}
```

### 依赖

+ Go 1.20+
+ MySQL Server 5.7+

## 设计和实现

## 路线图

## 构建依赖

- [TiDB](https://github.com/pingcap/tidb) - SQL 解析场景使用

## 联系我们

`Arana` 社区周会时间：**每双周周六 21:00**。

<img src="https://raw.githubusercontent.com/arana-db/arana/master/docs/pics/dingtalk-group.jpeg" width="300px"/>

## 贡献指南

感谢大家对 `Arana` 做出的贡献，非常欢迎您的参与！这里有一份[贡献指南](./CONTRIBUTING.md)来帮助大家快速参与到项目中。

## 贡献者列表

感谢所有 [贡献者](https://github.com/arana-db/arana/graphs/contributors)!

<a href="https://github.com/arana-db/arana/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=arana-db/arana" />
</a>

## 许可协议

`Arana` 使用 `Apache License Version 2.0` 许可协议，[协议详情](https://github.com/arana-db/arana/blob/master/LICENSE)。
