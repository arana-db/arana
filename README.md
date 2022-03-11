# arana
[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/dubbogo/arana/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/dubbogo/arana/branch/master/graph/badge.svg)](https://codecov.io/gh/dubbogo/arana)


![](./docs/pics/arana-logo.png)

## Introduction | [中文](https://github.com/dubbogo/arana/blob/master/README_CN.md)

Arana is a db proxy. It can be deployed as a sidecar.

## Architecture

## Features

| feature | complete |
| -- | -- |
| single db proxy | √ |
| read write splitting | × |
| tracing | × |
| metrics | × |
| sql audit | × |
| sharding | × |
| multi tenant | × |

## Getting started

```
arana start -c ${configFilePath}
```

### Prerequisites

+ MySQL server 5.7+

## Design and implementation

## Roadmap

## Built With
- [tidb](https://github.com/pingcap/tidb) - The sql parser used

## Contact

## Contributing

## License
Arana software is licenced under the Apache License Version 2.0. See the [LICENSE](https://github.com/dubbogo/arana/blob/master/LICENSE) file for details.
