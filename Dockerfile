#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# builder layer
FROM golang:1.18-alpine AS builder

RUN apk add --no-cache upx

WORKDIR /app

COPY ./go.mod ./
COPY ./go.sum ./

RUN go mod download

COPY . .

# use upx, the binary size is as smaller as better when running as a side-car.
RUN mkdir ./bin && \
    go build -ldflags "-X main.Version=`cat VERSION` -extldflags \"-static\" -s -w" -o ./bin/arana ./cmd && \
    upx -9 -o ./bin/arana-upx ./bin/arana && \
    mv ./bin/arana-upx ./bin/arana

# runtime layer
FROM alpine:3

WORKDIR /

RUN mkdir -p /etc/arana

VOLUME /etc/arana

EXPOSE 13306

COPY --from=builder /app/bin/arana /usr/local/bin/
COPY ./conf/* /etc/arana/

CMD ["arana", "start", "-c", "/etc/arana/bootstrap.yaml"]
