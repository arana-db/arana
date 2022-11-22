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

### BUILDER LAYER
FROM golang:1.18-alpine AS BE

WORKDIR /app

COPY ./go.mod ./
COPY ./go.sum ./

RUN go mod download

COPY . .

RUN mkdir ./bin && \
    go build -ldflags "-X main.Version=`cat VERSION` -extldflags \"-static\" -s -w" -o ./bin/arana ./cmd

### UI LAYER
FROM node:16-alpine as FE

RUN apk add --no-cache git

# specify git revision for arana-db/arana-ui repo
ARG UI_REVISION="3d46401"

WORKDIR /arana-ui

RUN git clone -n https://github.com/arana-db/arana-ui.git /arana-ui && \
    git checkout $UI_REVISION && \
    yarn && yarn build

### RUNTIME LAYER
FROM alpine:3

ENV ARANA_LOG_NAME=arana.log \
    ARANA_LOG_LEVEL=0 \
    ARANA_LOG_MAX_SIZE=10 \
    ARANA_LOG_MAX_BACKUPS=5 \
    ARANA_LOG_MAX_AGE=30 \
    ARANA_LOG_COMPRESS=false

WORKDIR /

RUN mkdir -p /etc/arana /var/www/arana

VOLUME /etc/arana
VOLUME /var/www/arana

EXPOSE 13306
EXPOSE 8080

COPY ./conf/* /etc/arana/
COPY --from=BE /app/bin/arana /usr/local/bin/
COPY --from=FE /arana-ui/dist/* /var/www/arana/

CMD ["arana", "start", "-c", "/etc/arana/bootstrap.yaml"]

# ui & nginx server
FROM bitnami/git

FROM node as web_builder

WORKDIR /var/www

RUN git clone https://github.com/arana-db/arana-ui.git /var/www && yarn && yarn build

FROM nginx

COPY /var/www/nginx/ /etc/nginx/conf.d/

COPY --from=web_builder /var/www/dist /usr/share/nginx/html
