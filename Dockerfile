# builder layer
FROM golang:1.16-alpine AS builder

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
