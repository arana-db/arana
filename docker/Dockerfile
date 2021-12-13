FROM alpine:latest

WORKDIR app

COPY dist/ .

CMD ["./arana", "start", "-c", "config.yaml"]