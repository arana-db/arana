FROM alpine:latest

WORKDIR app

COPY dist/ .

CMD ["./kylin", "start", "-c", "config.yaml"]