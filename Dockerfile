FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM golang:latest as builder

RUN mkdir /CORE-Server
WORKDIR /CORE-Server
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o CORE-Server .

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /CORE-Server/CORE-Server .

ENTRYPOINT [ "/CORE-Server", "-c", "/config/config.yaml", "run" ]