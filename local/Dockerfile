FROM golang:latest as builder

RUN mkdir /CORE-Server
WORKDIR /CORE-Server
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o CORE-Server .

FROM ubuntu
RUN apt-get -y update && apt-get -y upgrade
RUN curl https://binaries.cockroachdb.com/cockroach-v21.1.7.linux-amd64.tgz | tar -xz && sudo cp -i cockroach-v21.1.7.linux-amd64/cockroach /usr/local/bin/
COPY --from=builder /CORE-Server/CORE-Server .

RUN cockroach start-single-node --insecure --background