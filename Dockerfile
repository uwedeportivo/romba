FROM golang:1.12.9-alpine3.10 as builder

RUN apk add --no-cache leveldb-dev zlib-dev git mercurial build-base

RUN mkdir /app
ADD . /app
WORKDIR /app/cmds/rombaserver

RUN CGO_ENABLED=1 GOOS=linux go build .

FROM alpine:latest AS production

RUN apk add --no-cache zlib leveldb ca-certificates mailcap tini

COPY --from=builder /app .

RUN echo "hosts: files dns" > /etc/nsswitch.conf

ENV GO111MODULES=on LANG=en_US.utf8
ENTRYPOINT ["/sbin/tini", "--", "/cmds/rombaserver/rombaserver", "-ini", "/cmds/rombaserver/romba-docker.ini"]

