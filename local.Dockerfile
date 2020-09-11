FROM alpine:3.7
LABEL maintainer="Myrtea Metrics <contact@myrteametrics.com>"

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
RUN addgroup -S myrtea -g "1001" &&  \
    adduser -S myrtea -G myrtea -u "1001"

WORKDIR /app

COPY bin/myrtea-ingester-api myrtea-ingester-api
COPY config config

USER myrtea

ENTRYPOINT ["./myrtea-ingester-api"]
