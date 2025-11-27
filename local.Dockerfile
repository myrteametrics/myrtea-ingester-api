FROM alpine:3.19.1
LABEL maintainer="Mind7 Consulting <contact@mind7.com>"

RUN apk update && apk add --no-cache ca-certificates && rm -rf /var/cache/apk/*
RUN addgroup -S myrtea -g "1001" &&  \
    adduser -S myrtea -G myrtea -u "1001"

WORKDIR /app

COPY bin/myrtea-ingester-api myrtea-ingester-api
COPY config config

USER myrtea

ENTRYPOINT ["./myrtea-ingester-api"]
