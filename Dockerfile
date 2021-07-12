ARG crystal_version=1.0.0
FROM crystallang/crystal:${crystal_version}-alpine

WORKDIR /app

RUN apk add --no-cache bash
RUN apk add --no-cache -X http://dl-cdn.alpinelinux.org/alpine/edge/testing watchexec

COPY shard.yml /app

RUN shards install --ignore-crystal-version

COPY scripts/entrypoint.sh /app/entrypoint.sh
CMD /app/entrypoint.sh
