ARG POSTGRES_VERSION=18
FROM postgres:${POSTGRES_VERSION}

COPY init.sql /docker-entrypoint-initdb.d/
COPY run.sh /run.sh

RUN chmod +x /run.sh

ENTRYPOINT ["/run.sh"]
