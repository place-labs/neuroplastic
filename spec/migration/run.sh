#!/usr/bin/env bash

set -eu

psql "$PG_DATABASE_URL" -f /docker-entrypoint-initdb.d/init.sql
