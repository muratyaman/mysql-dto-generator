#!/bin/sh
rm -rf ./examples/demo/*.ts

PGHOST="localhost" \
PGPORT="5432" \
PGUSER="murat" \
PGPASSWORD="" \
PGDATABASE="demo" \
ts-node ./src/index.ts
