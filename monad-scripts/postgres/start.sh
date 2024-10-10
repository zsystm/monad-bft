#!/bin/bash

set -ex

docker run \
    --name monad-indexer \
    -p 5455:5432 \
    -e POSTGRES_USER=monaduser \
    -e POSTGRES_PASSWORD=monadpassword \
    -e POSTGRES_DB=localnet \
    -d \
    postgres
