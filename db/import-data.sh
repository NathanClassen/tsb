#!/bin/bash

export PGPASSWORD=password

psql -U postgres -h localhost < db/cpu_usage.sql

psql -U postgres -d homework -h localhost -c "\COPY cpu_usage FROM db/cpu_usage.csv CSV HEADER"