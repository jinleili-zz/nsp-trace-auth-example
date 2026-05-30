#!/bin/sh
set -e

DSN="${POSTGRES_DSN:-postgres://nsp:nsp123@localhost:5432/nsp?sslmode=disable}"

echo "Initializing saga schema..."

# Wait for postgres to be ready
until pg_isready -d "$DSN" > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 1
done

# Apply saga migration
psql "$DSN" -f /migrations/saga.sql

echo "Saga schema initialized."
