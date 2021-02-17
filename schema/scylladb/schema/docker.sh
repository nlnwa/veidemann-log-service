#!/usr/bin/env bash
#
# This script is used in the Docker image norsknettarkiv/veidemann-log-schema
# that allows installing veidemann keyspace and schema without installing cqlsh.

CQLSH=${CQLSH:-"/usr/bin/cqlsh"}
CQLSH_HOST=${CQLSH_HOST:-"scylla"}
CQLSH_PORT=${CQLSH_PORT:-"9042"}
CQLSH_SSL=${CQLSH_SSL:-""}
CASSANDRA_WAIT_TIMEOUT=${CASSANDRA_WAIT_TIMEOUT:-"60"}
DATACENTER=${DATACENTER:-"dc1"}
KEYSPACE=${KEYSPACE:-"v7n_v1_${DATACENTER}"}
MODE=${MODE:-"test"}
TEMPLATE=${TEMPLATE:-""}
USER=${CASSANDRA_USERNAME:-""}
PASSWORD=${CASSANDRA_PASSWORD:-""}

total_wait=0
while true
do
  if [ -z "$PASSWORD" ]; then
    ${CQLSH} ${CQLSH_SSL} ${CQLSH_HOST} ${CQLSH_PORT} -e "describe keyspaces"
  else
    ${CQLSH} ${CQLSH_SSL} ${CQLSH_HOST} ${CQLSH_PORT} -u ${USER} -p ${PASSWORD} -e "describe keyspaces"
  fi
  if (( $? == 0 )); then
    break
  else
    if (( total_wait >= ${CASSANDRA_WAIT_TIMEOUT} )); then
      echo "Timed out waiting for scylla."
      exit 1
    fi
    echo "Scylla is still not up at ${CQLSH_HOST}:${CQLSH_PORT}. Waiting 1 second."
    sleep 1s
    ((total_wait++))
  fi
done

echo "Generating the schema for the keyspace ${KEYSPACE} and datacenter ${DATACENTER}"


if [ -z "$PASSWORD" ]; then
  MODE="${MODE}" DATACENTER="${DATACENTER}" KEYSPACE="${KEYSPACE}" /schema/create.sh "${TEMPLATE}" | ${CQLSH} ${CQLSH_SSL} ${CQLSH_HOST} ${CQLSH_PORT}
else
  MODE="${MODE}" DATACENTER="${DATACENTER}" KEYSPACE="${KEYSPACE}" /schema/create.sh "${TEMPLATE}" | ${CQLSH} ${CQLSH_SSL} ${CQLSH_HOST} ${CQLSH_PORT} -u ${USER} -p ${PASSWORD}
fi
