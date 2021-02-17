# Licensed to the jaeger authors under one or more contributor license agreements.
# This file originates from https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/cassandra/schema

#!/usr/bin/env bash

function usage {
    >&2 echo "Error: $1"
    >&2 echo ""
    >&2 echo "Usage: MODE=(prod|test) [PARAM=value ...] $0 [template-file] | cqlsh"
    >&2 echo ""
    >&2 echo "The following parameters can be set via environment:"
    >&2 echo "  MODE               - prod or test. Test keyspace is usable on a single node cluster (no replication)"
    >&2 echo "  DATACENTER         - datacenter name for network topology used in prod (optional in MODE=test)"
    >&2 echo "  KEYSPACE           - keyspace (default: v7n_v1_{datacenter})"
    >&2 echo "  REPLICATION_FACTOR - replication factor for prod (default: 2 for prod, 1 for test)"
    >&2 echo ""
    >&2 echo "The template-file argument must be fully qualified path to a v00#.cql.tmpl template file."
    >&2 echo "If omitted, the template file with the highest available version will be used."
    exit 1
}

template=$1
if [[ "$template" == "" ]]; then
    template=$(ls $(dirname $0)/*cql.tmpl | sort | tail -1)
fi

if [[ "$MODE" == "" ]]; then
    usage "missing MODE parameter"
elif [[ "$MODE" == "prod" ]]; then
    if [[ "$DATACENTER" == "" ]]; then usage "missing DATACENTER parameter for prod mode"; fi
    datacenter=$DATACENTER
    replication_factor=${REPLICATION_FACTOR:-2}
    replication="{'class': 'NetworkTopologyStrategy', '$datacenter': '${replication_factor}' }"
elif [[ "$MODE" == "test" ]]; then 
    datacenter=${DATACENTER:-'test'}
    replication_factor=${REPLICATION_FACTOR:-1}
    replication="{'class': 'SimpleStrategy', 'replication_factor': '${replication_factor}'}"
else
    usage "invalid MODE=$MODE, expecting 'prod' or 'test'"
fi

keyspace=${KEYSPACE:-"v7n_v1_${datacenter}"}

if [[ $keyspace =~ [^a-zA-Z0-9_] ]]; then
    usage "invalid characters in KEYSPACE=$keyspace parameter, please use letters, digits or underscores"
fi

>&2 cat <<EOF
Using template file $template with parameters:
    mode = $MODE
    datacenter = $datacenter
    keyspace = $keyspace
    replication = ${replication}
EOF

# strip out comments, collapse multiple adjacent empty lines (cat -s), substitute variables
cat $template | sed \
    -e 's/--.*$//g'                                 \
    -e 's/^\s*$//g'                                 \
    -e "s/\${keyspace}/${keyspace}/g"               \
    -e "s/\${replication}/${replication}/g"         | cat -s
