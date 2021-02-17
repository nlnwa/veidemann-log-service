FROM scylladb/scylla:4.3.0

COPY schema/* /scylla-schema/

ENV CQLSH_HOST=scylla
ENTRYPOINT ["/scylla-schema/docker.sh"]
