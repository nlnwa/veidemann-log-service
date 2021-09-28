FROM scylladb/scylla:4.4.1

COPY schema/* /schema/

ENV CQLSH_HOST=scylla-client
ENTRYPOINT ["/schema/docker.sh"]
