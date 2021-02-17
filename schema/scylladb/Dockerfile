FROM scylladb/scylla:4.3.0

COPY schema/* /schema/

ENV CQLSH_HOST=scylla
ENTRYPOINT ["/schema/docker.sh"]
