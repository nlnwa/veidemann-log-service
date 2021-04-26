FROM scylladb/scylla:4.4.1

COPY schema/* /schema/

ENV CQLSH_HOST=scylla
ENTRYPOINT ["/schema/docker.sh"]
