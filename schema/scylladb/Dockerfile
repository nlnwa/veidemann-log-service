FROM scylladb/scylla:4.4.4

COPY schema/* /schema/

ENV TEMPLATE=/schema/v002.cql.tmpl
ENV CQLSH_HOST=scylla-client
ENTRYPOINT ["/schema/docker.sh"]
