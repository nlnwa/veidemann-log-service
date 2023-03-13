FROM scylladb/scylla:4.6.11

COPY schema/* /schema/

ENV TEMPLATE=/schema/v003.cql.tmpl
ENV CQLSH_HOST=scylla-client
ENV SERIAL_NUMBER=0
ENTRYPOINT ["/schema/docker.sh"]
