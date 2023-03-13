# veidemann-log-service
Service for reading and writing, crawl logs and page logs.

### Running integration test

The integration test spins up scylla and schema containers and is tagged
with "integration".

Using docker:

```shell
go clean -testcache && go test -tags=integration ./...
```

Using podman:

```shell
go clean -testcache && TESTCONTAINERS_RYUK_DISABLED=true DOCKER_HOST=unix:///var/run/user/${UID}/podman/podman.sock go test -tags=integration ./...
```

### Resources
- [Consistency level documentation][1]
- [Consistency level calculator][2]
- [Golang cql driver][3]
- [Scylla golang driver wrapper][4]

[1]: https://docs.scylladb.com/stable/cql/consistency.html
[2]: https://docs.scylladb.com/stable/cql/consistency-calculator.html
[3]: https://github.com/gocql/gocql
[4]: https://github.com/scylladb/gocqlx
