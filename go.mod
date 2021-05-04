module github.com/nlnwa/veidemann-log-service

go 1.15

require (
	github.com/gocql/gocql v0.0.0-20210129204804-4364a4b9cfdd
	github.com/google/uuid v1.2.0
	github.com/nlnwa/veidemann-api/go v0.0.0-20210413093311-7ff38e848604
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.10.0
	github.com/rs/zerolog v1.20.0
	github.com/scylladb/gocqlx/v2 v2.3.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/testcontainers/testcontainers-go v0.10.0
	github.com/uber/jaeger-client-go v2.27.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	google.golang.org/grpc v1.33.2
	google.golang.org/protobuf v1.26.0
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.5.0
