module github.com/nlnwa/veidemann-log-service

go 1.16

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/gocql/gocql v0.0.0-20210817081954-bc256bbb90de
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.2.0
	github.com/nlnwa/veidemann-api/go v0.0.0-20210414094839-b36ce92632fe
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.31.1
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rs/zerolog v1.25.0
	github.com/scylladb/gocqlx/v2 v2.4.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.9.0
	github.com/testcontainers/testcontainers-go v0.10.0
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/net v0.0.0-20210928044308-7d9f5e0b762b // indirect
	golang.org/x/sys v0.0.0-20210927094055-39ccf1dd6fa6 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20210927142257-433400c27d05 // indirect
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.5.0
