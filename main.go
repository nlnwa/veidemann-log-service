/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/nlnwa/veidemann-log-service/internal/logger"
	"github.com/nlnwa/veidemann-log-service/internal/logservice"
	"github.com/nlnwa/veidemann-log-service/internal/scylla"
	"github.com/nlnwa/veidemann-log-service/internal/tracing"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	pflag.String("host", "", "Interface the log service API is listening to. No value means all interfaces.")
	pflag.Int("port", 8090, "Port the log service api is listening to")

	pflag.Int("metrics-port", 9153, "Prometheus metrics port")

	pflag.StringSlice("db-host", []string{"localhost"}, "List of db hosts")
	pflag.String("db-keyspace", "", "Name of keyspace")
	pflag.String("db-consistency", "local_quorum", "Write consistency")
	pflag.String("db-read-consistency", "one", "Read consistency")

	pflag.Int("read-query-pool-size", 3, "Number of prepared statements in read pools")
	pflag.Int("write-query-pool-size", 10, "Number of prepared statements in write pools")

	pflag.String("log-level", "info", "Log level, available levels are: panic, fatal, error, warn, info, debug and trace")
	pflag.String("log-formatter", "logfmt", "Log formatter, available values are: logfmt and json")
	pflag.Bool("log-method", false, "Log file:line of method caller")
	pflag.Parse()

	_ = viper.BindPFlags(pflag.CommandLine)
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		panic(err)
	}

	logger.InitLog(viper.GetString("log-level"), viper.GetString("log-formatter"), viper.GetBool("log-method"))

	// setup tracing
	if tracer, closer, err := tracing.Init("Log Service"); err != nil {
		log.Warn().Err(err).Msg("Failed to initialize tracing")
	} else {
		defer closer.Close()
		opentracing.SetGlobalTracer(tracer)
		log.Info().Msg("Tracing initialized")
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(opentracing.GlobalTracer())),
		grpc.StreamInterceptor(otgrpc.OpenTracingStreamServerInterceptor(opentracing.GlobalTracer())),
	)

	scyllaCfg := scylla.CreateCluster(
		gocql.ParseConsistency(viper.GetString("db-consistency")),
		viper.GetString("db-keyspace"),
		viper.GetStringSlice("db-host")...)
	if session, err := scylla.Connect(scyllaCfg); err != nil {
		panic(err)
	} else {
		defer session.Close()

		logServer := logservice.New(
			session,
			viper.GetInt("read-query-pool-size"),
			viper.GetInt("write-query-pool-size"),
			gocql.ParseConsistency(viper.GetString("db-read-consistency")),
		)
		logV1.RegisterLogServer(server, logServer)
		defer logServer.Close()
	}
	log.Info().
		Str("hosts", strings.Join(viper.GetStringSlice("db-host"), ",")).
		Str("keyspace", viper.GetString("db-keyspace")).
		Msgf("Connected to scylla cluster")

	metricsServer := &http.Server{
		Addr: fmt.Sprintf("%s:%d", viper.GetString("host"), viper.GetInt("metrics-port")),
	}
	go func() {
		log.Info().
			Str("host", viper.GetString("host")).
			Int("port", viper.GetInt("metrics-port")).
			Str("path", "/metrics").
			Msg("Metrics server listening")
		http.Handle("/metrics", promhttp.Handler())
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Metrics server failed")
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		signals := make(chan os.Signal, 2)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		select {
		case sig := <-signals:
			log.Info().Str("signal", sig.String()).Msg("Shutting down")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(ctx)
		server.GracefulStop()
	}()

	addr := fmt.Sprintf("%s:%d", viper.GetString("host"), viper.GetInt("port"))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	log.Info().Str("address", addr).Msg("API server listening")
	if err = server.Serve(listener); err != nil {
		panic(err)
	}

	// wait for shutdown to complete
	<-done
}
