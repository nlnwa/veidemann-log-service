package main

import (
	"context"
	"fmt"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/nlnwa/veidemann-log-service/internal/connection"
	"github.com/nlnwa/veidemann-log-service/internal/logger"
	"github.com/nlnwa/veidemann-log-service/internal/scylla"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
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

	pflag.String("log-level", "info", "Log level, available levels are: panic, fatal, error, warn, info, debug and trace")
	pflag.String("log-formatter", "logfmt", "Log formatter, available values are: logfmt and json")
	pflag.Bool("log-method", false, "Log file:line of method caller")
	pflag.Parse()

	_ = viper.BindPFlags(pflag.CommandLine)
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse flags")
	}

	logger.InitLog(viper.GetString("log-level"), viper.GetString("log-formatter"), viper.GetBool("log-method"))

	logServer := scylla.New(scylla.Options{
		Hosts:    viper.GetStringSlice("db-host"),
		Keyspace: viper.GetString("db-keyspace"),
	})
	if err := logServer.Connect(); err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to scylla cluster")
	}
	defer logServer.Close()
	log.Info().
		Str("hosts", strings.Join(viper.GetStringSlice("db-host"), ",")).
		Str("keyspace", viper.GetString("db-keyspace")).
		Msgf("Connected to scylla cluster")

	tracer := opentracing.GlobalTracer()

	server := connection.NewGrpcServer(
		viper.GetString("host"),
		viper.GetInt("port"),
		grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(otgrpc.OpenTracingStreamServerInterceptor(tracer)),
	)
	logV1.RegisterLogServer(server.Server, logServer)

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
		signals := make(chan os.Signal)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		select {
		case sig := <-signals:
			log.Info().Str("signal", sig.String()).Msg("Shutting down")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("Metrics server shutdown failure")
		}
		server.Shutdown()
	}()

	log.Info().
		Str("host", viper.GetString("host")).
		Int("port", viper.GetInt("port")).
		Msg("API server listening")
	err = server.Serve()
	if err != nil {
		log.Error().Err(err).Msg("API server failure")
	}

	// wait for shutdown to complete
	<-done
}
