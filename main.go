package main

import (
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/nlnwa/veidemann-log-service/internal/connection"
	"github.com/nlnwa/veidemann-log-service/internal/logger"
	"github.com/nlnwa/veidemann-log-service/internal/scylla"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	pflag.String("host", "", "Interface the log service API is listening to. No value means all interfaces.")
	pflag.Int("port", 8090, "Port the log service api is listening to")

	pflag.StringSlice("db-host", []string{}, "List of db hosts")
	pflag.StringSlice("db-keyspace", []string{}, "Name of keyspace")

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
	err = logServer.Connect()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to scylla cluster")
	}
	defer logServer.Close()
	log.Info().
		Str("hosts", strings.Join(viper.GetStringSlice("db-host"), ",")).
		Str("keyspace", viper.GetString("db-keyspace")).
		Msgf("Connected to scylla cluster")

	server := connection.NewGrpcServer(
		viper.GetString("host"),
		viper.GetInt("port"),
	)
	logV1.RegisterLogServer(server.Server, logServer)

	go func() {
		signals := make(chan os.Signal)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		select {
		case sig := <-signals:
			log.Debug().Msgf("Received signal: %v", sig)
		}
		server.Shutdown()
	}()
	err = server.Serve()
	if err != nil {
		log.Err(err).Msg("")
	}
}
