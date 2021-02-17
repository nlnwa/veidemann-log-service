package logger

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	stdlog "log"
	"os"
	"strings"
	"time"
)

func InitLog(level string, format string, logCaller bool) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	switch strings.ToLower(level) {
	case "panic":
		log.Logger = log.Level(zerolog.PanicLevel)
	case "fatal":
		log.Logger = log.Level(zerolog.FatalLevel)
	case "error":
		log.Logger = log.Level(zerolog.ErrorLevel)
	case "warn":
		log.Logger = log.Level(zerolog.WarnLevel)
	case "info":
		log.Logger = log.Level(zerolog.InfoLevel)
	case "debug":
		log.Logger = log.Level(zerolog.DebugLevel)
	case "trace":
		log.Logger = log.Level(zerolog.TraceLevel)
	}

	if format == "logfmt" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	}

	if logCaller {
		log.Logger = log.With().Caller().Logger()
	}

	stdlog.SetFlags(0)
	stdlog.SetOutput(log.Logger)
}
