package logger

import (
	"github.com/pkg/errors"
)

const (
	defaultJSONOutput  = false
	defaultOutputLevel = "info"
)

// Option defines the sets of option for logging.
type Option func(l *logger) error

// WithOutputLevelString sets the log output level by string.
func WithOutputLevelString(outputLevel string) Option {
	return func(l *logger) error {
		if toLogLevel(outputLevel) == UndefinedLevel {
			return errors.Errorf("undefined Log Output Level: %s", outputLevel)
		}
		l.SetOutputLevel(toLogLevel(outputLevel))
		return nil
	}
}

// WithOutputLevel sets the log output level.
func WithOutputLevel(outputLevel LogLevel) Option {
	return func(l *logger) error {
		if outputLevel == UndefinedLevel {
			return errors.Errorf("undefined Log Output Level: %s", outputLevel)
		}
		l.SetOutputLevel(outputLevel)
		return nil
	}
}

// WithOutputLevel sets the log output format.
func WithOutputFormat(enableJson bool) Option {
	return func(l *logger) error {
		l.EnableJSONOutput(enableJson)
		return nil
	}
}
