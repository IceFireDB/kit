package logger

var global Logger

func Init(name string, options ...Option) {
	global = NewLogger(name, options...)
}

// Info logs a message at level Info.
func Info(args ...interface{}) {
	global.Info(args...)
}

// Infof logs a message at level Info.
func Infof(format string, args ...interface{}) {
	global.Infof(format, args...)
}

// Debug logs a message at level Debug.
func Debug(args ...interface{}) {
	global.Debug(args...)
}

// Debugf logs a message at level Debug.
func Debugf(format string, args ...interface{}) {
	global.Debugf(format, args...)
}

// Warn logs a message at level Warn.
func Warn(args ...interface{}) {
	global.Warn(args...)
}

// Warnf logs a message at level Warn.
func Warnf(format string, args ...interface{}) {
	global.Warnf(format, args...)
}

// Error logs a message at level Error.
func Error(args ...interface{}) {
	global.Error(args...)
}

// Errorf logs a message at level Error.
func Errorf(format string, args ...interface{}) {
	global.Errorf(format, args...)
}

// Fatal logs a message at level Fatal then the process will exit with status set to 1.
func Fatal(args ...interface{}) {
	global.Fatal(args...)
}

// Fatalf logs a message at level Fatal then the process will exit with status set to 1.
func Fatalf(format string, args ...interface{}) {
	global.Fatalf(format, args...)
}
