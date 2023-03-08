package schema

import (
	"context"
	"fmt"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ClientLogger This Component is used to print logs on the client to facilitate troubleshooting when problems occur
type ClientLogger interface {
	Debug(msg string, fields ...zap.Field)
	DebugF(msg string, args ...any)

	Info(msg string, fields ...zap.Field)
	InfoF(msg string, args ...any)

	Warn(msg string, fields ...zap.Field)
	WarnF(msg string, args ...any)

	Error(msg string, fields ...zap.Field)
	ErrorF(msg string, args ...any)

	Fatal(msg string, fields ...zap.Field)
	FatalF(msg string, args ...any)

	LogDiagnostics(prefix string, d *Diagnostics)
}

type DesensitizationFunction func(ctx context.Context, msg string, fields ...zap.Field) (string, []zap.Field)

// ------------------------------------------------- DefaultClientLogger -----------------------------------------------

type DefaultClientLogger struct {
	logger                  *zap.Logger
	loggerConfig            *DefaultClientLoggerConfig
	DesensitizationFunction DesensitizationFunction
}

var _ ClientLogger = &DefaultClientLogger{}

func (x *DefaultClientLogger) Log(level zapcore.Level, msg string, fields ...zap.Field) {
	if x.DesensitizationFunction != nil {
		msg, fields = x.DesensitizationFunction(context.Background(), msg, fields...)
	}
	x.logger.Log(level, msg, fields...)
}

func (x *DefaultClientLogger) Debug(msg string, fields ...zap.Field) {
	x.Log(zapcore.DebugLevel, msg, fields...)
}

func (x *DefaultClientLogger) DebugF(msg string, args ...any) {
	x.Log(zapcore.DebugLevel, fmt.Sprintf(msg, args...))
}

func (x *DefaultClientLogger) Info(msg string, fields ...zap.Field) {
	x.Log(zapcore.InfoLevel, msg, fields...)
}

func (x *DefaultClientLogger) InfoF(msg string, args ...any) {
	x.Log(zapcore.InfoLevel, fmt.Sprintf(msg, args...))
}

func (x *DefaultClientLogger) Warn(msg string, fields ...zap.Field) {
	x.Log(zapcore.WarnLevel, msg, fields...)
}

func (x *DefaultClientLogger) WarnF(msg string, args ...any) {
	x.Log(zapcore.WarnLevel, fmt.Sprintf(msg, args...))
}

func (x *DefaultClientLogger) Error(msg string, fields ...zap.Field) {
	x.Log(zapcore.ErrorLevel, msg, fields...)
}

func (x *DefaultClientLogger) ErrorF(msg string, args ...any) {
	x.Log(zapcore.ErrorLevel, fmt.Sprintf(msg, args...))
}

func (x *DefaultClientLogger) Fatal(msg string, fields ...zap.Field) {
	x.Log(zapcore.FatalLevel, msg, fields...)
}

func (x *DefaultClientLogger) FatalF(msg string, args ...any) {
	x.Log(zapcore.FatalLevel, fmt.Sprintf(msg, args...))
}

// LogDiagnostics Logs need to be able to print diagnostic logs directly
func (x *DefaultClientLogger) LogDiagnostics(prefix string, d *Diagnostics) {

	if d == nil {
		return
	}

	for _, diagnostic := range d.GetDiagnosticSlice() {

		var msg string
		if prefix != "" {
			msg = fmt.Sprintf("%s, %s", prefix, diagnostic.Content())
		} else {
			msg = diagnostic.Content()
		}

		switch diagnostic.level {
		case DiagnosisLevelTrace:
			x.Debug(msg)
		case DiagnosisLevelDebug:
			x.Debug(msg)
		case DiagnosisLevelInfo:
			x.Info(msg)
		case DiagnosisLevelWarn:
			x.Warn(msg)
		case DiagnosisLevelError:
			x.Error(msg)
		case DiagnosisLevelFatal:
			x.Fatal(msg)
		}
	}

}

func (x *DefaultClientLogger) Name() string {
	return "DefaultClientLogger"
}

func NewDefaultClientLogger(loggerConfig *DefaultClientLoggerConfig) (*DefaultClientLogger, error) {

	// create log directory if not exists
	_, err := os.Stat(loggerConfig.logDirectory)
	if os.IsNotExist(err) {
		err = os.MkdirAll(loggerConfig.logDirectory, 0755)
	}
	if err != nil {
		return nil, err
	}

	logger := zap.New(zapcore.NewTee(loggerConfig.GetEncoderCore()...))

	if loggerConfig.ShowLine {
		logger = logger.WithOptions(zap.AddCaller())
	}

	return &DefaultClientLogger{logger: logger, loggerConfig: loggerConfig}, nil
}

// Each Provider's logs are stored in a separate folder
func getProviderLogDirectory(workspace, providerName string) string {
	return filepath.Join(workspace, "logs", providerName)
}

// ------------------------------------------------- DefaultClientLoggerConfig -----------------------------------------

type DefaultClientLoggerConfig struct {
	DesensitizationFunction func(ctx context.Context, msg string, args ...any) (string, []any)

	FileLogEnabled      bool
	ConsoleLogEnabled   bool
	EncodeLogsAsJson    bool
	Level               string
	LevelIdentUppercase bool
	MaxDayAge           int
	ShowLine            bool
	ConsoleNoColor      bool
	MaxMegaBytesSize    int
	MaxBackups          int
	TimeFormat          string
	Prefix              string

	logDirectory string
}

func (x *DefaultClientLoggerConfig) EncodeLevel() zapcore.LevelEncoder {
	switch {
	case x.LevelIdentUppercase && x.ConsoleNoColor:
		return zapcore.CapitalLevelEncoder
	case x.LevelIdentUppercase && !x.ConsoleNoColor:
		return zapcore.CapitalColorLevelEncoder
	case !x.LevelIdentUppercase && x.ConsoleNoColor:
		return zapcore.LowercaseLevelEncoder
	case !x.LevelIdentUppercase && !x.ConsoleNoColor:
		return zapcore.LowercaseColorLevelEncoder
	default:
		return zapcore.LowercaseLevelEncoder
	}
}

func (x *DefaultClientLoggerConfig) TranslationLevel() zapcore.Level {
	switch strings.ToLower(x.Level) {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "dpanic":
		return zapcore.DPanicLevel
	case "panic":
		return zapcore.PanicLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

func (x *DefaultClientLoggerConfig) GetEncoder() zapcore.Encoder {
	if x.EncodeLogsAsJson {
		return zapcore.NewJSONEncoder(x.GetEncoderConfig())
	}
	return zapcore.NewConsoleEncoder(x.GetEncoderConfig())
}

func (x *DefaultClientLoggerConfig) GetEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:    "message",
		LevelKey:      "level",
		TimeKey:       "time",
		NameKey:       "logger",
		CallerKey:     "caller",
		FunctionKey:   "func",
		StacktraceKey: "stack",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   x.EncodeLevel(),
		EncodeTime: func(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString(x.Prefix + t.Format(x.TimeFormat))
		},
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}
}

func (x *DefaultClientLoggerConfig) GetLogWriter(level string) zapcore.WriteSyncer {
	lumberjackLogger := &lumberjack.Logger{
		Filename:   filepath.Join(x.logDirectory, level+".log"),
		MaxSize:    x.MaxMegaBytesSize,
		MaxAge:     x.MaxDayAge,
		MaxBackups: x.MaxBackups,
		LocalTime:  true,
		Compress:   false,
	}
	return zapcore.AddSync(lumberjackLogger)
}

func (x *DefaultClientLoggerConfig) GetEncoderCore() []zapcore.Core {
	cores := make([]zapcore.Core, 0, 7)
	for level := x.TranslationLevel(); level <= zapcore.FatalLevel; level++ {
		cores = append(cores, zapcore.NewCore(x.GetEncoder(), x.GetLogWriter(level.String()), x.GetLevelPriority(level)))
	}
	return cores
}

func (x *DefaultClientLoggerConfig) GetLevelPriority(level zapcore.Level) zap.LevelEnablerFunc {
	switch level {
	case zapcore.DebugLevel:
		return func(level zapcore.Level) bool {
			return level == zap.DebugLevel
		}
	case zapcore.InfoLevel:
		return func(level zapcore.Level) bool {
			return level == zap.InfoLevel
		}
	case zapcore.WarnLevel:
		return func(level zapcore.Level) bool {
			return level == zap.WarnLevel
		}
	case zapcore.ErrorLevel:
		return func(level zapcore.Level) bool {
			return level == zap.ErrorLevel
		}
	case zapcore.DPanicLevel:
		return func(level zapcore.Level) bool {
			return level == zap.DPanicLevel
		}
	case zapcore.PanicLevel:
		return func(level zapcore.Level) bool {
			return level == zap.PanicLevel
		}
	case zapcore.FatalLevel:
		return func(level zapcore.Level) bool {
			return level == zap.FatalLevel
		}
	default:
		return func(level zapcore.Level) bool {
			return level == zap.DebugLevel
		}
	}
}

func NewDefaultClientLoggerConfig(workspace, providerName string) *DefaultClientLoggerConfig {
	logDirectory := getProviderLogDirectory(workspace, providerName)
	return &DefaultClientLoggerConfig{
		FileLogEnabled:      true,
		ConsoleLogEnabled:   true,
		EncodeLogsAsJson:    false,
		Level:               "debug",
		LevelIdentUppercase: false,
		MaxDayAge:           365,
		ShowLine:            true,
		ConsoleNoColor:      true,
		MaxMegaBytesSize:    256,
		MaxBackups:          3,
		TimeFormat:          "[2006-01-02 15:04:05]",
		Prefix:              "",
		logDirectory:        logDirectory,
	}
}

// ---------------------------------------------------------------------------------------------------------------------
