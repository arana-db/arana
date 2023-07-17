/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package log

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

import (
	"github.com/natefinch/lumberjack"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	// LogLevel represents the level of logging.
	LogLevel int8
	// LogType represents the type of logging.
	LogType string
	// LoggerKey represents the context key of logging.
	LoggerKey string
)

const (
	// DebugLevel logs are typically voluminous, and are usually disabled in
	// production.
	DebugLevel = LogLevel(zapcore.DebugLevel)
	// InfoLevel is the default logging priority.
	InfoLevel = LogLevel(zapcore.InfoLevel)
	// WarnLevel logs are more important than Info, but don't need individual
	// human review.
	WarnLevel = LogLevel(zapcore.WarnLevel)
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ErrorLevel = LogLevel(zapcore.ErrorLevel)
	// PanicLevel logs a message, then panics.
	PanicLevel = LogLevel(zapcore.PanicLevel)
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel = LogLevel(zapcore.FatalLevel)

	_minLevel = DebugLevel
	_maxLevel = FatalLevel

	MainLog        = LogType("main")
	LogicalSqlLog  = LogType("logical sql")
	PhysicalSqlLog = LogType("physical sql")
	TxLog          = LogType("tx")

	defaultLoggerLevel = InfoLevel
)

type LoggingConfig struct {
	LogName            string `yaml:"log_name" json:"log_name"`
	LogPath            string `yaml:"log_path" json:"log_path"`
	LogLevel           int    `yaml:"log_level" json:"log_level"`
	LogMaxSize         int    `yaml:"log_max_size" json:"log_max_size"`
	LogMaxBackups      int    `yaml:"log_max_backups" json:"log_max_backups"`
	LogMaxAge          int    `yaml:"log_max_age" json:"log_max_age"`
	LogCompress        bool   `yaml:"log_compress" json:"log_compress"`
	DefaultLogName     string `yaml:"default_log_name" json:"default_log_name"`
	TxLogName          string `yaml:"tx_log_name" json:"tx_log_name"`
	SqlLogEnabled      bool   `yaml:"sql_log_enabled" json:"sql_log_enabled"`
	SqlLogName         string `yaml:"sql_log_name" json:"sql_log_name"`
	PhysicalSqlLogName string `yaml:"physical_sql_log_name" json:"physical_sql_log_name"`
}

func (l *LogLevel) UnmarshalText(text []byte) error {
	if l == nil {
		return errors.New("can't unmarshal a nil *Level")
	}
	if !l.unmarshalText(text) && !l.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized level: %q", text)
	}
	return nil
}

func (l *LogLevel) unmarshalText(text []byte) bool {
	switch string(text) {
	case "debug", "DEBUG":
		*l = DebugLevel
	case "info", "INFO", "": // make the zero value useful
		*l = InfoLevel
	case "warn", "WARN":
		*l = WarnLevel
	case "error", "ERROR":
		*l = ErrorLevel
	case "panic", "PANIC":
		*l = PanicLevel
	case "fatal", "FATAL":
		*l = FatalLevel
	default:
		return false
	}
	return true
}

//Logger TODO add methods support LogType
type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Warn(v ...interface{})
	Warnf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

var (
	globalLogger *compositeLogger

	// TODO remove in the future
	zapLoggerEncoderConfig = zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	defaultLoggingConfig = &LoggingConfig{
		LogName:            "arana.log",
		LogPath:            "log",
		LogLevel:           -1,
		LogMaxSize:         10,
		LogMaxBackups:      5,
		LogMaxAge:          30,
		LogCompress:        false,
		DefaultLogName:     "arana.log",
		TxLogName:          "tx.log",
		SqlLogEnabled:      true,
		SqlLogName:         "sql.log",
		PhysicalSqlLogName: "physql.log",
	}
	loggerCfg = defaultLoggingConfig
)

func init() {
	globalLogger = NewCompositeLogger(defaultLoggingConfig)
}

func Init(cfg *LoggingConfig) {
	loggerCfg = cfg
	globalLogger = NewCompositeLogger(cfg)
}

type compositeLogger struct {
	loggerCfg *LoggingConfig

	mainLog        *zap.SugaredLogger
	logicalSqlLog  *zap.SugaredLogger
	physicalSqlLog *zap.SugaredLogger
	txLog          *zap.SugaredLogger

	// TODO replace global slowLog
	slowLog *zap.SugaredLogger
}

func NewCompositeLogger(cfg *LoggingConfig) *compositeLogger {
	return &compositeLogger{
		loggerCfg:      cfg,
		mainLog:        NewLogger(MainLog, cfg),
		logicalSqlLog:  NewLogger(LogicalSqlLog, cfg),
		physicalSqlLog: NewLogger(PhysicalSqlLog, cfg),
		txLog:          NewLogger(TxLog, cfg),
	}
}

func NewLogger(logType LogType, cfg *LoggingConfig) *zap.SugaredLogger {
	syncer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(buildLumberJack(cfg.LogPath, logType, cfg)), zapcore.AddSync(os.Stdout))

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	// notice: can use zapLoggerEncoderConfig directly, because it would write gibberish into the file.
	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	level := zap.DebugLevel
	if logType == MainLog {
		level = getLoggerLevel(cfg.LogLevel)
	}
	core := zapcore.NewCore(encoder, syncer, zap.NewAtomicLevelAt(level))
	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2)).Sugar()
}

//nolint:staticcheck
func NewSlowLogger(logPath string, cfg *LoggingConfig) *zap.SugaredLogger {
	syncer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(buildLumberJack(logPath, MainLog, cfg)), zapcore.AddSync(os.Stdout))

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	core := zapcore.NewCore(encoder, syncer, zap.NewAtomicLevelAt(zap.WarnLevel))
	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2)).Sugar()
}

//nolint:staticcheck
func buildLumberJack(logPath string, logType LogType, cfg *LoggingConfig) *lumberjack.Logger {
	var logName string

	if logPath == "" {
		logPath = currentPath()
	}

	switch logType {
	case MainLog:
		logName = cfg.LogName
	case LogicalSqlLog:
		logName = cfg.SqlLogName
	case PhysicalSqlLog:
		logName = cfg.PhysicalSqlLogName
	case TxLog:
		logName = cfg.TxLogName
	}

	return &lumberjack.Logger{
		Filename:   logPath + string(os.PathSeparator) + logName,
		MaxSize:    cfg.LogMaxSize,
		MaxBackups: cfg.LogMaxBackups,
		MaxAge:     cfg.LogMaxAge,
		Compress:   cfg.LogCompress,
	}
}

func NewContext(ctx context.Context, connectionID uint32, fields ...zapcore.Field) context.Context {
	return context.WithValue(ctx, LoggerKey(strconv.Itoa(int(connectionID))), WithContext(connectionID, ctx).With(fields...))
}

func WithContext(connectionID uint32, ctx context.Context) *compositeLogger {
	if ctx == nil {
		return globalLogger
	}
	ctxLogger, ok := ctx.Value(LoggerKey(strconv.Itoa(int(connectionID)))).(*compositeLogger)
	if ok {
		return ctxLogger
	}
	return NewCompositeLogger(loggerCfg)
}

func getLoggerLevel(level int) zapcore.Level {
	logLevel := LogLevel(level)
	if logLevel < _minLevel || logLevel > _maxLevel {
		return zapcore.Level(defaultLoggerLevel)
	}
	return zapcore.Level(logLevel)
}

func currentPath() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		globalLogger.Error("can not get current path")
	}
	return dir
}

func (c *compositeLogger) Debug(v ...interface{}) {
	c.mainLog.Debug(v...)
}

func (c *compositeLogger) Debugf(format string, v ...interface{}) {
	c.mainLog.Debugf(format, v...)
}

func (c *compositeLogger) DebugfWithLogType(logType LogType, format string, v ...interface{}) {
	c.mainLog.Debugf(format, v...)
	switch logType {
	case LogicalSqlLog:
		c.logicalSqlLog.Debugf(format, v...)
	case PhysicalSqlLog:
		c.physicalSqlLog.Debugf(format, v...)
	case TxLog:
		c.txLog.Debugf(format, v...)
	case MainLog:
		break
	}
}

func (c *compositeLogger) Info(v ...interface{}) {
	c.mainLog.Info(v...)
}

func (c *compositeLogger) Infof(format string, v ...interface{}) {
	c.mainLog.Infof(format, v...)
}

func (c *compositeLogger) InfofWithLogType(logType LogType, format string, v ...interface{}) {
	c.mainLog.Infof(format, v...)
	switch logType {
	case LogicalSqlLog:
		c.logicalSqlLog.Infof(format, v...)
	case PhysicalSqlLog:
		c.physicalSqlLog.Infof(format, v...)
	case TxLog:
		c.txLog.Infof(format, v...)
	case MainLog:
		break
	}
}

func (c *compositeLogger) Warn(v ...interface{}) {
	c.mainLog.Warn(v...)
}

func (c *compositeLogger) Warnf(format string, v ...interface{}) {
	c.mainLog.Warnf(format, v...)
}

func (c *compositeLogger) WarnfWithLogType(logType LogType, format string, v ...interface{}) {
	c.mainLog.Warnf(format, v...)
	switch logType {
	case LogicalSqlLog:
		c.logicalSqlLog.Warnf(format, v...)
	case PhysicalSqlLog:
		c.physicalSqlLog.Warnf(format, v...)
	case TxLog:
		c.txLog.Warnf(format, v...)
	case MainLog:
		break
	}
}

func (c *compositeLogger) Error(v ...interface{}) {
	c.mainLog.Error(v...)
}

func (c *compositeLogger) Errorf(format string, v ...interface{}) {
	c.mainLog.Errorf(format, v...)
}

func (c *compositeLogger) ErrorfWithLogType(logType LogType, format string, v ...interface{}) {
	c.mainLog.Errorf(format, v...)
	switch logType {
	case LogicalSqlLog:
		c.logicalSqlLog.Errorf(format, v...)
	case PhysicalSqlLog:
		c.physicalSqlLog.Errorf(format, v...)
	case TxLog:
		c.txLog.Errorf(format, v...)
	case MainLog:
		break
	}
}

func (c *compositeLogger) Panic(v ...interface{}) {
	c.mainLog.Panic(v...)
}

func (c *compositeLogger) Panicf(format string, v ...interface{}) {
	c.mainLog.Panicf(format, v...)
}

func (c *compositeLogger) PanicfWithLogType(logType LogType, format string, v ...interface{}) {
	c.mainLog.Panicf(format, v...)
	switch logType {
	case LogicalSqlLog:
		c.logicalSqlLog.Panicf(format, v...)
	case PhysicalSqlLog:
		c.physicalSqlLog.Panicf(format, v...)
	case TxLog:
		c.txLog.Panicf(format, v...)
	case MainLog:
		break
	}
}

func (c *compositeLogger) Fatal(v ...interface{}) {
	c.mainLog.Fatal(v...)
}

func (c *compositeLogger) Fatalf(format string, v ...interface{}) {
	c.mainLog.Fatalf(format, v...)
}

func (c *compositeLogger) FatalfWithLogType(logType LogType, format string, v ...interface{}) {
	c.mainLog.Fatalf(format, v...)
	switch logType {
	case LogicalSqlLog:
		c.logicalSqlLog.Fatalf(format, v...)
	case PhysicalSqlLog:
		c.physicalSqlLog.Fatalf(format, v...)
	case TxLog:
		c.txLog.Fatalf(format, v...)
	case MainLog:
		break
	}
}

func (c *compositeLogger) With(fields ...zap.Field) *compositeLogger {
	args := make([]interface{}, 0, len(fields))
	for _, field := range fields {
		args = append(args, field)
	}
	c.mainLog = c.mainLog.With(args...)
	c.logicalSqlLog = c.logicalSqlLog.With(args...)
	c.physicalSqlLog = c.physicalSqlLog.With(args...)
	c.txLog = c.txLog.With(args...)
	return c
}

// Debug ...
func Debug(v ...interface{}) {
	globalLogger.Debug(v...)
}

// Debugf ...
func Debugf(format string, v ...interface{}) {
	globalLogger.Debugf(format, v...)
}

// DebugfWithLogType ...
func DebugfWithLogType(logType LogType, format string, v ...interface{}) {
	globalLogger.DebugfWithLogType(logType, format, v)
}

// Info ...
func Info(v ...interface{}) {
	globalLogger.Info(v...)
}

// Infof ...
func Infof(format string, v ...interface{}) {
	globalLogger.Infof(format, v...)
}

// InfofWithLogType ...
func InfofWithLogType(logType LogType, format string, v ...interface{}) {
	globalLogger.InfofWithLogType(logType, format, v)
}

// Warn ...
func Warn(v ...interface{}) {
	globalLogger.Warn(v...)
}

// Warnf ...
func Warnf(format string, v ...interface{}) {
	globalLogger.Warnf(format, v...)
}

// WarnfWithLogType ...
func WarnfWithLogType(logType LogType, format string, v ...interface{}) {
	globalLogger.WarnfWithLogType(logType, format, v)
}

// Error ...
func Error(v ...interface{}) {
	globalLogger.Error(v...)
}

// Errorf ...
func Errorf(format string, v ...interface{}) {
	globalLogger.Errorf(format, v...)
}

// ErrorfWithLogType ...
func ErrorfWithLogType(logType LogType, format string, v ...interface{}) {
	globalLogger.ErrorfWithLogType(logType, format, v)
}

// Panic ...
func Panic(v ...interface{}) {
	globalLogger.Panic(v...)
}

// Panicf ...
func Panicf(format string, v ...interface{}) {
	globalLogger.Panicf(format, v...)
}

// ErrorfWithLogType ...
func PanicfWithLogType(logType LogType, format string, v ...interface{}) {
	globalLogger.PanicfWithLogType(logType, format, v)
}

// Fatal ...
func Fatal(v ...interface{}) {
	globalLogger.Fatal(v...)
}

// Fatalf ...
func Fatalf(format string, v ...interface{}) {
	globalLogger.Fatalf(format, v...)
}

// FatalfWithLogType ...
func FatalfWithLogType(logType LogType, format string, v ...interface{}) {
	globalLogger.FatalfWithLogType(logType, format, v)
}
