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
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

import (
	"github.com/natefinch/lumberjack"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	// LogLevel represents the level of logging.
	LogLevel int8
	// LogType represents the type of logging
	LogType string
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
	mainLog        Logger
	logicalSqlLog  Logger
	physicalSqlLog Logger
	txLog          Logger

	loggerCfg *LoggingConfig
	zapLogger *zap.Logger

	zapLoggerConfig        = zap.NewDevelopmentConfig()
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
)

func init() {
	zapLoggerConfig.EncoderConfig = zapLoggerEncoderConfig
	zapLogger, _ = zapLoggerConfig.Build(zap.AddCallerSkip(1))
	mainLog = zapLogger.Sugar()
}

func Init(cfg *LoggingConfig) {
	loggerCfg = cfg
	mainLog = NewLogger(MainLog, cfg)
	logicalSqlLog = NewLogger(LogicalSqlLog, cfg)
	physicalSqlLog = NewLogger(PhysicalSqlLog, cfg)
	txLog = NewLogger(TxLog, cfg)
}

func NewLogger(logType LogType, cfg *LoggingConfig) *zap.SugaredLogger {
	syncer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(buildLumberJack(cfg.LogPath, logType, cfg)), zapcore.AddSync(os.Stdout))

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	level := zap.DebugLevel
	if logType == MainLog {
		level = getLoggerLevel(cfg.LogLevel)
	}
	core := zapcore.NewCore(encoder, syncer, zap.NewAtomicLevelAt(level))
	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
}

//nolint:staticcheck
func NewSlowLogger(logPath string, cfg *LoggingConfig) *zap.SugaredLogger {
	syncer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(buildLumberJack(logPath, MainLog, cfg)), zapcore.AddSync(os.Stdout))

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	core := zapcore.NewCore(encoder, syncer, zap.NewAtomicLevelAt(zap.WarnLevel))
	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
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
		mainLog.Error("can not get current path")
	}
	return dir
}

// SetLogger customize yourself logger.
func SetLogger(logger Logger) {
	mainLog = logger
}

// GetLogger get logger
func GetLogger() Logger {
	return mainLog
}

// Debug ...
func Debug(v ...interface{}) {
	mainLog.Debug(v...)
}

// Debugf ...
func Debugf(format string, v ...interface{}) {
	mainLog.Debugf(format, v...)
}

// DebugfWithLogType ...
func DebugfWithLogType(logType LogType, format string, v ...interface{}) {
	mainLog.Debugf(format, v...)
	switch logType {
	case LogicalSqlLog:
		logicalSqlLog.Debugf(format, v)
	case PhysicalSqlLog:
		physicalSqlLog.Debugf(format, v)
	case TxLog:
		txLog.Debugf(format, v)
	case MainLog:
		break
	}
}

// Info ...
func Info(v ...interface{}) {
	mainLog.Info(v...)
}

// Infof ...
func Infof(format string, v ...interface{}) {
	mainLog.Infof(format, v...)
}

// InfofWithLogType ...
func InfofWithLogType(logType LogType, format string, v ...interface{}) {
	mainLog.Infof(format, v...)
	switch logType {
	case LogicalSqlLog:
		logicalSqlLog.Infof(format, v)
	case PhysicalSqlLog:
		physicalSqlLog.Infof(format, v)
	case TxLog:
		txLog.Infof(format, v)
	case MainLog:
		break
	}
}

// Warn ...
func Warn(v ...interface{}) {
	mainLog.Warn(v...)
}

// Warnf ...
func Warnf(format string, v ...interface{}) {
	mainLog.Warnf(format, v...)
}

// WarnfWithLogType ...
func WarnfWithLogType(logType LogType, format string, v ...interface{}) {
	mainLog.Warnf(format, v...)
	switch logType {
	case LogicalSqlLog:
		logicalSqlLog.Warnf(format, v)
	case PhysicalSqlLog:
		physicalSqlLog.Warnf(format, v)
	case TxLog:
		txLog.Warnf(format, v)
	case MainLog:
		break
	}
}

// Error ...
func Error(v ...interface{}) {
	mainLog.Error(v...)
}

// Errorf ...
func Errorf(format string, v ...interface{}) {
	mainLog.Errorf(format, v...)
}

// ErrorfWithLogType ...
func ErrorfWithLogType(logType LogType, format string, v ...interface{}) {
	mainLog.Errorf(format, v...)
	switch logType {
	case LogicalSqlLog:
		logicalSqlLog.Errorf(format, v)
	case PhysicalSqlLog:
		physicalSqlLog.Errorf(format, v)
	case TxLog:
		txLog.Errorf(format, v)
	case MainLog:
		break
	}
}

// Panic ...
func Panic(v ...interface{}) {
	mainLog.Panic(v...)
}

// Panicf ...
func Panicf(format string, v ...interface{}) {
	mainLog.Panicf(format, v...)
}

// ErrorfWithLogType ...
func PanicfWithLogType(logType LogType, format string, v ...interface{}) {
	mainLog.Panicf(format, v...)
	switch logType {
	case LogicalSqlLog:
		logicalSqlLog.Panicf(format, v)
	case PhysicalSqlLog:
		physicalSqlLog.Panicf(format, v)
	case TxLog:
		txLog.Panicf(format, v)
	case MainLog:
		break
	}
}

// Fatal ...
func Fatal(v ...interface{}) {
	mainLog.Fatal(v...)
}

// Fatalf ...
func Fatalf(format string, v ...interface{}) {
	mainLog.Fatalf(format, v...)
}

// FatalfWithLogType ...
func FatalfWithLogType(logType LogType, format string, v ...interface{}) {
	mainLog.Fatalf(format, v...)
	switch logType {
	case LogicalSqlLog:
		logicalSqlLog.Fatalf(format, v)
	case PhysicalSqlLog:
		physicalSqlLog.Fatalf(format, v)
	case TxLog:
		txLog.Fatalf(format, v)
	case MainLog:
		break
	}
}
