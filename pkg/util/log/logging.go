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
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

import (
	"github.com/creasty/defaults"

	"github.com/docker/go-units"

	"github.com/natefinch/lumberjack"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	// LogType represents the type of logging.
	LogType string
	// LoggerKey represents the context key of logging.
	LoggerKey string
)

const (
	MainLog        = LogType("main")
	LogicalSqlLog  = LogType("logical sql")
	PhysicalSqlLog = LogType("physical sql")
	TxLog          = LogType("tx")
)

const (
	_defaultLogName = "arana.log"
	_sqlLogName     = "sql.log"
	_txLogName      = "tx.log"
	_phySqlLogName  = "physql.log"
)

type Config struct {
	Path          string `default:"~/arana/logs" yaml:"path" json:"path"`
	Level         string `default:"INFO" yaml:"level" json:"level"`
	MaxSize       string `default:"128MB" yaml:"max_size" json:"max_size"`
	MaxBackups    int    `default:"3" yaml:"max_backups" json:"max_backups"`
	MaxAge        int    `default:"7" yaml:"max_age" json:"max_age"`
	Compress      bool   `yaml:"compress" json:"compress"`
	Console       bool   `yaml:"console" json:"console"`
	SqlLogEnabled bool   `yaml:"sql_log_enabled" json:"sql_log_enabled"`
}

// Logger TODO add methods support LogType
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
	_globalLogger      atomic.Value // *CompositeLogger
	_globalLoggerMutex sync.Mutex
)

// DefaultConfig returns a *Config with default settings.
// These settings will be loaded from OS environs automatically.
func DefaultConfig() *Config {
	var c Config
	defaults.MustSet(&c)

	if s, ok := os.LookupEnv("ARANA_LOG_PATH"); ok {
		c.Path = s
	}

	if s, ok := os.LookupEnv("ARANA_LOG_LEVEL"); ok {
		c.Level = s
	}

	if s, ok := os.LookupEnv("ARANA_LOG_MAX_SIZE"); ok {
		c.MaxSize = s
	}

	if s, ok := os.LookupEnv("ARANA_LOG_MAX_BACKUPS"); ok {
		n, _ := strconv.Atoi(s)
		c.MaxBackups = n
	}

	if s, ok := os.LookupEnv("ARANA_LOG_MAX_AGE"); ok {
		n, _ := strconv.Atoi(s)
		c.MaxAge = n
	}

	isYes := func(s string) bool {
		switch strings.TrimSpace(strings.ToLower(s)) {
		case "yes", "on", "1", "true":
			return true
		}
		return false
	}

	if s, ok := os.LookupEnv("ARANA_LOG_COMPRESS"); ok {
		c.Compress = isYes(s)
	}

	if s, ok := os.LookupEnv("ARANA_LOG_CONSOLE"); ok {
		c.Console = isYes(s)
	}

	if s, ok := os.LookupEnv("ARANA_LOG_SQL"); ok {
		c.SqlLogEnabled = isYes(s)
	}

	return &c
}

func Init(cfg *Config) {
	_globalLoggerMutex.Lock()
	defer _globalLoggerMutex.Unlock()
	_globalLogger.Store(NewCompositeLogger(cfg))
}

func getGlobalLogger() *CompositeLogger {
	if l, ok := _globalLogger.Load().(*CompositeLogger); ok {
		return l
	}

	_globalLoggerMutex.Lock()
	defer _globalLoggerMutex.Unlock()

	if l, ok := _globalLogger.Load().(*CompositeLogger); ok {
		return l
	}

	var c Config
	_ = defaults.Set(&c)
	l := NewCompositeLogger(&c)
	_globalLogger.Store(l)

	return l
}

type CompositeLogger struct {
	loggerCfg *Config

	mainLog        *zap.SugaredLogger
	logicalSqlLog  *zap.SugaredLogger
	physicalSqlLog *zap.SugaredLogger
	txLog          *zap.SugaredLogger

	// TODO replace global slowLog
	slowLog *zap.SugaredLogger
}

func NewCompositeLogger(cfg *Config) *CompositeLogger {
	return &CompositeLogger{
		loggerCfg:      cfg,
		mainLog:        NewLogger(MainLog, cfg),
		logicalSqlLog:  NewLogger(LogicalSqlLog, cfg),
		physicalSqlLog: NewLogger(PhysicalSqlLog, cfg),
		txLog:          NewLogger(TxLog, cfg),
	}
}

func scanLevel(s string, l *zapcore.Level) bool {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "debug":
		*l = zap.DebugLevel
	case "info":
		*l = zap.InfoLevel
	case "warn", "warning":
		*l = zap.WarnLevel
	case "error", "err":
		*l = zap.ErrorLevel
	default:
		return false
	}
	return true
}

func NewLogger(logType LogType, cfg *Config) *zap.SugaredLogger {
	var ws []zapcore.WriteSyncer
	ws = append(ws, zapcore.AddSync(buildLumberJack(cfg.Path, logType, cfg)))

	level := zap.DebugLevel
	switch logType {
	case MainLog:
		if cfg.Console {
			ws = append(ws, zapcore.AddSync(os.Stdout))
		}
		_ = scanLevel(cfg.Level, &level)
	}

	syncer := zapcore.NewMultiWriteSyncer(ws...)

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	// notice: can use zapLoggerEncoderConfig directly, because it would write gibberish into the file.
	encoder := zapcore.NewConsoleEncoder(encoderConfig)

	core := zapcore.NewCore(encoder, syncer, zap.NewAtomicLevelAt(level))
	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2)).Sugar()
}

//nolint:staticcheck
func NewSlowLogger(logPath string, cfg *Config) *zap.SugaredLogger {
	syncer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(buildLumberJack(logPath, MainLog, cfg)), zapcore.AddSync(os.Stdout))

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	core := zapcore.NewCore(encoder, syncer, zap.NewAtomicLevelAt(zap.WarnLevel))
	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2)).Sugar()
}

//nolint:staticcheck
func buildLumberJack(logPath string, logType LogType, cfg *Config) *lumberjack.Logger {
	var logName string

	if logPath == "" {
		logPath = "."
	}

	if strings.HasPrefix(logPath, "~") {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = "."
		}
		logPath = home + logPath[1:]
	}

	switch logType {
	case MainLog:
		logName = _defaultLogName
	case LogicalSqlLog:
		logName = _sqlLogName
	case PhysicalSqlLog:
		logName = _phySqlLogName
	case TxLog:
		logName = _txLogName
	}

	filename := filepath.Clean(filepath.Join(logPath, logName))
	maxSize, _ := units.FromHumanSize(cfg.MaxSize)

	return &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    int(maxSize),
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
		Compress:   cfg.Compress,
	}
}

func NewContext(ctx context.Context, connectionID uint32, fields ...zapcore.Field) context.Context {
	return context.WithValue(ctx, LoggerKey(strconv.Itoa(int(connectionID))), WithContext(connectionID, ctx).With(fields...))
}

func WithContext(connectionID uint32, ctx context.Context) *CompositeLogger {
	if ctx == nil {
		return getGlobalLogger()
	}
	ctxLogger, ok := ctx.Value(LoggerKey(strconv.Itoa(int(connectionID)))).(*CompositeLogger)
	if ok {
		return ctxLogger
	}
	return getGlobalLogger()
}

func (c *CompositeLogger) Debug(v ...interface{}) {
	c.mainLog.Debug(v...)
}

func (c *CompositeLogger) Debugf(format string, v ...interface{}) {
	c.mainLog.Debugf(format, v...)
}

func (c *CompositeLogger) DebugfWithLogType(logType LogType, format string, v ...interface{}) {
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

func (c *CompositeLogger) Info(v ...interface{}) {
	c.mainLog.Info(v...)
}

func (c *CompositeLogger) Infof(format string, v ...interface{}) {
	c.mainLog.Infof(format, v...)
}

func (c *CompositeLogger) InfofWithLogType(logType LogType, format string, v ...interface{}) {
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

func (c *CompositeLogger) Warn(v ...interface{}) {
	c.mainLog.Warn(v...)
}

func (c *CompositeLogger) Warnf(format string, v ...interface{}) {
	c.mainLog.Warnf(format, v...)
}

func (c *CompositeLogger) WarnfWithLogType(logType LogType, format string, v ...interface{}) {
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

func (c *CompositeLogger) Error(v ...interface{}) {
	c.mainLog.Error(v...)
}

func (c *CompositeLogger) Errorf(format string, v ...interface{}) {
	c.mainLog.Errorf(format, v...)
}

func (c *CompositeLogger) ErrorfWithLogType(logType LogType, format string, v ...interface{}) {
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

func (c *CompositeLogger) Panic(v ...interface{}) {
	c.mainLog.Panic(v...)
}

func (c *CompositeLogger) Panicf(format string, v ...interface{}) {
	c.mainLog.Panicf(format, v...)
}

func (c *CompositeLogger) PanicfWithLogType(logType LogType, format string, v ...interface{}) {
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

func (c *CompositeLogger) Fatal(v ...interface{}) {
	c.mainLog.Fatal(v...)
}

func (c *CompositeLogger) Fatalf(format string, v ...interface{}) {
	c.mainLog.Fatalf(format, v...)
}

func (c *CompositeLogger) FatalfWithLogType(logType LogType, format string, v ...interface{}) {
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

func (c *CompositeLogger) With(fields ...zap.Field) *CompositeLogger {
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
	getGlobalLogger().Debug(v...)
}

// Debugf ...
func Debugf(format string, v ...interface{}) {
	getGlobalLogger().Debugf(format, v...)
}

// DebugfWithLogType ...
func DebugfWithLogType(logType LogType, format string, v ...interface{}) {
	getGlobalLogger().DebugfWithLogType(logType, format, v)
}

// Info ...
func Info(v ...interface{}) {
	getGlobalLogger().Info(v...)
}

// Infof ...
func Infof(format string, v ...interface{}) {
	getGlobalLogger().Infof(format, v...)
}

// InfofWithLogType ...
func InfofWithLogType(logType LogType, format string, v ...interface{}) {
	getGlobalLogger().InfofWithLogType(logType, format, v)
}

// Warn ...
func Warn(v ...interface{}) {
	getGlobalLogger().Warn(v...)
}

// Warnf ...
func Warnf(format string, v ...interface{}) {
	getGlobalLogger().Warnf(format, v...)
}

// WarnfWithLogType ...
func WarnfWithLogType(logType LogType, format string, v ...interface{}) {
	getGlobalLogger().WarnfWithLogType(logType, format, v)
}

// Error ...
func Error(v ...interface{}) {
	getGlobalLogger().Error(v...)
}

// Errorf ...
func Errorf(format string, v ...interface{}) {
	getGlobalLogger().Errorf(format, v...)
}

// ErrorfWithLogType ...
func ErrorfWithLogType(logType LogType, format string, v ...interface{}) {
	getGlobalLogger().ErrorfWithLogType(logType, format, v)
}

// Panic ...
func Panic(v ...interface{}) {
	getGlobalLogger().Panic(v...)
}

// Panicf ...
func Panicf(format string, v ...interface{}) {
	getGlobalLogger().Panicf(format, v...)
}

// ErrorfWithLogType ...
func PanicfWithLogType(logType LogType, format string, v ...interface{}) {
	getGlobalLogger().PanicfWithLogType(logType, format, v)
}

// Fatal ...
func Fatal(v ...interface{}) {
	getGlobalLogger().Fatal(v...)
}

// Fatalf ...
func Fatalf(format string, v ...interface{}) {
	getGlobalLogger().Fatalf(format, v...)
}

// FatalfWithLogType ...
func FatalfWithLogType(logType LogType, format string, v ...interface{}) {
	getGlobalLogger().FatalfWithLogType(logType, format, v)
}
