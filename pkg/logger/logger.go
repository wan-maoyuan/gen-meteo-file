package logger

import (
	"fmt"
	"gen-meteo-file/pkg/global"
	"io"

	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var stringToLevel = map[string]logrus.Level{
	"debug": logrus.DebugLevel,
	"info":  logrus.InfoLevel,
	"warn":  logrus.WarnLevel,
	"error": logrus.ErrorLevel,
	"fatal": logrus.FatalLevel,
	"panic": logrus.PanicLevel,
}

type Log struct {
	File       string         `mapstructure:"file" yaml:"file"`
	Level      string         `mapstructure:"level" yaml:"level"`
	MaxSize    int            `mapstructure:"size" yaml:"size"`
	MaxAge     int            `mapstructure:"age" yaml:"age"`
	MaxBackups int            `mapstructure:"backups" yaml:"backups"`
	Writer     io.WriteCloser `mapstructure:"compress" yaml:"-"`
	IsStdout   bool           `mapstructure:"compress" yaml:"-"`
}

func NewLog() Log {
	return Log{
		Level:      global.DefaultLevel,
		File:       "",
		MaxSize:    global.DefaultMaxLogSize,
		MaxAge:     global.DefaultMaxLogAge,
		MaxBackups: global.DefaultMaxBackups,
		Writer:     nil,
		IsStdout:   true,
	}
}

func (l *Log) InitLog() {
	l.CheckDefault()
	if l.File != "" {
		l.File = MakeDirectory(l.File)
	}

	l.ConfigWriter()
	logrus.SetOutput(l.Writer)
	logrus.SetLevel(stringToLevel[l.Level])
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
}

func (l *Log) ConfigWriter() {
	if l.File == "" {
		l.IsStdout = true
		l.Writer = os.Stdout
		return
	}

	l.IsStdout = false
	l.Writer = &lumberjack.Logger{
		Filename:   l.File,
		MaxSize:    l.MaxSize,
		MaxBackups: l.MaxBackups,
		MaxAge:     l.MaxAge,
	}
}

func (l *Log) CheckDefault() {
	if l.MaxAge == 0 {
		l.MaxAge = global.DefaultMaxLogAge
	}
	if l.MaxSize == 0 {
		l.MaxSize = global.DefaultMaxLogSize
	}
	if l.MaxBackups == 0 {
		l.MaxBackups = global.DefaultMaxBackups
	}
	if _, ok := stringToLevel[l.Level]; !ok {
		l.Level = global.DefaultLevel
	}
}

func (l *Log) Printf(format string, args ...interface{}) {
	logrus.Printf(format, args...)
}

func (l *Log) Close() {
	if l.Writer == nil || l.IsStdout {
		return
	}

	if err := l.Writer.Close(); err != nil {
		fmt.Printf("close Log writer error: %v \n", err)
	}
}

func GetWorkDir() string {
	dir, err := os.Getwd()
	if err != nil {
		logrus.Warnf("Cannot get the current directory: %v, using $HOME directory!", err)
		dir, err = os.UserHomeDir()
		if err != nil {
			logrus.Warnf("Cannot get the user home directory: %v, using /tmp directory!", err)
			dir = os.TempDir()
		}
	}
	return dir
}

func MakeDirectory(filename string) string {
	dir, file := filepath.Split(filename)
	if len(dir) <= 0 {
		dir = GetWorkDir()
	}

	if len(file) <= 0 {
		return dir
	}

	if strings.HasPrefix(dir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			logrus.Warnf("Cannot get the user home directory: %v, using /tmp directory as home", err)
			home = os.TempDir()
		}
		dir = filepath.Join(home, dir[2:])
	}

	dir, err := filepath.Abs(dir)
	if err != nil {
		logrus.Warnf("Cannot get the absolute path: %v", err)
		dir = GetWorkDir()
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			logrus.Warnf("Cannot create the directory: %v", err)
			dir = GetWorkDir()
		}
	}

	return filepath.Join(dir, file)
}
