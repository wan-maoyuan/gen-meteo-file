package main

import (
	"context"
	"gen-meteo-file/pkg/config"
	"gen-meteo-file/pkg/server"
	"gen-meteo-file/pkg/tools/manager"
	"syscall"

	"github.com/sirupsen/logrus"
)

func init() {
	c, err := config.New()
	if err != nil {
		logrus.Fatalf("generate config error: %v", err)
	}

	c.Show()
}

func main() {
	ec := server.NewECServer()
	mfwam := server.NewMFWAMServer()
	smoc := server.NewSMOCSever()

	manager := manager.New(
		"气象源数据处理",
		manager.AddServer(ec, mfwam, smoc),
		manager.BeforeStart(BeforeStartFunc),
		manager.AfterStop(AfterStopFunc),
		manager.Signal(syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT),
	)

	defer func() {
		if err := manager.Stop(); err != nil {
			logrus.Errorf("停止所有的服务失败: %v", err)
		}
	}()

	if err := manager.Run(); err != nil {
		logrus.Errorf("启动所有的服务失败: %v", err)
	}
}

func BeforeStartFunc(ctx context.Context) error {

	return nil
}

func AfterStopFunc(ctx context.Context) error {

	return nil
}
