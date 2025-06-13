package server

import (
	"context"
	"fmt"
	"gen-meteo-file/pkg/config"
	"gen-meteo-file/pkg/tools/nc"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

type ECServer struct {
	inputDir  string
	outputDir string
}

func NewECServer() *ECServer {
	return &ECServer{
		inputDir:  filepath.Join(config.Get().Server.NCDir, "ec_0p25"),
		outputDir: filepath.Join(config.Get().Server.CSVDir),
	}
}

func (s *ECServer) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			today, _ := time.Parse("20060102", time.Now().Format("20060102"))
			for range 8 * 3 {
				select {
				case <-ctx.Done():
					return nil
				default:
					if err := s.GenByDate(ctx, today); err != nil {
						logrus.Errorf("generate ec file by date failed: %v, date: %v", err, today)
					}

					today = today.Add(-time.Hour * 3)
				}
			}

			ticker.Reset(time.Hour * 24)
		}
	}
}

func (s *ECServer) GenByDate(ctx context.Context, date time.Time) error {
	var hour = 0
	if date.Hour() >= 12 {
		hour = 12
	}

	// /data2/alist_share/nc-files/ec_0p25/2025/2025-01-01/oper-00/ec_0p25_oper_2025010100_0h.nc
	info := &nc.NCFile{
		DateTime:   date,
		InputPath:  filepath.Join(s.inputDir, fmt.Sprintf("%d", date.Year()), date.Format(time.DateOnly), fmt.Sprintf("oper-%02d", hour), fmt.Sprintf("ec_0p25_oper_%s%02d_%dh.nc", date.Format("20060102"), hour, date.Hour()-hour)),
		OutputPath: filepath.Join(s.outputDir, fmt.Sprintf("%d", date.Year()), date.Format(time.DateOnly), fmt.Sprintf("ec_%s.csv", date.Format("2006010215"))),
	}

	nc, err := nc.NewECOper(info)
	if err != nil {
		return fmt.Errorf("new ec oper failed: %v", err)
	}
	defer nc.Close()

	if err := nc.Analysis(); err != nil {
		return fmt.Errorf("ec oper analysis failed: %v", err)
	}

	if err := nc.GenerateCSV(); err != nil {
		return fmt.Errorf("ec oper generate csv failed: %v", err)
	}

	return nil
}

func (s *ECServer) Stop(ctx context.Context) error {
	return nil
}
