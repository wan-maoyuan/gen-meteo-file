package server

import (
	"context"
	"fmt"
	"gen-meteo-file/pkg/config"
	"gen-meteo-file/pkg/tools/nc"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type SMOCSever struct {
	inputDir  string
	outputDir string
}

func NewSMOCSever() *SMOCSever {
	return &SMOCSever{
		inputDir:  filepath.Join(config.Get().Server.NCDir, "smoc"),
		outputDir: filepath.Join(config.Get().Server.CSVDir),
	}
}

func (s *SMOCSever) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			today, _ := time.Parse("20060102", time.Now().Format("20060102"))
			for range 2 {
				select {
				case <-ctx.Done():
					return nil
				default:
					if err := s.GenByDate(ctx, today); err != nil {
						logrus.Errorf("generate smoc file by date failed: %v, date: %v", err, today)
					}

					today = today.Add(-time.Hour * 24)
				}
			}

			ticker.Reset(time.Hour * 24)
		}
	}
}

func (s *SMOCSever) GenByDate(ctx context.Context, date time.Time) error {
	path, err := s.getSMOCPath(date)
	if err != nil {
		return fmt.Errorf("get smoc path failed: %v", err)
	}

	info := &nc.NCFile{
		DateTime:   date,
		InputPath:  path,
		OutputPath: filepath.Join(s.outputDir, fmt.Sprintf("%d", date.Year()), date.Format(time.DateOnly), fmt.Sprintf("smoc_%s.csv", date.Format("20060102"))),
	}

	nc, err := nc.NewSMOC(info)
	if err != nil {
		return fmt.Errorf("new smoc failed: %v", err)
	}
	defer nc.Close()

	if err := nc.Analysis(); err != nil {
		return fmt.Errorf("smoc analysis failed: %v", err)
	}

	if err := nc.GenerateCSV(); err != nil {
		return fmt.Errorf("smoc generate csv failed: %v", err)
	}

	return nil
}

func (s *SMOCSever) Stop(ctx context.Context) error {
	return nil
}

func (s *SMOCSever) getSMOCPath(date time.Time) (string, error) {
	dir := filepath.Join(s.inputDir, fmt.Sprintf("%d", date.Year()), fmt.Sprintf("%02d", date.Month()))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("read smoc dir: %s failed: %v", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if strings.Contains(entry.Name(), date.Format("20060102")) {
			return filepath.Join(dir, entry.Name()), nil
		}
	}

	return "", fmt.Errorf("mfwam file not found: %s", date.Format("20060102"))
}
