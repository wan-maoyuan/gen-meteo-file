package nc

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/batchatco/go-native-netcdf/netcdf"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

const (
	SMOCLatitudeField      = "latitude"
	SMOCLongitudeField     = "longitude"
	SMOCUCurrentField      = "utotal" // (time=24, depth=1, latitude=2041, longitude=4320)
	SMOCVCurrentField      = "vtotal" // (time=24, depth=1, latitude=2041, longitude=4320)
	SMOCUTideCurrentField  = "utide"  // (time=24, depth=1, latitude=2041, longitude=4320)
	SMOCVTideCurrentField  = "vtide"  // (time=24, depth=1, latitude=2041, longitude=4320)
	SMOCFillValueAttribute = "_FillValue"
	SMOCLatitudeCount      = 2041
	SMOCLongitudeCount     = 4320
	SMOCTimeCount          = 24
	SMOCStep               = float32(1. / 12.)
)

type SMOC struct {
	info                  *NCFile
	group                 api.Group
	latitudeList          []float32
	longitudeList         []float32
	uCurrentList          [][][][]float32
	uCurrentFillValue     float32
	vCurrentList          [][][][]float32
	vCurrentFillValue     float32
	uTideCurrentList      [][][][]float32
	uTideCurrentFillValue float32
	vTideCurrentList      [][][][]float32
	vTideCurrentFillValue float32
}

func NewSMOC(info *NCFile) (*SMOC, error) {
	if _, err := os.Stat(info.InputPath); err != nil {
		return nil, fmt.Errorf("smoc input file: %s not exists", info.InputPath)
	}

	if _, err := os.Stat(info.OutputPath); err == nil {
		return nil, fmt.Errorf("smoc output file: %s already exists", info.OutputPath)
	}

	if _, err := os.Stat(info.CompressionPath); err == nil {
		return nil, fmt.Errorf("smoc compression file: %s already exists", info.CompressionPath)
	}

	if err := os.MkdirAll(filepath.Dir(info.OutputPath), os.FileMode(0755)); err != nil {
		return nil, fmt.Errorf("create smoc output dir: %s failed: %v", filepath.Dir(info.OutputPath), err)
	}

	group, err := netcdf.Open(info.InputPath)
	if err != nil {
		return nil, fmt.Errorf("open smoc netcdf file: %s failed: %v", info.InputPath, err)
	}

	return &SMOC{
		info:  info,
		group: group,
	}, nil
}

// latitude: -80~90
// longitude: -180~180
// 1 / 12
func (nc *SMOC) Analysis() error {
	latitudeVari, err := nc.group.GetVariable(SMOCLatitudeField)
	if err != nil {
		return fmt.Errorf("解析 smoc 变量: %s 失败: %v", SMOCLatitudeField, err)
	}
	nc.latitudeList = latitudeVari.Values.([]float32)

	longitudeVari, err := nc.group.GetVariable(SMOCLongitudeField)
	if err != nil {
		return fmt.Errorf("解析 smoc 变量: %s 失败: %v", SMOCLongitudeField, err)
	}
	nc.longitudeList = longitudeVari.Values.([]float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	uCurrentVari, err := nc.group.GetVariable(SMOCUCurrentField)
	if err != nil {
		return fmt.Errorf("解析 smoc 变量: %s 失败: %v", SMOCUCurrentField, err)
	}
	fillvalue, ok := uCurrentVari.Attributes.Get(SMOCFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 smoc 变量: %s 属性: %s 失败: %v", SMOCUCurrentField, SMOCFillValueAttribute, err)
	}
	nc.uCurrentList = uCurrentVari.Values.([][][][]float32)
	nc.uCurrentFillValue = fillvalue.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	vCurrentVari, err := nc.group.GetVariable(SMOCVCurrentField)
	if err != nil {
		return fmt.Errorf("解析 smoc 变量: %s 失败: %v", SMOCVCurrentField, err)
	}
	fillvalue, ok = vCurrentVari.Attributes.Get(SMOCFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 smoc 变量: %s 属性: %s 失败: %v", SMOCVCurrentField, SMOCFillValueAttribute, err)
	}
	nc.vCurrentList = vCurrentVari.Values.([][][][]float32)
	nc.vCurrentFillValue = fillvalue.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	uTideCurrentVari, err := nc.group.GetVariable(SMOCUTideCurrentField)
	if err != nil {
		return fmt.Errorf("解析 smoc 变量: %s 失败: %v", SMOCUTideCurrentField, err)
	}
	fillvalue, ok = uTideCurrentVari.Attributes.Get(SMOCFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 smoc 变量: %s 属性: %s 失败: %v", SMOCUTideCurrentField, SMOCFillValueAttribute, err)
	}
	nc.uTideCurrentList = uTideCurrentVari.Values.([][][][]float32)
	nc.uTideCurrentFillValue = fillvalue.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	vTideCurrentVari, err := nc.group.GetVariable(SMOCVTideCurrentField)
	if err != nil {
		return fmt.Errorf("解析 smoc 变量: %s 失败: %v", SMOCVTideCurrentField, err)
	}
	fillvalue, ok = vTideCurrentVari.Attributes.Get(SMOCFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 smoc 变量: %s 属性: %s 失败: %v", SMOCVTideCurrentField, SMOCFillValueAttribute, err)
	}
	nc.vTideCurrentList = vTideCurrentVari.Values.([][][][]float32)
	nc.vTideCurrentFillValue = fillvalue.(float32)

	return nil
}

func (nc *SMOC) GenerateCSV() error {
	file, err := os.OpenFile(nc.info.OutputPath, os.O_CREATE|os.O_RDWR, os.FileMode(0664))
	if err != nil {
		return fmt.Errorf("output file: %s create failed: %v", nc.info.OutputPath, err)
	}
	defer file.Close()

	buf := bufio.NewWriter(file)
	buf.WriteString("lat,lon,dateTime,uCurrent,vCurrent,uTideCurrent,vTideCurrent\n")

	for timeIndex := 0; timeIndex < SMOCTimeCount; timeIndex += 3 {
		for latIndex := 0; latIndex < SMOCLatitudeCount; latIndex += 3 {
			for lonIndex := 0; lonIndex < SMOCLongitudeCount; lonIndex += 3 {
				buf.WriteString(fmt.Sprintf("%.3f,%.3f,%s",
					nc.latitudeList[latIndex],
					nc.longitudeList[lonIndex],
					nc.info.DateTime.Add(time.Hour*time.Duration(timeIndex)).Format(time.DateTime),
				))

				uCurrent := nc.uCurrentList[timeIndex][0][latIndex][lonIndex]
				if uCurrent == nc.uCurrentFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", uCurrent))
				}

				vCurrent := nc.vCurrentList[timeIndex][0][latIndex][lonIndex]
				if vCurrent == nc.vCurrentFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", vCurrent))
				}

				uTideCurrent := nc.uTideCurrentList[timeIndex][0][latIndex][lonIndex]
				if uTideCurrent == nc.uTideCurrentFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", uTideCurrent))
				}

				vTideCurrent := nc.vTideCurrentList[timeIndex][0][latIndex][lonIndex]
				if vTideCurrent == nc.vTideCurrentFillValue {
					buf.WriteString(",NaN\n")
				} else {
					buf.WriteString(fmt.Sprintf(",%f\n", vTideCurrent))
				}
			}
		}

		buf.Flush()
	}

	buf.Flush()
	return zipFile(nc.info.OutputPath, nc.info.CompressionPath)
}

func (nc *SMOC) Close() {
	nc.group.Close()
}
