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
	ECOperLatitudeField        = "lat"
	ECOperLongitudeField       = "lon"
	ECOperWind10mUField        = "10u" // 10米高度风的水平分量 (time=1, height_3=1, lat=721, lon=1440)
	ECOperWind10mVField        = "10v" // 10米高度风的垂直分量 (time=1, height_3=1, lat=721, lon=1440)
	ECOperTemperature2mField   = "2t"  // 2米高度的温度 (time=1, height_2=1, lat=721, lon=1440)
	ECOperSurfacePressureField = "sp"  // 大气压强 (time=1, lat=721, lon=1440)
	ECOperLatitudeCount        = 721
	ECOperLongitudeCount       = 1440
	ECOperStep                 = float32(0.25)
)

type ECOper struct {
	info                *NCFile
	group               api.Group
	latitudeList        []float64
	longitudeList       []float64
	wind10mUList        [][][][]float32
	wind10mVList        [][][][]float32
	temperature2mList   [][][][]float32
	surfacePressureList [][][]float32
}

func NewECOper(info *NCFile) (*ECOper, error) {
	if _, err := os.Stat(info.InputPath); err != nil {
		return nil, fmt.Errorf("ec_oper input file: %s not exists", info.InputPath)
	}

	if _, err := os.Stat(info.OutputPath); err == nil {
		return nil, fmt.Errorf("ec_oper output file: %s already exists", info.OutputPath)
	}

	if err := os.MkdirAll(filepath.Dir(info.OutputPath), os.FileMode(0755)); err != nil {
		return nil, fmt.Errorf("create ec_oper output dir: %s failed: %v", filepath.Dir(info.OutputPath), err)
	}

	group, err := netcdf.Open(info.InputPath)
	if err != nil {
		return nil, fmt.Errorf("open ec_oper netcdf file: %s failed: %v", info.InputPath, err)
	}

	return &ECOper{
		info:  info,
		group: group,
	}, nil
}

// latitude: 90 ~ -90
// longitude: -180~180
// 0.25
func (nc *ECOper) Analysis() error {
	latitudeVari, err := nc.group.GetVariable(ECOperLatitudeField)
	if err != nil {
		return fmt.Errorf("解析 ec_oper 变量: %s 失败: %v", ECOperLatitudeField, err)
	}
	nc.latitudeList = latitudeVari.Values.([]float64)

	longitudeVari, err := nc.group.GetVariable(ECOperLongitudeField)
	if err != nil {
		return fmt.Errorf("解析 ec_oper 变量: %s 失败: %v", ECOperLongitudeField, err)
	}
	nc.longitudeList = longitudeVari.Values.([]float64)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	wind10mUVari, err := nc.group.GetVariable(ECOperWind10mUField)
	if err != nil {
		return fmt.Errorf("解析 ec_oper 变量: %s 失败: %v", ECOperWind10mUField, err)
	}
	nc.wind10mUList = wind10mUVari.Values.([][][][]float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	wind10mVVari, err := nc.group.GetVariable(ECOperWind10mVField)
	if err != nil {
		return fmt.Errorf("解析 ec_oper 变量: %s 失败: %v", ECOperWind10mVField, err)
	}
	nc.wind10mVList = wind10mVVari.Values.([][][][]float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	temperature2mVari, err := nc.group.GetVariable(ECOperTemperature2mField)
	if err != nil {
		return fmt.Errorf("解析 ec_oper 变量: %s 失败: %v", ECOperTemperature2mField, err)
	}
	nc.temperature2mList = temperature2mVari.Values.([][][][]float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	surfacePressureVari, err := nc.group.GetVariable(ECOperSurfacePressureField)
	if err != nil {
		return fmt.Errorf("解析 ec_oper 变量: %s 失败: %v", ECOperSurfacePressureField, err)
	}
	nc.surfacePressureList = surfacePressureVari.Values.([][][]float32)

	return nil
}

func (nc *ECOper) GenerateCSV() error {
	file, err := os.OpenFile(nc.info.OutputPath, os.O_CREATE|os.O_RDWR, os.FileMode(0664))
	if err != nil {
		return fmt.Errorf("output file: %s create failed: %v", nc.info.OutputPath, err)
	}
	defer file.Close()

	buf := bufio.NewWriter(file)
	buf.WriteString("lat,lon,dateTime,wind10mU,wind10mV,temperature2m,surfacePressure\n")
	for latIndex := range ECOperLatitudeCount {
		for lonIndex := range ECOperLongitudeCount {
			buf.WriteString(fmt.Sprintf(
				"%.2f,%.2f,%s",
				nc.latitudeList[latIndex], nc.longitudeList[lonIndex],
				nc.info.DateTime.UTC().Format(time.DateTime),
			))

			buf.WriteString(fmt.Sprintf(",%f", nc.wind10mUList[0][0][latIndex][lonIndex]))
			buf.WriteString(fmt.Sprintf(",%f", nc.wind10mVList[0][0][latIndex][lonIndex]))
			buf.WriteString(fmt.Sprintf(",%f", nc.temperature2mList[0][0][latIndex][lonIndex]))
			buf.WriteString(fmt.Sprintf(",%f", nc.surfacePressureList[0][latIndex][lonIndex]))
			buf.WriteString("\n")
		}
	}

	buf.Flush()

	return nil
}

func (nc *ECOper) Close() {
	nc.group.Close()
}
