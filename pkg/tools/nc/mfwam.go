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
	MFWAMLatitudeField       = "latitude"
	MFWAMLongitudeField      = "longitude"
	MFWAMSeaHeightField      = "VHM0"      // 海浪高度 (time=4, latitude=2041, longitude=4320)
	MFWAMSeaDirectionField   = "VMDR"      // 海浪方向 (time=4, latitude=2041, longitude=4320)
	MFWAMSeaPeriodField      = "VTM10"     // 海浪周期 (time=4, latitude=2041, longitude=4320)
	MFWAMSwellHeightField    = "VHM0_SW1"  // 涌浪高度 (time=4, latitude=2041, longitude=4320)
	MFWAMSwellDirectionField = "VMDR_SW1"  // 涌浪方向 (time=4, latitude=2041, longitude=4320)
	MFWAMSwellPeriodField    = "VTM01_SW1" // 涌浪周期 (time=4, latitude=2041, longitude=4320)
	MFWAMWindHeightField     = "VHM0_WW"   // 风浪高度 (time=4, latitude=2041, longitude=4320)
	MFWAMWindDirectionField  = "VMDR_WW"   // 风浪方向 (time=4, latitude=2041, longitude=4320)
	MFWAMWindPeriodField     = "VTM01_WW"  // 风浪周期 (time=4, latitude=2041, longitude=4320)
	MFWAMFillValueAttribute  = "_FillValue"
	MFWAMAddOffsetAttribute  = "add_offset"
	MFWAMScaleAttribute      = "scale_factor"
	MFWAMLatitudeCount       = 2041
	MFWAMLongitudeCount      = 4320
	MFWAMTimeCount           = 4
	MFWAMStep                = float32(1. / 12.)
)

type MFWAM struct {
	info                    *NCFile
	group                   api.Group
	latitudeList            []float64
	longitudeList           []float64
	seaDirectionList        [][][]int16
	seaDirectionFillValue   int16
	seaDirectionAddOffset   float32
	seaDirectionScale       float32
	seaHeightList           [][][]int16
	seaHeightFillValue      int16
	seaHeightAddOffset      float32
	seaHeightScale          float32
	seaPeriodList           [][][]int16
	seaPeriodFillValue      int16
	seaPeriodAddOffset      float32
	seaPeriodScale          float32
	swellDirectionList      [][][]int16
	swellDirectionFillValue int16
	swellDirectionAddOffset float32
	swellDirectionScale     float32
	swellHeightList         [][][]int16
	swellHeightFillValue    int16
	swellHeightAddOffset    float32
	swellHeightScale        float32
	swellPeriodList         [][][]int16
	swellPeriodFillValue    int16
	swellPeriodAddOffset    float32
	swellPeriodScale        float32
	windDirectionList       [][][]int16
	windDirectionFillValue  int16
	windDirectionAddOffset  float32
	windDirectionScale      float32
	windHeightList          [][][]int16
	windHeightFillValue     int16
	windHeightAddOffset     float32
	windHeightScale         float32
	windPeriodList          [][][]int16
	windPeriodFillValue     int16
	windPeriodAddOffset     float32
	windPeriodScale         float32
}

func NewMFWAM(info *NCFile) (*MFWAM, error) {
	if _, err := os.Stat(info.InputPath); err != nil {
		return nil, fmt.Errorf("mfwam input file: %s not exists", info.InputPath)
	}

	if _, err := os.Stat(info.OutputPath); err == nil {
		return nil, fmt.Errorf("mfwam output file: %s already exists", info.OutputPath)
	}

	if _, err := os.Stat(info.CompressionPath); err == nil {
		return nil, fmt.Errorf("mfwam compression file: %s already exists", info.CompressionPath)
	}

	if err := os.MkdirAll(filepath.Dir(info.OutputPath), os.FileMode(0755)); err != nil {
		return nil, fmt.Errorf("create mfwam output dir: %s failed: %v", filepath.Dir(info.OutputPath), err)
	}

	group, err := netcdf.Open(info.InputPath)
	if err != nil {
		return nil, fmt.Errorf("open mfwam netcdf file: %s failed: %v", info.InputPath, err)
	}

	return &MFWAM{
		info:  info,
		group: group,
	}, nil
}

// latitude: -80~90
// longitude: -180~180
// 1 / 12
func (nc *MFWAM) Analysis() error {
	latitudeVari, err := nc.group.GetVariable(MFWAMLatitudeField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMLatitudeField, err)
	}
	nc.latitudeList = latitudeVari.Values.([]float64)

	longitudeVari, err := nc.group.GetVariable(MFWAMLongitudeField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMLongitudeField, err)
	}
	nc.longitudeList = longitudeVari.Values.([]float64)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	seaHeightVari, err := nc.group.GetVariable(MFWAMSeaHeightField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMSeaHeightField, err)
	}
	fillvalue, ok := seaHeightVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaHeightField, MFWAMFillValueAttribute, err)
	}
	offset, ok := seaHeightVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaHeightField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok := seaHeightVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaHeightField, MFWAMScaleAttribute, err)
	}
	nc.seaHeightList = seaHeightVari.Values.([][][]int16)
	nc.seaHeightFillValue = fillvalue.(int16)
	nc.seaHeightAddOffset = offset.(float32)
	nc.seaHeightScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	seaDirectionVari, err := nc.group.GetVariable(MFWAMSeaDirectionField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMSeaDirectionField, err)
	}
	fillvalue, ok = seaDirectionVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaDirectionField, MFWAMFillValueAttribute, err)
	}
	offset, ok = seaDirectionVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaDirectionField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = seaDirectionVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaDirectionField, MFWAMScaleAttribute, err)
	}
	nc.seaDirectionList = seaDirectionVari.Values.([][][]int16)
	nc.seaDirectionFillValue = fillvalue.(int16)
	nc.seaDirectionAddOffset = offset.(float32)
	nc.seaDirectionScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	seaPeriodVari, err := nc.group.GetVariable(MFWAMSeaPeriodField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMSeaPeriodField, err)
	}
	fillvalue, ok = seaPeriodVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaPeriodField, MFWAMFillValueAttribute, err)
	}
	offset, ok = seaPeriodVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaPeriodField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = seaPeriodVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSeaPeriodField, MFWAMScaleAttribute, err)
	}
	nc.seaPeriodList = seaPeriodVari.Values.([][][]int16)
	nc.seaPeriodFillValue = fillvalue.(int16)
	nc.seaPeriodAddOffset = offset.(float32)
	nc.seaPeriodScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	swellHeightVari, err := nc.group.GetVariable(MFWAMSwellHeightField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMSwellHeightField, err)
	}
	fillvalue, ok = swellHeightVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellHeightField, MFWAMFillValueAttribute, err)
	}
	offset, ok = swellHeightVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellHeightField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = swellHeightVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellHeightField, MFWAMScaleAttribute, err)
	}
	nc.swellHeightList = swellHeightVari.Values.([][][]int16)
	nc.swellHeightFillValue = fillvalue.(int16)
	nc.swellHeightAddOffset = offset.(float32)
	nc.swellHeightScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	swellDirectionVari, err := nc.group.GetVariable(MFWAMSwellDirectionField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMSwellDirectionField, err)
	}
	fillvalue, ok = swellDirectionVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellDirectionField, MFWAMFillValueAttribute, err)
	}
	offset, ok = swellDirectionVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellDirectionField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = swellDirectionVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellDirectionField, MFWAMScaleAttribute, err)
	}
	nc.swellDirectionList = swellDirectionVari.Values.([][][]int16)
	nc.swellDirectionFillValue = fillvalue.(int16)
	nc.swellDirectionAddOffset = offset.(float32)
	nc.swellDirectionScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	swellPeriodVari, err := nc.group.GetVariable(MFWAMSwellPeriodField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMSwellPeriodField, err)
	}
	fillvalue, ok = swellPeriodVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellPeriodField, MFWAMFillValueAttribute, err)
	}
	offset, ok = swellPeriodVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellPeriodField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = swellPeriodVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMSwellPeriodField, MFWAMScaleAttribute, err)
	}
	nc.swellPeriodList = swellPeriodVari.Values.([][][]int16)
	nc.swellPeriodFillValue = fillvalue.(int16)
	nc.swellPeriodAddOffset = offset.(float32)
	nc.swellPeriodScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	windHeightVari, err := nc.group.GetVariable(MFWAMWindHeightField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMWindHeightField, err)
	}
	fillvalue, ok = windHeightVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindHeightField, MFWAMFillValueAttribute, err)
	}
	offset, ok = windHeightVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindHeightField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = windHeightVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindHeightField, MFWAMScaleAttribute, err)
	}
	nc.windHeightList = windHeightVari.Values.([][][]int16)
	nc.windHeightFillValue = fillvalue.(int16)
	nc.windHeightAddOffset = offset.(float32)
	nc.windHeightScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	windDirectionVari, err := nc.group.GetVariable(MFWAMWindDirectionField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMWindDirectionField, err)
	}
	fillvalue, ok = windDirectionVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindDirectionField, MFWAMFillValueAttribute, err)
	}
	offset, ok = windDirectionVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindDirectionField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = windDirectionVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindDirectionField, MFWAMScaleAttribute, err)
	}
	nc.windDirectionList = windDirectionVari.Values.([][][]int16)
	nc.windDirectionFillValue = fillvalue.(int16)
	nc.windDirectionAddOffset = offset.(float32)
	nc.windDirectionScale = scale.(float32)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	windPeriodVari, err := nc.group.GetVariable(MFWAMWindPeriodField)
	if err != nil {
		return fmt.Errorf("解析 mfwam 变量: %s 失败: %v", MFWAMWindPeriodField, err)
	}
	fillvalue, ok = windPeriodVari.Attributes.Get(MFWAMFillValueAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindPeriodField, MFWAMFillValueAttribute, err)
	}
	offset, ok = windPeriodVari.Attributes.Get(MFWAMAddOffsetAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindPeriodField, MFWAMAddOffsetAttribute, err)
	}
	scale, ok = windPeriodVari.Attributes.Get(MFWAMScaleAttribute)
	if !ok {
		return fmt.Errorf("解析 mfwam 变量: %s 属性: %s 失败: %v", MFWAMWindPeriodField, MFWAMScaleAttribute, err)
	}
	nc.windPeriodList = windPeriodVari.Values.([][][]int16)
	nc.windPeriodFillValue = fillvalue.(int16)
	nc.windPeriodAddOffset = offset.(float32)
	nc.windPeriodScale = scale.(float32)

	return nil
}

func (nc *MFWAM) GenerateCSV() error {
	file, err := os.OpenFile(nc.info.OutputPath, os.O_CREATE|os.O_RDWR, os.FileMode(0664))
	if err != nil {
		return fmt.Errorf("output file: %s create failed: %v", nc.info.OutputPath, err)
	}
	defer file.Close()

	buf := bufio.NewWriter(file)
	buf.WriteString("lat,lon,dateTime,seaWaveHeight,seaWaveDirection,seaWavePeriod,swellWaveHeight,swellWaveDirection,swellWavePeriod,windWaveHeight,windWaveDirection,windWavePeriod\n")
	for timeIndex := range MFWAMTimeCount {
		for latIndex := 0; latIndex < MFWAMLatitudeCount; latIndex += 3 {
			for lonIndex := 0; lonIndex < MFWAMLongitudeCount; lonIndex += 3 {
				buf.WriteString(
					fmt.Sprintf(
						"%.3f,%.3f,%s",
						nc.latitudeList[latIndex],
						nc.longitudeList[lonIndex],
						nc.info.DateTime.Add(time.Hour*time.Duration(timeIndex*3)).Format(time.DateTime),
					),
				)

				// 显浪
				seaHeight := nc.seaHeightList[timeIndex][latIndex][lonIndex]
				if seaHeight == nc.seaHeightFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(seaHeight, nc.seaHeightScale, nc.seaHeightAddOffset)))
				}

				seaDirection := nc.seaDirectionList[timeIndex][latIndex][lonIndex]
				if seaDirection == nc.seaDirectionFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(seaDirection, nc.seaDirectionScale, nc.seaDirectionAddOffset)))
				}

				seaPeriod := nc.seaPeriodList[timeIndex][latIndex][lonIndex]
				if seaPeriod == nc.seaPeriodFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(seaPeriod, nc.seaPeriodScale, nc.seaPeriodAddOffset)))
				}

				// 涌浪
				swellHeight := nc.swellHeightList[timeIndex][latIndex][lonIndex]
				if swellHeight == nc.swellHeightFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(swellHeight, nc.swellHeightScale, nc.swellHeightAddOffset)))
				}

				swellDirection := nc.swellDirectionList[timeIndex][latIndex][lonIndex]
				if swellDirection == nc.swellDirectionFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(swellDirection, nc.swellDirectionScale, nc.swellDirectionAddOffset)))
				}

				swellPeriod := nc.swellPeriodList[timeIndex][latIndex][lonIndex]
				if swellPeriod == nc.swellPeriodFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(swellPeriod, nc.swellPeriodScale, nc.swellPeriodAddOffset)))
				}

				// 风浪
				windHeight := nc.windHeightList[timeIndex][latIndex][lonIndex]
				if windHeight == nc.windHeightFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(windHeight, nc.windHeightScale, nc.windHeightAddOffset)))
				}

				windDirection := nc.windDirectionList[timeIndex][latIndex][lonIndex]
				if windDirection == nc.windDirectionFillValue {
					buf.WriteString(",NaN")
				} else {
					buf.WriteString(fmt.Sprintf(",%f", convertInt16ToFloat32(windDirection, nc.windDirectionScale, nc.windDirectionAddOffset)))
				}

				windPeriod := nc.windPeriodList[timeIndex][latIndex][lonIndex]
				if windPeriod == nc.windPeriodFillValue {
					buf.WriteString(",NaN\n")
				} else {
					buf.WriteString(fmt.Sprintf(",%f\n", convertInt16ToFloat32(windPeriod, nc.windPeriodScale, nc.windPeriodAddOffset)))
				}
			}
		}

		buf.Flush()
	}

	buf.Flush()
	return zipFile(nc.info.OutputPath, nc.info.CompressionPath)
}

func (nc *MFWAM) Close() {
	nc.group.Close()
}
