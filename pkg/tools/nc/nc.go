package nc

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"time"
)

type NCFile struct {
	DateTime        time.Time
	InputPath       string
	OutputPath      string
	CompressionPath string
}

// source: https://help.marine.copernicus.eu/en/articles/5470092-how-to-use-add_offset-and-scale_factor-to-calculate-real-values-of-a-variable
// Real_Value = (Display_Value X scale_factor) + add_offset
func convertInt16ToFloat32(real int16, scaleFactor, addOffset float32) float32 {
	return float32(real)*scaleFactor + addOffset
}

// src: /data1/cosco-generate-files/2025/06/2025-06-13/ec_2025061315.csv
// dst: /data1/cosco-generate-files/2025/06/2025-06-13/ec_2025061315.zip
func zipFile(src, dst string) error {
	defer os.Remove(src)

	// 创建目标 zip 文件
	zipFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create zip file: %s failed: %v", dst, err)
	}
	defer zipFile.Close()

	// 创建 zip writer
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// 打开源文件
	fileToZip, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file: %s failed: %v", src, err)
	}
	defer fileToZip.Close()

	// 获取源文件信息
	info, err := fileToZip.Stat()
	if err != nil {
		return fmt.Errorf("get file info failed: %v", err)
	}

	// 创建 zip 文件头
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("create zip header failed: %v", err)
	}

	// 设置压缩方法
	header.Method = zip.Deflate

	// 创建 zip 文件写入器
	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("create zip writer failed: %v", err)
	}

	// 将源文件内容复制到 zip 文件中
	_, err = io.Copy(writer, fileToZip)
	if err != nil {
		return fmt.Errorf("copy file content failed: %v", err)
	}

	return nil
}
