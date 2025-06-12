package nc

import "time"

type NCFile struct {
	DateTime   time.Time
	InputPath  string
	OutputPath string
}

// source: https://help.marine.copernicus.eu/en/articles/5470092-how-to-use-add_offset-and-scale_factor-to-calculate-real-values-of-a-variable
// Real_Value = (Display_Value X scale_factor) + add_offset
func convertInt16ToFloat32(real int16, scaleFactor, addOffset float32) float32 {
	return float32(real)*scaleFactor + addOffset
}
