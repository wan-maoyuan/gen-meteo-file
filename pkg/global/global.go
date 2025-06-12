package global

import "fmt"

const (
	// 日志配置
	DefaultLevel      = "info"
	DefaultMaxLogSize = 20
	DefaultMaxLogAge  = 10
	DefaultMaxBackups = 5
)

func ShowProgramInfo() {
	fmt.Printf(`


                                   _                __ _ _      
  __ _  ___ _ __    _ __ ___   ___| |_ ___  ___    / _(_) | ___ 
 / _  |/ _ \ '_ \  | '_   _ \ / _ \ __/ _ \/ _ \  | |_| | |/ _ \
| (_| |  __/ | | | | | | | | |  __/ ||  __/ (_) | |  _| | |  __/
 \__, |\___|_| |_| |_| |_| |_|\___|\__\___|\___/  |_| |_|_|\___|
 |___/                                                          
                                                                                 

   
	ProgramName: gen-meteo-file
	Org: nav_green
	OrgUrl: http://www.navgreen.cn


`)
}
