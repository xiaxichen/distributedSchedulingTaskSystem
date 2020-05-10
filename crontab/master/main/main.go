package main

import (
	"distributedSchedulingTask/crontab/master"
	"distributedSchedulingTask/crontab/master/config"
	"distributedSchedulingTask/crontab/master/route"
	"flag"
	"fmt"
	Log "github.com/sirupsen/logrus"
	"os"
	"runtime"
)

var (
	ConfigFilePath string
)

func init() {
	// 设置日志格式为json格式
	//Log.SetFormatter(&Log.JSONFormatter{})

	// 设置将日志输出到标准输出（默认的输出为stderr，标准错误）
	// 日志消息输出可以是任意的io.writer类型
	Log.SetOutput(os.Stdout)

	// 设置日志级别为warn以上
	Log.SetLevel(Log.DebugLevel)
	// 设置行号
	Log.SetReportCaller(true)

	Log.SetFormatter(&Log.TextFormatter{
		FullTimestamp: true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			//filename := path.Base(f.File)
			return fmt.Sprintf("%s", f.Function), ""
		},
	})
}

func initArgs() {
	//master -conifg ./master.json
	flag.StringVar(&ConfigFilePath, "config", "./master.json", "指定master.json配置文件")
	flag.Parse()
}
func intiEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	//初始化线程
	intiEnv()
	//初始化参数
	initArgs()
	if err := config.InitConfig(ConfigFilePath); err != nil {
		panic(err)
	}
	err := master.InitLogMgr(config.G_Config)
	if err != nil {
		panic(err)
	}
	// 任务管理器 ETCD
	if err := master.InitJobMgr(config.G_Config); err != nil {
		panic(err)
	}
	if err = master.InitWorkerMgr(config.G_Config); err != nil {
		panic(err)
	}
	//初始化服务
	engine := route.InitRoute(config.G_Config)
	Log.Fatal(engine.Run(fmt.Sprintf(":%d", config.G_Config.ApiPort)))
}
