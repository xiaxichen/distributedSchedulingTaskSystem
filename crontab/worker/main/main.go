package main

import (
	"distributedSchedulingTask/crontab/worker/config"
	"distributedSchedulingTask/crontab/worker/logSink"
	"distributedSchedulingTask/crontab/worker/register"
	"distributedSchedulingTask/crontab/worker/scheduler"
	"flag"
	"fmt"
	Log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"time"
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
	flag.StringVar(&ConfigFilePath, "config", "./worker.json", "指定worker.json配置文件")
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
	//启动日志协程
	if err := logSink.InitLogSink(config.G_Config); err != nil {
		panic(err)
	}
	// 启动日志
	go logSink.G_logSink.Loop()
	// 启动执行器
	if err := scheduler.InitExector(); err != nil {
		panic(err)
	}
	// 启动调度器
	if err := scheduler.InitScheduler(); err != nil {
		panic(err)
	}
	// 任务管理器 ETCD
	if err := scheduler.InitJobMgr(config.G_Config); err != nil {
		panic(err)
	}
	// 注册任务
	if err := register.InitRegister(config.G_Config); err != nil {
		panic(err)
	}
	// 启动观察管理
	if err := scheduler.G_jobMgr.WatchJobs(); err != nil {
		panic(err)
	}
	scheduler.G_jobMgr.WatchKiller()
	for {
		time.Sleep(1)
	}
	//初始化服务
	//engine := route.InitRoute(config.G_Config)
	//Log.Fatal(engine.Run(fmt.Sprintf(":%d", config.G_Config.ApiPort)))
}
