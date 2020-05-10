package scheduler

import (
	"distributedSchedulingTask/crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

var (
	G_executor *Executor
)

type Executor struct {
}

// ExecuteJob:执行器执行函数
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		result := &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      nil,
			Err:         nil,
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
		lock := G_jobMgr.CreateJobLock(info.Job.Name)
		err := lock.TryLock()
		defer lock.Unlock()

		if err != nil {
			result.Err = err
			result.EndTime = time.Now()
			result.StartTime = time.Now()
		} else {
			result.StartTime = time.Now()
			//执行shell命令
			cmd := exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)
			//执行并捕获输出
			output, err := cmd.CombinedOutput()
			result.Output = output
			result.EndTime = time.Now()
			result.Err = err
		}
		G_Scheduler.PushJobResult(result)
		return
	}()
}

// InitExector:初始化执行器
func InitExector() error {
	G_executor = &Executor{}
	return nil
}
