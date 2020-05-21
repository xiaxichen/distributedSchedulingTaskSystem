package scheduler

import (
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/lib"
	"distributedSchedulingTask/crontab/worker/logSink"
	Log "github.com/sirupsen/logrus"
	"time"
)

// Scheduler:调度任务体
type Scheduler struct {
	jobEventChan      chan *common.JobEvent
	jobPlanTable      map[string]*common.JobSchedulePlan //任务队列
	jobExecutingTable map[string]*common.JobExecuteInfo  //执行队列
	jobResultChan     chan *common.JobExecuteResult      //结果队列
}

var (
	G_Scheduler *Scheduler
)

// handlerJobEvent:处理任务理事件
func (scheduler *Scheduler) handlerJobEvent(jobEvent *common.JobEvent) error {
	switch jobEvent.EventType {
	case common.JOB_EVEN_START:
		// 执行临时任务
		if _, isRun := scheduler.jobExecutingTable[jobEvent.Job.Name]; isRun {
			return common.ERR_JOB_IS_RUNNING
		} else {
			plan := common.BuildJobToTemporarySchedulePlan(jobEvent.Job)
			scheduler.TryStartJob(plan)
		}

	case common.JOB_EVEN_SAVE:
		plan, err := common.BuildJobToSchedulePlan(jobEvent.Job)
		if err != nil {
			Log.Errorf("Cron解析失败！错误:%s", err)
			return nil
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = plan

	case common.JOB_EVEN_DELETE:
		if _, PlanExit := scheduler.jobPlanTable[jobEvent.Job.Name]; PlanExit {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}

	case common.JOB_EVEN_KILL:
		//取消command执行 判断任务是否在执行中
		if job, isRun := scheduler.jobExecutingTable[jobEvent.Job.Name]; isRun {
			job.CancelFunc() //触发command杀死子进程
		} else {
			return common.ERR_JOB_NOT_RUNNING
		}

	default:
		return common.ERR_NOT_FOUND_EVENT
	}

	return nil
}

// TryStartJob:尝试开始任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	if jobExecutInfo, jobExecuting := scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		Log.Info("尚未完成调出执行：", jobExecutInfo.Job.Name)
		return
	} else {
		jobExecuteInfo := common.BuildJobExecuteInfo(jobPlan)
		scheduler.jobExecutingTable[jobExecuteInfo.Job.Name] = jobExecuteInfo
		Log.Info("执行任务：", jobExecuteInfo.Job.Name)
		G_executor.ExecuteJob(jobExecuteInfo)
	}
}

// TryScheduler:重新计算任务调度状态
func (scheduler *Scheduler) TryScheduler() time.Duration {
	var nearTime *time.Time
	if len(scheduler.jobPlanTable) == 0 {
		return 1 * time.Second
	}
	now := time.Now()
	for _, jobPlan := range scheduler.jobPlanTable {
		if jobPlan.Job.JobType == common.JOB_TYPE_CRON {
			if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
				scheduler.TryStartJob(jobPlan)            //尝试执行任务
				jobPlan.NextTime = jobPlan.Expr.Next(now) //更新下次任务执行时间
			}
			if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
				nearTime = &jobPlan.NextTime
			}
		}
	}
	// 下次调度时间（最近要执行的调度时间 - 当前时间）
	return (*nearTime).Sub(now)
}

// handlerJobResult:处理任务返回日志
func (scheduler *Scheduler) handlerJobResult(result *common.JobExecuteResult) {
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog := common.JobLog{
			JobName:       result.ExecuteInfo.Job.Name,
			Err:           "",
			Command:       result.ExecuteInfo.Job.Command,
			Output:        lib.Bytes2str(result.Output),
			PlanTime:      result.ExecuteInfo.PlanTime.UnixNano() / 1e6,
			SchedulerTime: result.ExecuteInfo.RealTime.UnixNano() / 1e6,
			StartTime:     result.StartTime.UnixNano() / 1e6,
			EndTime:       result.EndTime.UnixNano() / 1e6,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		}
		//存储到mongo
		logSink.G_logSink.Send(&jobLog)
	}
	Log.Info("任务执行完成：", result.ExecuteInfo.Job.Name, "\n", lib.Bytes2str(result.Output))

}

// Loop:调度主循环
func (scheduler *Scheduler) Loop() {
	schedulerAfter := scheduler.TryScheduler()
	timer := time.NewTimer(schedulerAfter)
	for {
		select {
		case event := <-scheduler.jobEventChan: //监听任务变化事件
			if event.Job.JobType == common.JOB_TYPE_CRON {
				err := scheduler.handlerJobEvent(event)
				if err != nil {
					Log.Errorf("异常事件！任务名：%s,任务事件类型：%s,错误信息：%s", event.Job.Name, event.EventType, err)
				}
			} else if event.Job.JobType == common.JOB_TYPE_TEMPORARY {
				err := scheduler.handlerJobEvent(event)
				if err != nil {
					Log.Errorf("异常事件！任务名：%s,任务事件类型：%s,错误信息：%s", event.Job.Name, event.EventType, err)
				}
			}
		case <-timer.C:
		case JobResult := <-scheduler.jobResultChan: //监听任务结果
			scheduler.handlerJobResult(JobResult)
		}
		schedulerAfter = scheduler.TryScheduler()
		//重置调度间隔
		timer.Reset(schedulerAfter)
		time.Sleep(1000 * time.Millisecond)
	}
}

// PushJobResult:提交任务返回
func (scheduler *Scheduler) PushJobResult(JobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- JobResult
}

// PushJobEvent:提交任务任务事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// InitScheduler:初始化调度
func InitScheduler() error {
	G_Scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan, 1000),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo, 1000),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}
	go G_Scheduler.Loop()
	return nil
}
