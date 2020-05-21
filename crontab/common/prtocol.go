package common

import (
	"context"
	"distributedSchedulingTask/crontab/lib"
	"encoding/json"
	"fmt"
	"github.com/gorhill/cronexpr"
	Log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

// Job:任务结构体
type Job struct {
	Name     string `json:"name"`      // 任务名
	Command  string `json:"command"`   // shell命令
	CronExpr string `json:"cron_expr"` // Cron 表达式
	JobType  int    `json:"job_type" ` //任务类型
}

// JobSchedulePlan:任务计划结构体
type JobSchedulePlan struct {
	Job               *Job                       //任务信息
	Expr              *cronexpr.Expression       //任务表达式
	NextTime          time.Time                  //下一次执行时间
	jobExecutingTable map[string]*JobExecuteInfo //任务执行散列表
}

// JobExecuteInfo:任务执行信息
type JobExecuteInfo struct {
	Job        *Job               //任务信息
	JobType    int                //任务类型
	PlanTime   time.Time          //理论调度时间
	RealTime   time.Time          //实际调度时间
	CancelCtx  context.Context    //任务command的上下文
	CancelFunc context.CancelFunc //取消command执行的cancel函数
}

// JobExecuteResult:执行任务日志结构返回值体
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo
	Output      []byte //脚本输出
	Err         error
	StartTime   time.Time
	EndTime     time.Time
}

// JobLog:日志结构
type JobLog struct {
	JobName       string `bson:"job_name" json:"job_name"`             //任务名字
	Command       string `bson:"command" json:"command"`               //脚本命令
	Err           string `bson:"err" json:"err"`                       //错误原因
	Output        string `bson:"output" json:"output"`                 //脚本输出
	PlanTime      int64  `bson:"plan_time" json:"plan_time"`           //计划开始时间
	SchedulerTime int64  `bson:"scheduler_time" json:"scheduler_time"` //实际调度时间
	StartTime     int64  `bson:"start_time" json:"start_time"`         //任务开始时间
	EndTime       int64  `bson:"end_time" json:"end_time"`             //任务结束时间
}

// LogBatch:日志批次
type LogBatch struct {
	Logs []interface{} //多条日志
}

// JobLogFilter:任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"job_name"`
}

// SortLogByStartTime:任务日志排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"` // {startTime: -1}
}

// DelJob:删除任务
type DelJob struct {
	Name string `json:"name"` // 任务名
}

// Response:view视图处理返回体
type Response struct {
	Error int         `json:"error"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// BaseJobMgr:任务处理接口
type BaseJobMgr interface {
	SaveJob(*Job) (*Job, error)
	DeleteJob(*DelJob) (*Job, error)
	ListJob() ([]*Job, error)
	KillJob(string) error
	StartJob(string) error
}

// JobEvent:任务时间结构体
type JobEvent struct {
	EventType int
	Job       *Job
}

// String:格式化输出
func (job *JobEvent) String() string {
	return fmt.Sprintf("EvnetType %v ,jobName %v", job.EventType, job.Job.Name)
}

// ExtractJobName:解析任务名
func ExtractJobName(JobKey string) string {
	return strings.TrimPrefix(JobKey, JOB_SAVE_DIR)
}

// ExtractKillerName:解析kill任务名
func ExtractKillerName(KillerKey string) string {
	return strings.TrimPrefix(KillerKey, JOB_KILL_DIR)
}

// UnJsonfull:反序列化任务
func UnJsonfull(value []byte) (*Job, error) {
	job := &Job{}
	err := json.Unmarshal(value, job)
	if err != nil {
		return nil, err
	}
	return job, err
}

// BuildJobEvent:转化任务为任务事件
func BuildJobEvent(eventType int, job *Job) *JobEvent {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// BuildJobToTemporarySchedulePlan:将临时任务转化为调度任务
func BuildJobToTemporarySchedulePlan(job *Job) *JobSchedulePlan {
	jobSchedulePlan := &JobSchedulePlan{
		Job:      job,
		Expr:     nil,
		NextTime: time.Now(),
	}
	return jobSchedulePlan
}

// BuildJobToSchedulePlan:解析任务表达式转化为调度任务
func BuildJobToSchedulePlan(job *Job) (*JobSchedulePlan, error) {
	parse, err := cronexpr.Parse(job.CronExpr)
	if err != nil {
		Log.Errorf("解析表达式异常！错误：%s", err)
		return nil, err
	}
	jobSchedulePlan := &JobSchedulePlan{
		Job:      job,
		Expr:     parse,
		NextTime: parse.Next(time.Now()),
	}
	return jobSchedulePlan, nil
}

// BuildJobExecuteInfo:将调度任务转化为执行任务
func BuildJobExecuteInfo(plan *JobSchedulePlan) *JobExecuteInfo {
	ctx, cancelFunc := context.WithCancel(context.TODO())
	jobExecuteinfo := &JobExecuteInfo{
		Job:        plan.Job,
		JobType:    JOB_TYPE_CRON,
		PlanTime:   plan.NextTime,
		RealTime:   time.Now(),
		CancelCtx:  ctx,
		CancelFunc: cancelFunc,
	}

	return jobExecuteinfo
}

// ExtractWorkerIP:提取worker的IP
func ExtractWorkerIP(regKey string) string {
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}

// ExtractJobDir:返回任务路径以及任务名
func ExtractJobDir(key []byte) (string, string) {
	eventKvKeyList := strings.Split(lib.Bytes2str(key), "/") //提取任务路径
	eventKvKey := strings.Join(eventKvKeyList[0:len(eventKvKeyList)-1], "/") + "/"
	return eventKvKey, eventKvKeyList[len(eventKvKeyList)-1] //返回任务路径和任务名
}
