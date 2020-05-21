package scheduler

import (
	"context"
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/lib"
	"distributedSchedulingTask/crontab/worker/config"
	"distributedSchedulingTask/crontab/worker/lock"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	Log "github.com/sirupsen/logrus"
	"time"
)

var (
	G_jobMgr *JobMgr
)

// JobMgr:任务调度结构体:
type JobMgr struct {
	client       *clientv3.Client // etcd客户端会话
	kv           clientv3.KV
	lease        clientv3.Lease   // etcd租约
	watcher      clientv3.Watcher // etcd观察对象
	Timeout      int              // 超时时间
	JobSaveDir   string           // 任务etcd地址
	JobKillerDir string           // 强杀任务etcd地址
}

// WokerJobMgr:任务调度结接口
type WokerJobMgr interface {
	common.BaseJobMgr
	WatchJobs() error
}

// switchJobEvent:选择任务事件！
func (jobMgr *JobMgr) switchJobEvent(eventKvKey string, jobName string, event *clientv3.Event) (*common.JobEvent, error) {
	switch eventKvKey {
	case common.JOB_SAVE_DIR:
		job, err := common.UnJsonfull(event.Kv.Value)
		if err != nil {
			//todo:将失败序列化写入日志
			return nil, err
		}
		jobEvent := common.BuildJobEvent(common.JOB_EVEN_SAVE, job)
		Log.Info(event)
		return jobEvent, nil
	case common.JOB_START_DIR:
		job := &common.Job{Name: jobName, JobType: common.JOB_TYPE_TEMPORARY}
		jobEvent := common.BuildJobEvent(common.JOB_EVEN_START, job)
		Log.Info(event)
		return jobEvent, nil
	default:
		//todo:未识别事件
		Log.Warn("未识别事件：", event)
		return nil, common.ERR_NOT_FOUND_EVENT
	}
}

// WatchJobs:任务观察调度器
func (jobMgr *JobMgr) WatchJobs() error {
	timeoutCtx, _ := context.WithTimeout(context.TODO(), time.Duration(int(time.Millisecond)*jobMgr.Timeout))
	getResp, err := jobMgr.kv.Get(timeoutCtx, common.JOB_DIR, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range getResp.Kvs {
		var event *common.JobEvent
		job, err := common.UnJsonfull(kv.Value)
		if err == nil {
			if job.JobType == common.JOB_TYPE_CRON {
				event = common.BuildJobEvent(common.JOB_EVEN_SAVE, job)
			} else if job.JobType == common.JOB_TYPE_TEMPORARY {
				event = common.BuildJobEvent(common.JOB_EVEN_START, job)
			} else {
				Log.Errorf("位置任务类型！%s", common.ERR_NOT_FOUND_JOB_TYPE)
				continue
			}
			Log.Info(event)
			G_Scheduler.PushJobEvent(event)
		}
	}
	go func() {
		watchStartRevision := getResp.Header.Revision + 1 //获取观察任务起始版本
		watchChan := jobMgr.watcher.Watch(context.TODO(), common.JOB_DIR, clientv3.WithRev(watchStartRevision),
			clientv3.WithPrefix())
		var jobEvent *common.JobEvent //定义任务事件
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				jobEvent = nil
				switch event.Type {
				case mvccpb.PUT: //新建;修改事件
					jobDir, jobName := common.ExtractJobDir(event.Kv.Key)
					jobEvent, err = jobMgr.switchJobEvent(jobDir, jobName, event)
					if err != nil {
						//todo:异常日志记录
						Log.Errorf("选择事件错误！错误：%s", err)
						continue
					}
				case mvccpb.DELETE: //删除任务事件
					jobName := common.ExtractJobName(lib.Bytes2str(event.Kv.Key))
					job := &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVEN_DELETE, job)
					Log.Info(event)
				}
				//提交任务事件到调度器中
				G_Scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return nil
}

// WatchKiller:监听强杀任务通知
func (jobMgr *JobMgr) WatchKiller() {
	go func() {
		watchChan := jobMgr.watcher.Watch(context.TODO(), common.JOB_KILL_DIR, clientv3.WithPrefix(),
			clientv3.WithPrefix())
		var jobEvent *common.JobEvent
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				jobEvent = nil
				switch event.Type {
				case mvccpb.PUT: //杀死任务事件
					jobName := common.ExtractKillerName(lib.Bytes2str(event.Kv.Key))
					job := &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVEN_KILL, job)
					G_Scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: //killer自动过期被自动删除

				}
			}
		}
	}()
}

// SaveJob:保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (*common.Job, error) {
	//把任务保存到/cron/jobs/任务名 —>json
	jobKey := jobMgr.JobSaveDir + job.Name
	marshal, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	timeoutCtx, _ := context.WithTimeout(context.TODO(), time.Duration(int(time.Millisecond)*jobMgr.Timeout))
	putResp, err := jobMgr.kv.Put(timeoutCtx, jobKey, lib.Bytes2str(marshal), clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}
	oldJobObj := &common.Job{}
	if putResp.PrevKv != nil {
		if err := json.Unmarshal(putResp.PrevKv.Value, oldJobObj); err != nil {
			return nil, err
		}
	}
	return oldJobObj, nil
}

// DeleteJob:删除任务
func (jobMgr *JobMgr) DeleteJob(job *common.DelJob) (*common.Job, error) {
	//把任务保存到/cron/jobs/任务名 —>json
	jobKey := jobMgr.JobSaveDir + job.Name
	timeoutCtx, _ := context.WithTimeout(context.TODO(), time.Duration(int(time.Millisecond)*jobMgr.Timeout))
	DelResp, err := jobMgr.kv.Delete(timeoutCtx, jobKey, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}
	oldJobObj := &common.Job{}
	if len(DelResp.PrevKvs) != 0 {
		if err := json.Unmarshal(DelResp.PrevKvs[0].Value, oldJobObj); err != nil {
			return nil, err
		}
	}
	return oldJobObj, nil
}

// ListJob:任务列表
func (jobMgr *JobMgr) ListJob() ([]*common.Job, error) {
	//把任务保存到/cron/jobs/任务名 —>json
	jobKey := jobMgr.JobSaveDir
	timeoutCtx, _ := context.WithTimeout(context.TODO(), time.Duration(int(time.Millisecond)*jobMgr.Timeout))
	GetResp, err := jobMgr.kv.Get(timeoutCtx, jobKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	oldJobObjs := make([]*common.Job, len(GetResp.Kvs))
	if len(GetResp.Kvs) != 0 {
		for i, kv := range GetResp.Kvs {
			oldJobObj := &common.Job{}
			if err := json.Unmarshal(kv.Value, oldJobObj); err != nil {
				continue
			} else {
				oldJobObjs[i] = oldJobObj
			}
		}

	}
	return oldJobObjs, nil
}

// KillJob:杀死任务
func (jobMgr *JobMgr) KillJob(name string) error {
	KillKey := jobMgr.JobKillerDir + name
	timeoutCtx, _ := context.WithTimeout(context.TODO(), time.Duration(int(time.Millisecond)*jobMgr.Timeout))
	LeaseGrant, err := jobMgr.lease.Grant(timeoutCtx, 1)
	if err != nil {
		return err
	}
	LeaseId := LeaseGrant.ID
	timeoutCtx, _ = context.WithTimeout(context.TODO(), time.Duration(int(time.Millisecond)*jobMgr.Timeout))
	_, err = jobMgr.kv.Put(timeoutCtx, KillKey, "", clientv3.WithLease(LeaseId))
	if err != nil {
		return err
	}
	return nil
}

// CreateJobLock:创建任务锁
func (jobMgr *JobMgr) CreateJobLock(JobName string) *lock.JobLock {
	return lock.InitJobLock(JobName, jobMgr.kv, jobMgr.lease)
}

// InitJobMgr:初始化任务
func InitJobMgr(con *config.Config) error {
	c := clientv3.Config{
		Endpoints:   con.EtcdEndPoints,
		DialTimeout: time.Duration(con.EtcdDtailTimeout) * time.Microsecond,
	}
	client, err := clientv3.New(c)
	if err != nil {
		return err
	}
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	G_jobMgr = &JobMgr{
		client:       client,
		kv:           kv,
		lease:        lease,
		Timeout:      con.EtcdDtailTimeout,
		JobSaveDir:   common.JOB_SAVE_DIR,
		JobKillerDir: common.JOB_KILL_DIR,
		watcher:      clientv3.NewWatcher(client),
	}
	return err
}
