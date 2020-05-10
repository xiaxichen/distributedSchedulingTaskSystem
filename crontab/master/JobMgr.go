package master

import (
	"context"
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/lib"
	"distributedSchedulingTask/crontab/master/config"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"time"
)

var (
	G_jobMgr *JobMgr
)

// JobMgr:任务管理结构体
type JobMgr struct {
	client       *clientv3.Client
	kv           clientv3.KV
	lease        clientv3.Lease
	Timeout      int
	JobSaveDir   string
	JobKillerDir string
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
	}
	return err
}
