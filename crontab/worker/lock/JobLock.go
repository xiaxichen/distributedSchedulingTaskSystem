package lock

import (
	"context"
	"distributedSchedulingTask/crontab/common"
	"github.com/coreos/etcd/clientv3"
)

// JobLock:任务锁
type JobLock struct {
	kv         clientv3.KV
	lease      clientv3.Lease
	jobName    string
	cancelFunc context.CancelFunc
	leaseId    clientv3.LeaseID
	isLock     bool
}

// InitJobLock:初始化任务锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	return &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
}

// TryLock:加锁
func (joblock *JobLock) TryLock() error {
	grant, err := joblock.lease.Grant(context.TODO(), 5)
	if err != nil {
		return err
	}
	leaseId := grant.ID
	cancelCtx, cancelFunc := context.WithCancel(context.TODO())
	//defer func(can context.CancelFunc, lock *JobLock, id clientv3.LeaseID) error {
	//	errorMsg := recover()
	//	if errorMsg != nil {
	//		can()
	//		lock.lease.Revoke(context.TODO(), id)
	//		return err
	//	} else {
	//		return err
	//	}
	//}(cancelFunc, joblock, leaseId)
	defer func() error {
		if err != nil {
			cancelFunc()
			joblock.lease.Revoke(context.TODO(), leaseId)
			return err
		} else {
			return err
		}
	}()
	keepResp, err := joblock.lease.KeepAlive(cancelCtx, leaseId)
	if err != nil {
		return err
	}
	go func(respChan <-chan *clientv3.LeaseKeepAliveResponse) {
		for {
			select {
			case keepResp := <-respChan:
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}(keepResp)
	// 创建事务
	txn := joblock.kv.Txn(context.TODO())
	// 锁路径
	lockKey := common.JOB_LOCK_DIR + joblock.jobName
	// 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).Else(clientv3.OpGet(lockKey))
	commit, err := txn.Commit()
	if err != nil {
		return err
	}
	if !commit.Succeeded {
		return common.ERR_LOCK_ALREADY_REQUIRED
	}
	joblock.leaseId = leaseId
	joblock.cancelFunc = cancelFunc
	joblock.isLock = true
	return nil
}

// Unlock:解锁
func (joblock *JobLock) Unlock() {
	if joblock.isLock == true {
		joblock.cancelFunc()
		joblock.lease.Revoke(context.TODO(), joblock.leaseId)
	}
	return
}
