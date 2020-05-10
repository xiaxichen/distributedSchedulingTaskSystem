package master

import (
	"context"
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/master/config"
	"github.com/coreos/etcd/clientv3"
	"time"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

// ListWorkers:获取在线worker列表
func (workerMgr *WorkerMgr) ListWorkers() ([]string, error) {
	// 初始化数组
	workerArr := make([]string, 0)
	// 获取目录下所有Kv
	if getResp, err := workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return workerArr, err
	} else {
		// 解析每个节点的IP
		for _, kv := range getResp.Kvs {
			// kv.Key : /cron/workers/192.168.2.1
			workerIP := common.ExtractWorkerIP(string(kv.Key))
			workerArr = append(workerArr, workerIP)
		}
	}
	return workerArr, nil
}

// InitWorkerMgr:初始化worker
func InitWorkerMgr(conf *config.Config) error {
	// 初始化配置
	configs := clientv3.Config{
		Endpoints:   conf.EtcdEndPoints,                                      // 集群地址
		DialTimeout: time.Duration(conf.EtcdDtailTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	client, err := clientv3.New(configs)
	if err != nil {
		return err
	}

	// 得到KV和Lease的API子集
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return nil
}
