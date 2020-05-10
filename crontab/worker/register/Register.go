package register

import (
	"context"
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/worker/config"
	"github.com/coreos/etcd/clientv3"
	"net"
	"time"
)

// 注册节点到etcd： /cron/workers/IP地址
type Register struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease

	localIP string // 本机IP
}

var (
	G_register *Register
)

// getLocalIP:获取本机网卡IP
func getLocalIP() (string, error) {
	// 获取所有网卡
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	// 取第一个非lo的网卡IP
	for _, addr := range addrs {
		// 这个网络地址是IP地址: ipv4, ipv6
		if ipNet, isIpNet := addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			// 跳过IPV6
			if ipNet.IP.To4() != nil {
				ipv4 := ipNet.IP.String() // 192.168.1.1
				return ipv4, err
			}
		}
	}

	err = common.ERR_NO_LOCAL_IP_FOUND
	return "", err
}

// keepOnline:注册到/cron/workers/IP, 并自动续租
func (register *Register) keepOnline() error {
	var err error
	for {
		// 注册路径
		regKey := common.JOB_WORKER_DIR + register.localIP
		cancelCtx, cancelFunc := context.WithCancel(context.TODO())
		defer func() {
			if err != nil {
				time.Sleep(1 * time.Second)
				if cancelFunc != nil {
					cancelFunc()
				}
			}
		}()
		// 创建租约
		leaseGrantResp, err := register.lease.Grant(context.TODO(), 10)
		if err != nil {
			return err
		}
		// 自动续租
		keepAliveChan, err := register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID)
		if err != nil {
			return err
		}
		// 注册到etcd
		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			return err
		}
		// 处理续租应答
		for {
			select {
			case keepAliveResp := <-keepAliveChan:
				if keepAliveResp == nil { // 续租失败
					return err
				}
			}
		}
	}
}

// InitRegister:初始化注册事件
func InitRegister(conf *config.Config) error {
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
	// 本机IP
	localIp, err := getLocalIP()
	if err != nil {
		return err
	}
	// 得到KV和Lease的API子集
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIp,
	}
	// 服务注册
	go G_register.keepOnline()
	return err
}
