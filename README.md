# distributedSchedulingTaskSystem
分布式调度系统

##架构图

![架构.png](https://github.com/xiaxichen/distributedSchedulingTaskSystem/blob/master/doc/%E5%88%86%E5%B8%83%E5%BC%8F%E8%B0%83%E5%BA%A6%E6%9E%B6%E6%9E%84.png)

##主集群功能

任务管理HTTP接口:增删改查   
任务日志HTTP接口:查看任务执行历史  
任务控制HTTP接口:提供强制结束任务的接口

##woker集群功能

任务同步:监听etcd中/cron/jobs/目录变化  
任务调度:基于cron表达式计算,触发过期任务  
任务执行:协程池并发执行多任务,基于etcdf分布式锁抢占  
日志保存:捕获任务执行输出,保存到MongoDB  

##web页面

![masterWeb.png](https://github.com/xiaxichen/distributedSchedulingTaskSystem/blob/master/doc/masterWeb.png)

##master实现

![masterWeb.png](https://github.com/xiaxichen/distributedSchedulingTaskSystem/blob/master/doc/master%E5%AE%9E%E7%8E%B0.png)

##worker实现

![masterWeb.png](https://github.com/xiaxichen/distributedSchedulingTaskSystem/blob/master/doc/woker%E5%AE%9E%E7%8E%B0.png)

#启动
首先启动etcd集群(单机也可)和mongo;
其次配置master/main下的配置文件和woker/main下的配置文件;
```bash
#master节点
#进入master/main 
go run main.go
#进入worker/main
go run main.go
```