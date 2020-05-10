package master

import (
	"context"
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/master/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

// LogMgr:mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

// LogMgrInterface:LogMgr接口
type LogMgrInterface interface {
	SelectListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error)
}

// SelectListLog:查询日志列表
func (logMgr *LogMgr) SelectListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	// len(logArr)
	logArr = make([]*common.JobLog, 0)
	// 过滤条件
	filter := &common.JobLogFilter{JobName: name}
	// 按照任务开始时间倒排
	logSort := &common.SortLogByStartTime{SortOrder: -1}
	limit64 := int64(limit)
	skip64 := int64(skip)
	// 查询
	cursor, err := logMgr.logCollection.Find(context.TODO(), filter, &options.FindOptions{
		Skip: &skip64, Limit: &limit64, Sort: logSort})
	if err != nil {
		return
	}
	// 延迟释放游标
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		jobLog := &common.JobLog{}

		// 反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			continue // 有日志不合法
		}
		logArr = append(logArr, jobLog)
	}
	return
}

// InitLogMgr:初始化日志管理
func InitLogMgr(con *config.Config) (err error) {
	clientOptions := options.Client().ApplyURI(con.MongoURI)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(con.MongoConnectTimeout)*time.Millisecond)
	if client, err := mongo.Connect(ctx, clientOptions); err != nil {
		return err
	} else {
		if err := client.Ping(ctx, readpref.Primary()); err != nil {
			return err
		}
		G_logMgr = &LogMgr{
			client:        client,
			logCollection: client.Database("cron").Collection("log"),
		}
	}
	return
}
