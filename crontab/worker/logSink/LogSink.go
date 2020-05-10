package logSink

import (
	"context"
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/worker/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

// LogSink:日志结构体
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

// SaveLogs:存储日志
func (logSink *LogSink) SaveLogs(batch common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// Loop:任务循环
func (logSink *LogSink) Loop() {
	var (
		logBatch    *common.LogBatch
		commitTimer *time.Timer
	)
	for {
		select {
		case log := <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				commitTimer = time.AfterFunc(time.Duration(config.G_Config.JobLogCommitTime)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- logBatch
						}
					}(logBatch),
				)
			}
			//把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)
			if len(logBatch.Logs) >= config.G_Config.JobLogBatchSize {
				go logSink.SaveLogs(*logBatch)
				logBatch = nil
				commitTimer.Stop()
			}
		case batch := <-logSink.autoCommitChan:
			if batch != logBatch {
				continue //跳过已经被提交的批次
			}
			go logSink.SaveLogs(*logBatch)
			logBatch = nil
		}
	}
}

// Send:发送日志
func (logSink *LogSink) Send(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		//队伍满了就丢弃
	}
}

// InitLogSink:初始化任务记录
func InitLogSink(conf *config.Config) error {
	clientOptions := options.Client().ApplyURI(conf.MongoURI)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(conf.MongoConnectTimeout)*time.Millisecond)
	if client, err := mongo.Connect(ctx, clientOptions); err != nil {
		return err
	} else {
		if err := client.Ping(ctx, readpref.Primary()); err != nil {
			return err
		}
		G_logSink = &LogSink{
			client:         client,
			logCollection:  client.Database("cron").Collection("log"),
			logChan:        make(chan *common.JobLog, 1000),
			autoCommitChan: make(chan *common.LogBatch, 1000),
		}
	}
	return nil
}
