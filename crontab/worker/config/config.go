package config

import (
	"encoding/json"
	"io/ioutil"
)

// Config:设置结构体
type Config struct {
	EtcdEndPoints       []string `json:"etcd_end_points"`
	EtcdDtailTimeout    int      `json:"etcd_dtail_timeout"`
	MongoURI            string   `json:"mongo_uri"`
	MongoConnectTimeout int      `json:"mongo_connect_timeout"`
	JobLogBatchSize     int      `json:"job_log_batch_size"`
	JobLogCommitTime    int      `json:"job_log_commit_time"`
}

var (
	G_Config *Config
)

// InitConfig:解析json文件到config体中
func InitConfig(filename string) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	GC := &Config{}
	err = json.Unmarshal(file, GC)
	G_Config = GC
	return err
}
