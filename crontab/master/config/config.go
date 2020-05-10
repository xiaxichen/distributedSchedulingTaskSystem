package config

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ApiPort             int      `json:"api_port"`
	ApiReadTimeout      int      `json:"api_read_timeout"`
	ApiWriteTimeout     int      `json:"api_write_timeout"`
	EtcdEndPoints       []string `json:"etcd_end_points"`
	EtcdDtailTimeout    int      `json:"etcd_dtail_timeout"`
	Webroot             string   `json:"webroot"`
	MongoURI            string   `json:"mongo_uri"`
	MongoConnectTimeout int      `json:"mongo_connect_timeout"`
}

var (
	G_Config *Config
)

// InitConfig:根据配置文件初始化任务
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
