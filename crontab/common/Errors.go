package common

import "errors"

var (
	ERR_LOCK_ALREADY_REQUIRED = errors.New("锁被占用!")
	ERR_NO_LOCAL_IP_FOUND     = errors.New("没有找到网卡IP")
	ERR_NOT_FOUND_EVENT       = errors.New("没有找到对应事件")
	ERR_NOT_FOUND_JOB_TYPE    = errors.New("没有找到对应任务类型")
	ERR_JOB_IS_RUNNING        = errors.New("任务正在运行")
	ERR_JOB_NOT_RUNNING       = errors.New("任务没在运行")
)
