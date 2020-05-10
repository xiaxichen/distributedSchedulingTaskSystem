package common

const (
	JOB_SAVE_DIR    = "/cron/jobs/"
	JOB_KILL_DIR    = "/cron/killer/"
	JOB_LOCK_DIR    = "/cron/lock/"
	JOB_EVEN_SAVE   = 1
	JOB_EVEN_DELETE = 2
	JOB_EVEN_KILL   = 3
	// 服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"
)
