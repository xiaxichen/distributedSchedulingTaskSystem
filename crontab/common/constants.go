package common

const (
	//任务目录
	JOB_SAVE_DIR  = "/cron/jobs/save/"
	JOB_KILL_DIR  = "/cron/killer/"
	JOB_LOCK_DIR  = "/cron/lock/"
	JOB_START_DIR = "/cron/jobs/start/"
	JOB_DIR       = "/cron/jobs/"
	//事件类型
	JOB_EVEN_SAVE   = 1
	JOB_EVEN_DELETE = 2
	JOB_EVEN_KILL   = 3
	JOB_EVEN_START  = 4
	// 服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"
	// 任务类型
	JOB_TYPE_CRON      = 0
	JOB_TYPE_TEMPORARY = 1
)
