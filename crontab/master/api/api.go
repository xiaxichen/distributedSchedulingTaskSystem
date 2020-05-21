package api

import (
	"distributedSchedulingTask/crontab/common"
	"distributedSchedulingTask/crontab/master"
	"distributedSchedulingTask/crontab/master/handler"
	"github.com/gin-gonic/gin"
	"github.com/gorhill/cronexpr"
	"net/http"
	"strconv"
)

// HandlerJobSave:保存任务
func HandlerJobSave(ctx *gin.Context) error {
	data := &common.Job{}
	err := ctx.BindJSON(data)
	if err != nil {
		return handler.ParameterError("Post Data Err")
	}
	if data.CronExpr == "" {
		data.JobType = common.JOB_TYPE_TEMPORARY
	} else {
		_, err = cronexpr.Parse(data.CronExpr)
		if err != nil {
			return err
		}
	}
	job, err := master.G_jobMgr.SaveJob(data)
	if err != nil {
		return err
	}
	ctx.JSON(http.StatusOK, common.Response{Error: 0, Msg: "", Data: job})
	return nil
}

// HandlerJobDelete:删除任务
func HandlerJobDelete(ctx *gin.Context) error {
	data := &common.DelJob{}
	err := ctx.BindJSON(data)
	if err != nil {
		return handler.ParameterError("Post Data Err")
	}
	job, err := master.G_jobMgr.DeleteJob(data)
	if err != nil {
		return err
	}
	ctx.JSON(http.StatusOK, common.Response{Error: 0, Msg: "", Data: job})
	return nil
}

// HandlerJobList:任务列表
func HandlerJobList(ctx *gin.Context) error {
	jobs, err := master.G_jobMgr.ListJob()
	if err != nil {
		return err
	}
	ctx.JSON(http.StatusOK, common.Response{Error: 0, Msg: "", Data: jobs})
	return nil
}

// HandleWorkerList:获取健康worker节点列表
func HandleWorkerList(ctx *gin.Context) error {

	workerArr, err := master.G_workerMgr.ListWorkers()
	if err != nil {
		return err
	}

	// 正常应答
	ctx.JSON(http.StatusOK, common.Response{Error: 0, Msg: "", Data: workerArr})
	return nil
}

// HandlerJobLog:获取任务日志
func HandlerJobLog(ctx *gin.Context) error {
	name, b := ctx.GetQuery("name")
	skipParam := ctx.DefaultQuery("skip", "0")
	skip, err := strconv.Atoi(skipParam)
	if err != nil {
		skip = 0
	}
	limitParam := ctx.DefaultQuery("limit", "0")
	limit, err := strconv.Atoi(limitParam)
	if err != nil {
		skip = 0
	}
	if b {
		if log, err := master.G_logMgr.SelectListLog(name, skip, limit); err != nil {
			return err
		} else {
			ctx.JSON(http.StatusOK, common.Response{Error: 0, Msg: "", Data: log})
		}
	} else {
		ctx.JSON(http.StatusOK, common.Response{Error: -1, Msg: "param error!", Data: nil})
	}
	return nil
}

// HandlerKiller:杀死任务
func HandlerKiller(ctx *gin.Context) error {
	query, b := ctx.GetQuery("name")
	if b {
		err := master.G_jobMgr.KillJob(query)
		if err != nil {
			return err
		}
		ctx.JSON(http.StatusOK, common.Response{Error: 0, Msg: "", Data: nil})
	} else {
		ctx.JSON(http.StatusOK, common.Response{Error: -1, Msg: "param error!", Data: nil})
	}
	return nil
}

// HandlerStartJob:开始指定任务
func HandlerStartJob(ctx *gin.Context) error {
	query, b := ctx.GetQuery("name")
	if b {
		err := master.G_jobMgr.StartJob(query)
		if err != nil {
			return err
		}
		ctx.JSON(http.StatusOK, common.Response{Error: 0, Msg: "", Data: nil})
	} else {
		ctx.JSON(http.StatusOK, common.Response{Error: -1, Msg: "param error!", Data: nil})
	}
	return nil
}
