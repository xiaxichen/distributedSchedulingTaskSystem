package route

import (
	"distributedSchedulingTask/crontab/master/api"
	"distributedSchedulingTask/crontab/master/config"
	"distributedSchedulingTask/crontab/master/handler"
	"distributedSchedulingTask/crontab/master/util"
	"github.com/gin-gonic/gin"
	"time"
)

func InitRoute(con *config.Config) *gin.Engine {
	route := gin.Default()
	route.Use(util.Cors())
	route.Use(util.TimeOutMiddleware(time.Duration(con.ApiReadTimeout * int(time.Millisecond))))
	route.POST("/job/save", handler.Wrapper(api.HandlerJobSave))
	route.POST("/job/delete", handler.Wrapper(api.HandlerJobDelete))
	route.GET("/job/list", handler.Wrapper(api.HandlerJobList))
	route.GET("/job/kill", handler.Wrapper(api.HandlerKiller))
	route.GET("/job/log", handler.Wrapper(api.HandlerJobLog))
	route.GET("/job/start", handler.Wrapper(api.HandlerStartJob))
	route.GET("/worker/list", handler.Wrapper(api.HandleWorkerList))
	route.StaticFile("/", con.Webroot)
	return route
}
