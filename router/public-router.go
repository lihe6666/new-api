package router

import (
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/middleware"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

// SetPublicRouter 挂载稳线公开 API（fork 新增，独立路由组 /api/public）。
// 匿名可访问：用于首页状态与价格展示，仅读取，不暴露任何管理数据。
func SetPublicRouter(router *gin.Engine) {
	publicRouter := router.Group("/api/public")
	publicRouter.Use(middleware.RouteTag("api"))
	publicRouter.Use(gzip.Gzip(gzip.DefaultCompression))
	publicRouter.Use(middleware.GlobalAPIRateLimit())
	publicRouter.GET("/status", controller.GetPublicStatus)
	publicRouter.GET("/pricing", controller.GetPublicPricing)
}
