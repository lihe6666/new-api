package controller

import (
	"context"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/gin-gonic/gin"
)

// 稳线公开状态/价格 API 与探活采样（fork 新增文件，不改上游逻辑）。
//
// 探活复用上游 testChannel（与控制台「渠道测试」同一条代码路径），但只做观察：
// 不触发自动禁用/启用，禁用逻辑仍归上游自己的测试机制管。

const (
	stablelineStatusTTL    = 30 * time.Second
	stablelinePricingTTL   = 30 * time.Second
	stablelineProbeTimeout = 60 * time.Second
	stablelineStaleAfter   = 15 * time.Minute
	stablelineKeepDays     = 30
)

// —————————————— 探活采样 ——————————————

func stablelineEnvDuration(key string, def time.Duration) time.Duration {
	if raw := os.Getenv(key); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			return d
		}
		common.SysLog(common.LogText("stableline: invalid %s=%q, using %s", key, raw, def))
	}
	return def
}

// StartStablelineMonitor 启动后台探活循环（main.go 以 go 启动，仅 master 节点）。
func StartStablelineMonitor() {
	if os.Getenv("STABLELINE_MONITOR") == "false" {
		common.SysLog(common.LogText("stableline: monitor disabled by STABLELINE_MONITOR"))
		return
	}
	if !common.IsMasterNode {
		return
	}
	if err := model.StablelineInitTable(); err != nil {
		common.SysLog(common.LogText("stableline: init probe table failed: %s", err.Error()))
		return
	}

	interval := stablelineEnvDuration("STABLELINE_PROBE_INTERVAL", 30*time.Second)
	workers := 4
	if raw := os.Getenv("STABLELINE_PROBE_WORKERS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 32 {
			workers = n
		}
	}
	common.SysLog(common.LogText("stableline: monitor started, interval=%s workers=%d", interval, workers))

	go stablelinePurgeLoop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		stablelineProbeOnce(workers)
	}
}

func stablelinePurgeLoop() {
	for {
		model.StablelineDeleteProbesBefore(time.Now().AddDate(0, 0, -stablelineKeepDays))
		time.Sleep(24 * time.Hour)
	}
}

func stablelineProbeOnce(workers int) {
	channels, err := model.GetAllChannels(0, 0, true, false)
	if err != nil {
		common.SysLog(common.LogText("stableline: load channels failed: %s", err.Error()))
		return
	}
	testUserID, err := resolveChannelTestUserID(nil)
	if err != nil {
		common.SysLog(common.LogText("stableline: resolve probe user failed: %s", err.Error()))
		return
	}

	var (
		mu      sync.Mutex
		probes  []*model.StablelineProbe
		wg      sync.WaitGroup
		sem     = make(chan struct{}, workers)
		nowFunc = time.Now
	)
	for _, channel := range channels {
		if channel.Status != common.ChannelStatusEnabled {
			continue
		}
		ch := channel
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), stablelineProbeTimeout)
			defer cancel()
			start := time.Now()
			result := testChannel(ctx, ch, testUserID, "", "", shouldUseStreamForAutomaticChannelTest(ch))
			latency := time.Since(start).Milliseconds()
			if ctx.Err() != nil {
				// 超时/取消的样本不入库，避免把采样器自身抖动算成渠道故障
				return
			}
			probe := &model.StablelineProbe{
				ChannelId: ch.Id,
				Latency:   latency,
				Success:   result.newAPIError == nil,
				CreatedAt: nowFunc(),
			}
			if result.localErr != nil {
				probe.Error = truncateProbeError(result.localErr.Error())
			} else if result.newAPIError != nil {
				probe.Error = truncateProbeError(result.newAPIError.Error())
			}
			mu.Lock()
			probes = append(probes, probe)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if err := model.StablelineSaveProbes(probes); err != nil {
		common.SysLog(common.LogText("stableline: save probes failed: %s", err.Error()))
	}
}

func truncateProbeError(msg string) string {
	if len(msg) > 1000 {
		return msg[:1000]
	}
	return msg
}

// —————————————— 公开状态 API ——————————————

type publicStatusChannel struct {
	Id          int     `json:"id"`
	Name        string  `json:"name"`
	Models      string  `json:"models"`
	Uptime24h   float64 `json:"uptime_24h"` // %
	Uptime30d   float64 `json:"uptime_30d"` // %
	LatencyMs   int64   `json:"latency_ms"`
	Status      string  `json:"status"` // good | warn | crit | unknown
	LastProbeAt string  `json:"last_probe_at"`
}

type publicStatusMetrics struct {
	Uptime30d      float64 `json:"uptime_30d"`       // %
	AvgLatency24h  int64   `json:"avg_latency_24h"`  // ms
	RequestsMonth  int64   `json:"requests_month"`   // 本月请求数（logs 表）
	DaysRunning    int64   `json:"days_running"`     // 首个渠道创建至今
	SampledChannels int    `json:"sampled_channels"` // 有探活样本的渠道数
}

type publicStatusPayload struct {
	Overall     string               `json:"overall"`
	GeneratedAt string               `json:"generated_at"`
	Metrics     publicStatusMetrics  `json:"metrics"`
	Channels    []publicStatusChannel `json:"channels"`
}

var (
	stablelineStatusCacheMu sync.Mutex
	stablelineStatusCache   *publicStatusPayload
	stablelineStatusCacheAt time.Time
)

func uptimePercent(ok, total int64) float64 {
	if total == 0 {
		return 0
	}
	return math.Round(float64(ok)/float64(total)*10000) / 100
}

func classifyChannel(status24 publicStatusChannel, hasProbe bool, lastProbeAt time.Time, now time.Time) string {
	if !hasProbe {
		return "unknown"
	}
	if now.Sub(lastProbeAt) > stablelineStaleAfter {
		return "crit"
	}
	if status24.Uptime24h < 90 {
		return "crit"
	}
	if status24.Uptime24h < 99.9 || status24.LatencyMs > 500 {
		return "warn"
	}
	return "good"
}

func buildPublicStatus() (*publicStatusPayload, error) {
	now := time.Now()
	channels, err := model.GetAllChannels(0, 0, true, false)
	if err != nil {
		return nil, err
	}
	stats24, err := model.StablelineChannelStats24h(now)
	if err != nil {
		return nil, err
	}
	stats30, err := model.StablelineChannelStats30d(now)
	if err != nil {
		return nil, err
	}
	by24 := make(map[int]model.StablelineChannelStat, len(stats24))
	for _, s := range stats24 {
		by24[s.ChannelId] = s
	}
	by30 := make(map[int]model.StablelineChannelStat, len(stats30))
	for _, s := range stats30 {
		by30[s.ChannelId] = s
	}

	payload := &publicStatusPayload{
		GeneratedAt: now.Format(time.RFC3339),
		Channels:    []publicStatusChannel{},
	}
	worstRank := map[string]int{"unknown": 0, "good": 1, "warn": 2, "crit": 3}
	overall := "unknown"

	var sumOk30, sumTotal30, sumOk24, sumTotal24, sumLatency24 int64
	for _, ch := range channels {
		s24, s30 := by24[ch.Id], by30[ch.Id]
		disabled := ch.Status != common.ChannelStatusEnabled
		if disabled && s24.Total == 0 {
			// 已禁用且 24h 内无样本：视为下线，不出现在公示列表
			continue
		}
		row := publicStatusChannel{
			Id:       ch.Id,
			Name:     ch.Name,
			Models:   ch.Models,
			Uptime24h: uptimePercent(s24.Ok, s24.Total),
			Uptime30d: uptimePercent(s30.Ok, s30.Total),
			LatencyMs: int64(math.Round(s24.AvgLatency)),
		}
		lastProbe := time.Time{}
		if s24.LastProbeAt != nil {
			lastProbe = *s24.LastProbeAt
		} else if s30.LastProbeAt != nil {
			lastProbe = *s30.LastProbeAt
		}
		if !lastProbe.IsZero() {
			row.LastProbeAt = lastProbe.Format(time.RFC3339)
		}
		if disabled {
			// 有历史样本但已被禁用：如实公示为不可用
			row.Status = "crit"
		} else {
			row.Status = classifyChannel(row, s24.Total > 0, lastProbe, now)
		}
		if worstRank[row.Status] > worstRank[overall] {
			overall = row.Status
		}
		payload.Channels = append(payload.Channels, row)

		sumOk30 += s30.Ok
		sumTotal30 += s30.Total
		sumOk24 += s24.Ok
		sumTotal24 += s24.Total
		sumLatency24 += int64(s24.AvgLatency * float64(s24.Ok))
		payload.Metrics.SampledChannels += int(s24.Total)
	}
	sort.Slice(payload.Channels, func(i, j int) bool { return payload.Channels[i].Id < payload.Channels[j].Id })
	payload.Overall = overall
	payload.Metrics.Uptime30d = uptimePercent(sumOk30, sumTotal30)
	if sumOk24 > 0 {
		payload.Metrics.AvgLatency24h = sumLatency24 / sumOk24
	}

	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	var requestsMonth int64
	if err := DBCountLogsSince(monthStart, &requestsMonth); err == nil {
		payload.Metrics.RequestsMonth = requestsMonth
	}
	start := now
	if first, err := stablelineFirstChannelCreatedAt(); err == nil && !first.IsZero() && first.Before(start) {
		start = first
	}
	if earliest, err := model.StablelineEarliestProbeAt(); err == nil && !earliest.IsZero() && earliest.Before(start) {
		start = earliest
	}
	if start.Before(now) {
		payload.Metrics.DaysRunning = int64(now.Sub(start).Hours() / 24)
	}
	return payload, nil
}

func DBCountLogsSince(since time.Time, out *int64) error {
	return model.DB.Model(&model.Log{}).Where("created_at >= ?", since).Count(out).Error
}

func stablelineFirstChannelCreatedAt() (time.Time, error) {
	var first model.Channel
	err := model.DB.Select("created_time").Order("created_time").First(&first).Error
	if err != nil {
		return time.Time{}, err
	}
	if first.CreatedTime <= 0 {
		return time.Time{}, nil
	}
	return time.Unix(first.CreatedTime, 0), nil
}

func GetPublicStatus(c *gin.Context) {
	stablelineStatusCacheMu.Lock()
	if stablelineStatusCache != nil && time.Since(stablelineStatusCacheAt) < stablelineStatusTTL {
		cached := stablelineStatusCache
		stablelineStatusCacheMu.Unlock()
		c.JSON(200, gin.H{"success": true, "data": cached})
		return
	}
	stablelineStatusCacheMu.Unlock()

	payload, err := buildPublicStatus()
	if err != nil {
		common.ApiError(c, err)
		return
	}
	stablelineStatusCacheMu.Lock()
	stablelineStatusCache = payload
	stablelineStatusCacheAt = time.Now()
	stablelineStatusCacheMu.Unlock()
	c.JSON(200, gin.H{"success": true, "data": payload})
}

// —————————————— 公开价格 API ——————————————

type publicPricingModel struct {
	Model      string  `json:"model"`
	Vendor     string  `json:"vendor"`
	Tags       string  `json:"tags,omitempty"`
	Agent      bool    `json:"agent"`
	QuotaType  int     `json:"quota_type"` // 0 按量 1 按次
	InputPrice float64 `json:"input_price,omitempty"`  // ¥/百万 tokens（quota_type=0）
	OutputPrice float64 `json:"output_price,omitempty"` // ¥/百万 tokens（quota_type=0）
	Price      float64 `json:"price,omitempty"`         // ¥/次（quota_type=1）
}

type publicPricingPayload struct {
	Currency    string               `json:"currency"`
	Unit        string               `json:"unit"`
	Models      []publicPricingModel `json:"models"`
	GeneratedAt string               `json:"generated_at"`
}

var (
	stablelinePricingCacheMu sync.Mutex
	stablelinePricingCache   *publicPricingPayload
	stablelinePricingCacheAt time.Time
)

// quotaPerMillionTokens 把 ratio 换算成 ¥/百万 tokens（与控制台一致的换算口径）。
func quotaPerMillionTokens(ratio float64) float64 {
	usd := ratio * 1_000_000 / common.QuotaPerUnit
	return math.Round(usd*operation_setting.USDExchangeRate*100) / 100
}

func quotaToCNY(quota float64) float64 {
	return math.Round(quota/common.QuotaPerUnit*operation_setting.USDExchangeRate*100) / 100
}

func buildPublicPricing() *publicPricingPayload {
	pricing := model.GetPricing()
	usableGroup := service.GetUserUsableGroups("")
	pricing = filterPricingByUsableGroups(pricing, usableGroup)

	payload := &publicPricingPayload{
		Currency:    "CNY",
		Unit:        "per_1m_tokens",
		Models:      make([]publicPricingModel, 0, len(pricing)),
		GeneratedAt: time.Now().Format(time.RFC3339),
	}
	for _, p := range pricing {
		if p.ModelName == "" {
			continue
		}
		row := publicPricingModel{
			Model:     p.ModelName,
			Vendor:    p.OwnerBy,
			Tags:      p.Tags,
			Agent:     strings.Contains(strings.ToLower(p.Tags), "agent"),
			QuotaType: p.QuotaType,
		}
		switch p.QuotaType {
		case 1:
			row.Price = quotaToCNY(p.ModelPrice)
		default:
			row.InputPrice = quotaPerMillionTokens(p.ModelRatio)
			row.OutputPrice = quotaPerMillionTokens(p.ModelRatio * p.CompletionRatio)
		}
		payload.Models = append(payload.Models, row)
	}
	sort.Slice(payload.Models, func(i, j int) bool {
		if payload.Models[i].Vendor != payload.Models[j].Vendor {
			return payload.Models[i].Vendor < payload.Models[j].Vendor
		}
		return payload.Models[i].Model < payload.Models[j].Model
	})
	return payload
}

func GetPublicPricing(c *gin.Context) {
	stablelinePricingCacheMu.Lock()
	if stablelinePricingCache != nil && time.Since(stablelinePricingCacheAt) < stablelinePricingTTL {
		cached := stablelinePricingCache
		stablelinePricingCacheMu.Unlock()
		c.JSON(200, gin.H{"success": true, "data": cached})
		return
	}
	stablelinePricingCacheMu.Unlock()

	payload := buildPublicPricing()
	stablelinePricingCacheMu.Lock()
	stablelinePricingCache = payload
	stablelinePricingCacheAt = time.Now()
	stablelinePricingCacheMu.Unlock()
	c.JSON(200, gin.H{"success": true, "data": payload})
}
