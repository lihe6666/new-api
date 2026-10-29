package model

import (
	"time"
)

// StablelineProbe 是稳线公开状态页的探活采样记录。
// 独立自有表：不改动、不迁移任何上游表结构，随我们的 monitor 写入。
type StablelineProbe struct {
	Id        int64     `json:"id" gorm:"primaryKey autoIncrement"`
	ChannelId int       `json:"channel_id" gorm:"size:10;index:idx_stableline_probe_ch_time,priority:1"`
	Model     string    `json:"model" gorm:"size:128"`
	Latency   int64     `json:"latency"` // 毫秒
	Success   bool      `json:"success"`
	Error     string    `json:"error" gorm:"type:varchar(1024)"`
	CreatedAt time.Time `json:"created_at" gorm:"index:idx_stableline_probe_ch_time,priority:2"`
}

func StablelineInitTable() error {
	return DB.AutoMigrate(&StablelineProbe{})
}

func StablelineSaveProbes(probes []*StablelineProbe) error {
	if len(probes) == 0 {
		return nil
	}
	return DB.CreateInBatches(probes, 100).Error
}

func StablelineDeleteProbesBefore(t time.Time) error {
	return DB.Where("created_at < ?", t).Delete(&StablelineProbe{}).Error
}

// StablelineChannelStat 按渠道聚合的探活统计。
type StablelineChannelStat struct {
	ChannelId   int        `json:"channel_id"`
	Total       int64      `json:"total"`
	Ok          int64      `json:"ok"`
	AvgLatency  float64    `json:"avg_latency"` // 毫秒，仅成功样本
	LastProbeAt *time.Time `json:"last_probe_at"`
}

type stablelineProbeAggRow struct {
	ChannelId  int
	Total      int64
	Ok         int64
	SumLatency int64
}

// stablelineLastProbes 单独查每渠道最近样本时间：MAX() 的驱动返回类型五花八门，
// 扫描到 string 字段后统一解析，规避 interface{} 字段不被 GORM 填充的问题。
func stablelineLastProbes(since time.Time) (map[int]time.Time, error) {
	var rows []struct {
		ChannelId   int
		LastProbeAt string
	}
	err := DB.Model(&StablelineProbe{}).
		Where("created_at >= ?", since).
		Group("channel_id").
		Select("channel_id, MAX(created_at) AS last_probe_at").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make(map[int]time.Time, len(rows))
	for _, r := range rows {
		if t, ok := parseAggTimeString(r.LastProbeAt); ok {
			out[r.ChannelId] = t
		}
	}
	return out, nil
}

func stablelineAggregate(since time.Time) ([]StablelineChannelStat, map[int]time.Time, error) {
	var rows []stablelineProbeAggRow
	err := DB.Model(&StablelineProbe{}).
		Where("created_at >= ?", since).
		Group("channel_id").
		Select(
			"channel_id, COUNT(*) AS total, "+
				"COALESCE(SUM(CASE WHEN success = "+commonTrueVal+" THEN 1 ELSE 0 END), 0) AS ok, "+
				"COALESCE(SUM(CASE WHEN success = "+commonTrueVal+" THEN latency ELSE 0 END), 0) AS sum_latency",
		).
		Scan(&rows).Error
	if err != nil {
		return nil, nil, err
	}
	lastProbes, err := stablelineLastProbes(since)
	if err != nil {
		return nil, nil, err
	}
	stats := make([]StablelineChannelStat, 0, len(rows))
	for _, r := range rows {
		avg := 0.0
		if r.Ok > 0 {
			avg = float64(r.SumLatency) / float64(r.Ok)
		}
		stat := StablelineChannelStat{
			ChannelId:  r.ChannelId,
			Total:      r.Total,
			Ok:         r.Ok,
			AvgLatency: avg,
		}
		if last, ok := lastProbes[r.ChannelId]; ok {
			stat.LastProbeAt = &last
		}
		stats = append(stats, stat)
	}
	return stats, lastProbes, nil
}

// StablelineChannelStats24h 返回最近 24 小时的分渠道聚合。
func StablelineChannelStats24h(now time.Time) ([]StablelineChannelStat, error) {
	stats, _, err := stablelineAggregate(now.Add(-24 * time.Hour))
	return stats, err
}

// StablelineChannelStats30d 返回最近 30 天的分渠道聚合。
func StablelineChannelStats30d(now time.Time) ([]StablelineChannelStat, error) {
	stats, _, err := stablelineAggregate(now.Add(-30 * 24 * time.Hour))
	return stats, err
}

// StablelineEarliestProbeAt 返回最早一条探活样本的时间（站点运行天数的兜底口径）。
func StablelineEarliestProbeAt() (time.Time, error) {
	var v any
	err := DB.Raw("SELECT MIN(created_at) FROM stableline_probes").Row().Scan(&v)
	if err != nil {
		return time.Time{}, err
	}
	if t, ok := parseAggTimeAny(v); ok {
		return t, nil
	}
	return time.Time{}, nil
}

// 各驱动存时间的方式不同（SQLite 文本 / MySQL time.Time / PG 附带时区文本），统一解析。
func parseAggTimeString(s string) (time.Time, bool) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999+00:00",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, s); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func parseAggTimeAny(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case string:
		return parseAggTimeString(t)
	case []byte:
		return parseAggTimeString(string(t))
	}
	return time.Time{}, false
}
