// 基础设施：Redis。
package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
)

// NewRedis 初始化 Redis 客户端。
// Redis 用于缓存、限流及会话管理。
func NewRedis(ctx context.Context, cfg *config.Redis) (*redis.Client, error) {
	cli := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cli.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return cli, nil
}

/*
// Stream 相关 Key 常量（对齐 td.md §5.4）。
const (
	StreamDocETL = "stream:doc-etl" // 文档 ETL 任务流
	StreamEval   = "stream:eval"    // 评估任务流
	StreamDLQ    = "stream:dlq"     // 死信队列
)

// Consumer Group 常量。
const (
	GroupDocETL = "doc-etl-workers"
	GroupEval   = "eval-workers"
)
*/
