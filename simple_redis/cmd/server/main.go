package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redis "simple_redis"
)

func main() {
	fmt.Println("启动 Simple Redis 服务器...")
	fmt.Println("监听端口: 6379")

	// 创建 Redis 实例
	r := redis.NewRedis()

	// 尝试从持久化文件加载数据
	if err := r.LoadRDB(); err != nil {
		fmt.Printf("加载 RDB 文件失败 (这是正常的，如果是首次启动): %v\n", err)
	} else {
		fmt.Println("成功从 RDB 文件加载数据")
	}

	// 启动RDB保存检查器（每10秒检查一次保存条件）
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if r.ShouldSave() {
				changes, duration := r.GetSaveStats()
				fmt.Printf("开始执行RDB保存 (%.0f秒内%d次修改)...\n", duration.Seconds(), changes)

				if err := r.SaveRDB(); err != nil {
					fmt.Printf("RDB保存失败: %v\n", err)
				}
			}
		}
	}()

	// 设置优雅关闭
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fmt.Println("\n收到关闭信号，正在保存数据...")

		// 关闭前保存数据
		if err := r.SaveRDB(); err != nil {
			fmt.Printf("关闭前保存数据失败: %v\n", err)
		} else {
			fmt.Println("数据保存成功")
		}

		r.Close()
		fmt.Println("服务器已关闭")
		os.Exit(0)
	}()

	// 创建并启动服务器
	server := redis.NewServer(r)

	fmt.Println("Simple Redis 服务器已启动")
	fmt.Println("可以使用以下命令连接:")
	fmt.Println("  go run cmd/redis-cli/main.go")
	fmt.Println("按 Ctrl+C 停止服务器")
	fmt.Println()
	fmt.Println("RDB保存规则:")
	fmt.Println("  save 900 1     # 15分钟内至少1个修改")
	fmt.Println("  save 300 10    # 5分钟内至少10个修改")
	fmt.Println("  save 60 10000  # 1分钟内至少10000个修改")
	fmt.Println("每10秒检查一次保存条件")
	fmt.Println()

	// 启动服务器
	if err := server.Start(":6379"); err != nil {
		log.Fatalf("服务器启动失败: %v", err)
	}
}
