package main

import (
	"fmt"
	"io"
	"strings"

	redis "simple_redis"

	"github.com/chzyer/readline"
)

func main() {
	fmt.Println("Simple Redis CLI")
	fmt.Println("连接到 localhost:6379...")

	// 连接到 Redis 服务器
	client, err := redis.NewClient("localhost:6379")
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		fmt.Println("请确保 Redis 服务器正在运行：")
		fmt.Println("  go run cmd/server/main.go")
		return
	}
	defer client.Close()

	fmt.Println("已连接！输入 'help' 查看可用命令，输入 'quit' 或 'exit' 退出")
	fmt.Println("支持上下键浏览命令历史，Tab键自动补全")
	fmt.Println()

	// 配置readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:            "simple-redis> ",
		HistoryFile:       ".redis_history",
		AutoComplete:      completer,
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				fmt.Println("使用 'quit' 或 'exit' 退出，或者按 Ctrl+D")
				continue
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		command := strings.TrimSpace(line)
		if command == "" {
			continue
		}

		// 处理特殊命令
		switch strings.ToLower(command) {
		case "quit", "exit":
			fmt.Println("再见!")
			return
		case "help":
			showHelp()
			continue
		case "clear":
			fmt.Print("\033[2J\033[H") // 清屏
			continue
		case "history":
			showHistory(rl)
			continue
		}

		// 发送命令到服务器
		response, err := client.SendCommand(command)
		if err != nil {
			fmt.Printf("错误: %v\n", err)
			continue
		}

		// 显示响应
		printResponse(response)
	}
}

// 自动补全器
var completer = readline.NewPrefixCompleter(
	readline.PcItem("SET"),
	readline.PcItem("GET"),
	readline.PcItem("HSET"),
	readline.PcItem("HGET"),
	readline.PcItem("HDEL"),
	readline.PcItem("EXPIRE"),
	readline.PcItem("TTL"),
	readline.PcItem("BGSAVE"),
	readline.PcItem("BGREWRITEAOF"),
	readline.PcItem("LASTSAVE"),
	readline.PcItem("PING"),
	readline.PcItem("help"),
	readline.PcItem("clear"),
	readline.PcItem("history"),
	readline.PcItem("quit"),
	readline.PcItem("exit"),
)

func showHistory(rl *readline.Instance) {
	fmt.Println("\n历史命令功能已启用")
	fmt.Println("使用 ↑↓ 键浏览命令历史")
	fmt.Println("历史记录保存在 .redis_history 文件中")
	fmt.Println()
}

func showHelp() {
	fmt.Println(`
可用命令:

字符串操作:
  SET key value          - 设置键值对
  GET key               - 获取键的值

哈希表操作:
  HSET key field value  - 设置哈希表字段
  HGET key field        - 获取哈希表字段值  
  HDEL key field        - 删除哈希表字段

过期设置:
  EXPIRE key seconds    - 设置键的过期时间(秒)
  TTL key              - 获取键的剩余生存时间

持久化:
  BGSAVE               - 后台保存 RDB 快照
  BGREWRITEAOF         - 后台重写 AOF 文件
  LASTSAVE             - 查看上次保存时间和修改统计

其他:
  PING                 - 测试连接
  help                 - 显示此帮助信息
  history              - 显示命令历史
  clear                - 清屏
  quit/exit            - 退出客户端

快捷键:
  ↑↓                   - 浏览命令历史
  Tab                  - 自动补全命令
  Ctrl+A               - 光标移到行首
  Ctrl+E               - 光标移到行尾
  Ctrl+C               - 中断当前输入
  Ctrl+D               - 退出程序

示例:
  SET name "John Doe"
  GET name
  HSET user:1 name Alice
  HGET user:1 name
  EXPIRE name 60
  TTL name
`)
}

func printResponse(response string) {
	// 简单的响应格式化
	if strings.HasPrefix(response, "+") {
		// 简单字符串响应
		fmt.Println(response[1:])
	} else if strings.HasPrefix(response, ":") {
		// 整数响应
		fmt.Println(response[1:])
	} else if strings.HasPrefix(response, "$") {
		// 批量字符串响应
		if response == "$-1" {
			fmt.Println("(nil)")
		} else {
			// 跳过长度信息，直接显示内容
			lines := strings.Split(response, "\n")
			if len(lines) > 1 {
				fmt.Printf("\"%s\"\n", lines[1])
			} else {
				fmt.Println(response)
			}
		}
	} else if strings.HasPrefix(response, "-") {
		// 错误响应
		fmt.Printf("(error) %s\n", response[1:])
	} else {
		// 其他响应
		fmt.Println(response)
	}
}
