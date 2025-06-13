package simple_redis

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// 调试高并发失败原因
func TestDebugFailureReasons(t *testing.T) {
	fmt.Println("=== 调试高并发失败原因 ===")

	// 测试不同规模下的失败模式
	testCases := []struct {
		name        string
		connections int
		commands    int
	}{
		{"正常规模", 100, 5},
		{"中等压力", 500, 3},
		{"高压力", 1000, 2},
		{"极限压力", 2000, 1},
	}

	for _, tc := range testCases {
		fmt.Printf("\n--- %s测试 (%d连接 x %d命令) ---\n", tc.name, tc.connections, tc.commands)

		// 测试传统服务器
		fmt.Printf("🔍 传统服务器详细分析:\n")
		analyzeTraditionalServer(tc.connections, tc.commands)

		time.Sleep(500 * time.Millisecond)

		// 测试Ants服务器
		fmt.Printf("\n🔍 Ants服务器详细分析:\n")
		analyzeAntsServer(tc.connections, tc.commands, tc.connections/2)

		time.Sleep(500 * time.Millisecond)
	}
}

// 分析传统服务器失败原因
func analyzeTraditionalServer(connections, commandsPerConn int) {
	redis := NewRedis()
	server := NewServer(redis)

	go func() {
		server.Start(":6420")
	}()
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	results := make(chan ConnectionResult, connections)

	start := time.Now()

	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			result := testSingleConnection(":6420", clientID, commandsPerConn)
			results <- result
		}(i)
	}

	wg.Wait()
	close(results)

	duration := time.Since(start)
	analyzeResults("传统服务器", results, duration, connections)
}

// 分析Ants服务器失败原因
func analyzeAntsServer(connections, commandsPerConn, poolSize int) {
	redis := NewRedis()
	server, err := NewAntsServer(redis, poolSize)
	if err != nil {
		fmt.Printf("❌ 创建Ants服务器失败: %v\n", err)
		return
	}

	go func() {
		server.Start(":6421")
	}()
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	results := make(chan ConnectionResult, connections)

	start := time.Now()

	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			result := testSingleConnection(":6421", clientID, commandsPerConn)
			results <- result
		}(i)
	}

	wg.Wait()
	close(results)

	duration := time.Since(start)
	analyzeResults(fmt.Sprintf("Ants服务器(池大小:%d)", poolSize), results, duration, connections)

	server.Stop()
}

// 连接测试结果
type ConnectionResult struct {
	ClientID    int
	Success     bool
	Error       string
	Duration    time.Duration
	CommandsRun int
}

// 测试单个连接
func testSingleConnection(addr string, clientID, commandsPerConn int) ConnectionResult {
	result := ConnectionResult{
		ClientID: clientID,
		Success:  false,
	}

	start := time.Now()
	defer func() {
		result.Duration = time.Since(start)
	}()

	// 尝试连接
	conn, err := net.DialTimeout("tcp", "localhost"+addr, 5*time.Second)
	if err != nil {
		result.Error = fmt.Sprintf("连接失败: %v", err)
		return result
	}
	defer conn.Close()

	// 设置读写超时
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// 执行命令
	for i := 0; i < commandsPerConn; i++ {
		key := fmt.Sprintf("debug_%d_%d", clientID, i)
		value := fmt.Sprintf("val_%d", i)

		// SET命令
		setCmd := fmt.Sprintf("SET %s %s\n", key, value)
		_, err := conn.Write([]byte(setCmd))
		if err != nil {
			result.Error = fmt.Sprintf("写SET命令失败(第%d个): %v", i+1, err)
			return result
		}

		// 读取SET响应
		buffer := make([]byte, 1024)
		_, err = conn.Read(buffer)
		if err != nil {
			result.Error = fmt.Sprintf("读SET响应失败(第%d个): %v", i+1, err)
			return result
		}

		// GET命令
		getCmd := fmt.Sprintf("GET %s\n", key)
		_, err = conn.Write([]byte(getCmd))
		if err != nil {
			result.Error = fmt.Sprintf("写GET命令失败(第%d个): %v", i+1, err)
			return result
		}

		// 读取GET响应
		_, err = conn.Read(buffer)
		if err != nil {
			result.Error = fmt.Sprintf("读GET响应失败(第%d个): %v", i+1, err)
			return result
		}

		result.CommandsRun++
	}

	result.Success = true
	return result
}

// 分析测试结果
func analyzeResults(serverType string, results chan ConnectionResult, duration time.Duration, totalConnections int) {
	var (
		successCount    = 0
		connectionFails = 0
		timeoutFails    = 0
		writeFails      = 0
		readFails       = 0
		otherFails      = 0
		totalCommands   = 0
	)

	errorSamples := make(map[string]int)

	for result := range results {
		totalCommands += result.CommandsRun

		if result.Success {
			successCount++
		} else {
			// 分类错误类型
			if result.Error != "" {
				errorSamples[result.Error]++

				switch {
				case contains(result.Error, "连接失败"):
					connectionFails++
				case contains(result.Error, "timeout") || contains(result.Error, "deadline"):
					timeoutFails++
				case contains(result.Error, "写") && contains(result.Error, "失败"):
					writeFails++
				case contains(result.Error, "读") && contains(result.Error, "失败"):
					readFails++
				default:
					otherFails++
				}
			}
		}
	}

	successRate := float64(successCount) / float64(totalConnections) * 100

	fmt.Printf("📊 %s 结果分析:\n", serverType)
	fmt.Printf("   总连接数: %d\n", totalConnections)
	fmt.Printf("   成功连接: %d (%.1f%%)\n", successCount, successRate)
	fmt.Printf("   失败连接: %d (%.1f%%)\n", totalConnections-successCount, 100-successRate)
	fmt.Printf("   总耗时: %v\n", duration)
	fmt.Printf("   完成命令数: %d\n", totalCommands)

	if totalConnections > successCount {
		fmt.Printf("\n🔍 失败原因分析:\n")
		fmt.Printf("   连接失败: %d\n", connectionFails)
		fmt.Printf("   超时失败: %d\n", timeoutFails)
		fmt.Printf("   写入失败: %d\n", writeFails)
		fmt.Printf("   读取失败: %d\n", readFails)
		fmt.Printf("   其他失败: %d\n", otherFails)

		fmt.Printf("\n📝 错误样本 (前5个):\n")
		count := 0
		for errMsg, freq := range errorSamples {
			if count >= 5 {
				break
			}
			fmt.Printf("   [%dx] %s\n", freq, errMsg)
			count++
		}
	}

	if successCount > 0 {
		avgQPS := float64(totalCommands*2) / duration.Seconds() // SET + GET
		fmt.Printf("📈 性能指标:\n")
		fmt.Printf("   平均QPS: %.0f\n", avgQPS)
	}
}

// 辅助函数：检查字符串是否包含子串
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					findSubstring(s, substr))))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// 系统资源限制测试
func TestSystemLimits(t *testing.T) {
	fmt.Println("\n=== 系统资源限制检查 ===")

	// 检查文件描述符限制
	fmt.Printf("🔍 检查系统限制...\n")

	// 尝试创建大量连接来测试限制
	maxConnections := 0
	connections := make([]net.Conn, 0)

	// 启动一个简单的服务器
	listener, err := net.Listen("tcp", ":6422")
	if err != nil {
		fmt.Printf("❌ 无法启动测试服务器: %v\n", err)
		return
	}
	defer listener.Close()

	// 接受连接的goroutine
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// 保持连接打开
			go func(c net.Conn) {
				buffer := make([]byte, 1024)
				for {
					_, err := c.Read(buffer)
					if err != nil {
						c.Close()
						return
					}
				}
			}(conn)
		}
	}()

	// 尝试创建连接直到失败
	for i := 0; i < 10000; i++ {
		conn, err := net.Dial("tcp", "localhost:6422")
		if err != nil {
			fmt.Printf("❌ 在第%d个连接时失败: %v\n", i+1, err)
			break
		}
		connections = append(connections, conn)
		maxConnections++

		if i%100 == 0 {
			fmt.Printf("✅ 已创建 %d 个连接\n", i+1)
		}
	}

	fmt.Printf("📊 系统限制分析:\n")
	fmt.Printf("   最大并发连接数: %d\n", maxConnections)

	// 清理连接
	for _, conn := range connections {
		conn.Close()
	}

	if maxConnections < 1000 {
		fmt.Printf("⚠️  系统连接限制较低，这可能是高并发测试失败的原因\n")
		fmt.Printf("💡 建议:\n")
		fmt.Printf("   - 检查 ulimit -n (文件描述符限制)\n")
		fmt.Printf("   - 检查系统的 net.core.somaxconn 设置\n")
		fmt.Printf("   - 考虑调整系统参数以支持更高并发\n")
	}
}
