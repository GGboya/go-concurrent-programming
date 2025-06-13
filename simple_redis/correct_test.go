package simple_redis

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// 正确的测试：多个客户端连接同一个服务器
func TestCorrectConcurrency(t *testing.T) {
	fmt.Println("=== 正确的并发测试：多客户端连接同一服务器 ===")

	testCases := []struct {
		name        string
		connections int
		commands    int
		poolSize    int
	}{
		{"轻量级", 100, 5, 50},
		{"中等规模", 300, 3, 150},
		{"重量级", 500, 2, 250},
		{"极限测试", 1000, 1, 500},
	}

	for _, tc := range testCases {
		fmt.Printf("\n--- %s测试 (%d客户端 x %d命令) ---\n", tc.name, tc.connections, tc.commands)

		// 测试传统服务器
		fmt.Printf("🔄 传统服务器测试...\n")
		traditionalResult := testSingleTraditionalServer(tc.connections, tc.commands)
		fmt.Printf("   结果: 成功率 %.1f%%, 耗时 %v, QPS %.0f\n",
			traditionalResult.SuccessRate, traditionalResult.Duration, traditionalResult.QPS)

		time.Sleep(200 * time.Millisecond)

		// 测试Ants服务器
		fmt.Printf("🔄 Ants协程池服务器测试...\n")
		antsResult := testSingleAntsServer(tc.connections, tc.commands, tc.poolSize)
		fmt.Printf("   结果: 成功率 %.1f%%, 耗时 %v, QPS %.0f\n",
			antsResult.SuccessRate, antsResult.Duration, antsResult.QPS)

		// 对比分析
		if traditionalResult.SuccessRate > 90 && antsResult.SuccessRate > 90 {
			fmt.Printf("📊 性能对比:\n")
			if traditionalResult.QPS > antsResult.QPS {
				speedup := traditionalResult.QPS / antsResult.QPS
				fmt.Printf("   🏆 传统服务器获胜！快了 %.2fx\n", speedup)
			} else {
				speedup := antsResult.QPS / traditionalResult.QPS
				fmt.Printf("   🏆 Ants服务器获胜！快了 %.2fx\n", speedup)
			}
			fmt.Printf("   💾 资源节省: Ants使用 %d goroutines vs 传统 %d goroutines\n",
				tc.poolSize, tc.connections)
		} else {
			fmt.Printf("⚠️  高并发下出现失败，需要优化\n")
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// 测试结果
type TestResult struct {
	SuccessCount int
	TotalCount   int
	SuccessRate  float64
	Duration     time.Duration
	QPS          float64
	Errors       map[string]int
}

// 测试单个传统服务器实例
func testSingleTraditionalServer(connections, commandsPerConn int) TestResult {
	// 创建一个Redis实例和一个服务器实例
	redis := NewRedis()
	server := NewServer(redis)

	// 启动服务器
	serverReady := make(chan bool)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("服务器panic: %v\n", r)
			}
		}()

		// 通知服务器已准备好
		go func() {
			time.Sleep(50 * time.Millisecond)
			serverReady <- true
		}()

		server.Start(":6430")
	}()

	// 等待服务器启动
	<-serverReady
	time.Sleep(50 * time.Millisecond)

	// 现在多个客户端连接这个单一服务器实例
	return runConcurrentClients(":6430", connections, commandsPerConn)
}

// 测试单个Ants服务器实例
func testSingleAntsServer(connections, commandsPerConn, poolSize int) TestResult {
	// 创建一个Redis实例和一个Ants服务器实例
	redis := NewRedis()
	server, err := NewAntsServer(redis, poolSize)
	if err != nil {
		return TestResult{
			SuccessRate: 0,
			Errors:      map[string]int{"创建服务器失败": 1},
		}
	}

	// 启动服务器
	serverReady := make(chan bool)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Ants服务器panic: %v\n", r)
			}
		}()

		// 通知服务器已准备好
		go func() {
			time.Sleep(50 * time.Millisecond)
			serverReady <- true
		}()

		server.Start(":6431")
	}()

	// 等待服务器启动
	<-serverReady
	time.Sleep(50 * time.Millisecond)

	// 现在多个客户端连接这个单一Ants服务器实例
	result := runConcurrentClients(":6431", connections, commandsPerConn)

	// 停止服务器
	server.Stop()

	return result
}

// 运行并发客户端连接同一个服务器
func runConcurrentClients(serverAddr string, connections, commandsPerConn int) TestResult {
	var wg sync.WaitGroup
	results := make(chan ClientResult, connections)

	start := time.Now()

	// 启动多个客户端goroutine，都连接同一个服务器
	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			result := runSingleClient(serverAddr, clientID, commandsPerConn)
			results <- result
		}(i)
	}

	wg.Wait()
	close(results)

	duration := time.Since(start)

	// 统计结果
	var (
		successCount  = 0
		totalCommands = 0
		errors        = make(map[string]int)
	)

	for result := range results {
		if result.Success {
			successCount++
		} else {
			errors[result.Error]++
		}
		totalCommands += result.CommandsExecuted
	}

	successRate := float64(successCount) / float64(connections) * 100
	qps := float64(totalCommands*2) / duration.Seconds() // SET + GET

	return TestResult{
		SuccessCount: successCount,
		TotalCount:   connections,
		SuccessRate:  successRate,
		Duration:     duration,
		QPS:          qps,
		Errors:       errors,
	}
}

// 客户端结果
type ClientResult struct {
	ClientID         int
	Success          bool
	Error            string
	CommandsExecuted int
	Duration         time.Duration
}

// 运行单个客户端
func runSingleClient(serverAddr string, clientID, commandsPerConn int) ClientResult {
	result := ClientResult{
		ClientID: clientID,
		Success:  false,
	}

	start := time.Now()
	defer func() {
		result.Duration = time.Since(start)
	}()

	// 连接到服务器
	conn, err := net.DialTimeout("tcp", "localhost"+serverAddr, 3*time.Second)
	if err != nil {
		result.Error = fmt.Sprintf("连接失败: %v", err)
		return result
	}
	defer conn.Close()

	// 设置超时
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// 执行命令
	for i := 0; i < commandsPerConn; i++ {
		key := fmt.Sprintf("client_%d_cmd_%d", clientID, i)
		value := fmt.Sprintf("value_%d_%d", clientID, i)

		// SET命令
		setCmd := fmt.Sprintf("SET %s %s\n", key, value)
		_, err := conn.Write([]byte(setCmd))
		if err != nil {
			result.Error = fmt.Sprintf("SET写入失败: %v", err)
			return result
		}

		// 读取SET响应
		buffer := make([]byte, 1024)
		_, err = conn.Read(buffer)
		if err != nil {
			result.Error = fmt.Sprintf("SET响应读取失败: %v", err)
			return result
		}

		// GET命令
		getCmd := fmt.Sprintf("GET %s\n", key)
		_, err = conn.Write([]byte(getCmd))
		if err != nil {
			result.Error = fmt.Sprintf("GET写入失败: %v", err)
			return result
		}

		// 读取GET响应
		_, err = conn.Read(buffer)
		if err != nil {
			result.Error = fmt.Sprintf("GET响应读取失败: %v", err)
			return result
		}

		result.CommandsExecuted++
	}

	result.Success = true
	return result
}

// 压力测试：逐步增加并发
func TestGradualLoad(t *testing.T) {
	fmt.Println("\n=== 逐步增加负载测试 ===")

	loads := []int{50, 100, 200, 300, 500, 750, 1000}

	fmt.Printf("🔄 传统服务器负载测试:\n")
	testGradualLoadForServer("传统", loads, func(load int) TestResult {
		return testSingleTraditionalServer(load, 2)
	})

	time.Sleep(1 * time.Second)

	fmt.Printf("\n🔄 Ants服务器负载测试:\n")
	testGradualLoadForServer("Ants", loads, func(load int) TestResult {
		return testSingleAntsServer(load, 2, load/2)
	})
}

func testGradualLoadForServer(serverType string, loads []int, testFunc func(int) TestResult) {
	for _, load := range loads {
		fmt.Printf("   测试 %d 并发连接...", load)
		result := testFunc(load)

		if result.SuccessRate >= 95 {
			fmt.Printf(" ✅ 成功率 %.1f%%, QPS %.0f\n", result.SuccessRate, result.QPS)
		} else if result.SuccessRate >= 80 {
			fmt.Printf(" ⚠️  成功率 %.1f%%, QPS %.0f\n", result.SuccessRate, result.QPS)
		} else {
			fmt.Printf(" ❌ 成功率 %.1f%%, 开始出现明显问题\n", result.SuccessRate)
			break
		}

		time.Sleep(200 * time.Millisecond)
	}
}
