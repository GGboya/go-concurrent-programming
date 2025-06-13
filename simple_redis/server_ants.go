package simple_redis

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
)

// AntsServer 基于ants协程池的Redis服务器
type AntsServer struct {
	redis     *Redis
	pool      *ants.Pool
	listener  net.Listener
	mu        sync.RWMutex
	connCount int64
	isRunning bool
}

// ConnectionTask 连接处理任务
type ConnectionTask struct {
	conn   net.Conn
	server *AntsServer
}

// NewAntsServer 创建新的ants服务器
func NewAntsServer(redis *Redis, poolSize int) (*AntsServer, error) {
	server := &AntsServer{
		redis: redis,
	}

	// 创建协程池
	pool, err := ants.NewPool(poolSize, ants.WithOptions(ants.Options{
		ExpiryDuration:   time.Minute * 10, // 10分钟后回收空闲goroutine
		PreAlloc:         true,             // 预分配goroutine
		MaxBlockingTasks: poolSize * 2,     // 最大阻塞任务数
		Nonblocking:      false,            // 允许阻塞
		PanicHandler: func(i interface{}) {
			log.Printf("协程池panic: %v", i)
		},
	}))

	if err != nil {
		return nil, fmt.Errorf("创建协程池失败: %v", err)
	}

	server.pool = pool
	return server, nil
}

// Start 启动服务器
func (s *AntsServer) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("监听失败: %v", err)
	}

	s.listener = listener
	s.isRunning = true

	fmt.Printf("Ants Redis服务器启动在 %s\n", addr)
	fmt.Printf("协程池大小: %d\n", s.pool.Cap())
	fmt.Printf("使用协程池处理连接...\n")

	for s.isRunning {
		conn, err := listener.Accept()
		if err != nil {
			if s.isRunning {
				log.Printf("接受连接失败: %v", err)
			}
			continue
		}

		// 增加连接计数
		s.mu.Lock()
		s.connCount++
		connID := s.connCount
		s.mu.Unlock()

		fmt.Printf("新连接: %s (ID: %d)\n", conn.RemoteAddr(), connID)

		// 创建连接任务
		task := &ConnectionTask{
			conn:   conn,
			server: s,
		}

		// 提交任务到协程池
		err = s.pool.Submit(task.Handle)
		if err != nil {
			log.Printf("提交任务到协程池失败: %v", err)
			conn.Close()
			continue
		}
	}

	return nil
}

// Handle 处理连接的任务函数
func (task *ConnectionTask) Handle() {
	defer func() {
		task.conn.Close()
		fmt.Printf("连接已关闭: %s\n", task.conn.RemoteAddr())

		// 减少连接计数
		task.server.mu.Lock()
		task.server.connCount--
		task.server.mu.Unlock()
	}()

	scanner := bufio.NewScanner(task.conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// 解析并执行命令
		response := task.server.processCommand(line)

		// 发送响应
		_, err := task.conn.Write([]byte(response + "\n"))
		if err != nil {
			log.Printf("发送响应失败: %v", err)
			return
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("读取连接数据失败: %v", err)
	}
}

// processCommand 处理命令
func (s *AntsServer) processCommand(command string) string {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "-ERR empty command"
	}

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "SET":
		return s.handleSet(parts)
	case "GET":
		return s.handleGet(parts)
	case "HSET":
		return s.handleHSet(parts)
	case "HGET":
		return s.handleHGet(parts)
	case "HDEL":
		return s.handleHDel(parts)
	case "EXPIRE":
		return s.handleExpire(parts)
	case "TTL":
		return s.handleTTL(parts)
	case "BGSAVE":
		return s.handleBGSave()
	case "BGREWRITEAOF":
		return s.handleBGRewriteAOF()
	case "LASTSAVE":
		return s.handleLastSave()
	case "PING":
		return "+PONG"
	case "QUIT":
		return "+OK"
	default:
		return fmt.Sprintf("-ERR unknown command '%s'", cmd)
	}
}

// handleSet 处理 SET 命令
func (s *AntsServer) handleSet(parts []string) string {
	if len(parts) < 3 {
		return "-ERR wrong number of arguments for 'set' command"
	}

	key := parts[1]
	value := strings.Join(parts[2:], " ")

	s.redis.Set(key, value)
	return "+OK"
}

// handleGet 处理 GET 命令
func (s *AntsServer) handleGet(parts []string) string {
	if len(parts) != 2 {
		return "-ERR wrong number of arguments for 'get' command"
	}

	key := parts[1]
	value, exists := s.redis.Get(key)

	if !exists {
		return "$-1"
	}

	valueStr := fmt.Sprintf("%v", value)
	return fmt.Sprintf("$%d\r\n%s", len(valueStr), valueStr)
}

// handleHSet 处理 HSET 命令
func (s *AntsServer) handleHSet(parts []string) string {
	if len(parts) < 4 {
		return "-ERR wrong number of arguments for 'hset' command"
	}

	key := parts[1]
	field := parts[2]
	value := strings.Join(parts[3:], " ")

	s.redis.HSet(key, field, value)
	return ":1"
}

// handleHGet 处理 HGET 命令
func (s *AntsServer) handleHGet(parts []string) string {
	if len(parts) != 3 {
		return "-ERR wrong number of arguments for 'hget' command"
	}

	key := parts[1]
	field := parts[2]
	value, exists := s.redis.HGet(key, field)

	if !exists {
		return "$-1"
	}

	valueStr := fmt.Sprintf("%v", value)
	return fmt.Sprintf("$%d\r\n%s", len(valueStr), valueStr)
}

// handleHDel 处理 HDEL 命令
func (s *AntsServer) handleHDel(parts []string) string {
	if len(parts) != 3 {
		return "-ERR wrong number of arguments for 'hdel' command"
	}

	key := parts[1]
	field := parts[2]
	deleted := s.redis.HDel(key, field)

	if deleted {
		return ":1"
	}
	return ":0"
}

// handleExpire 处理 EXPIRE 命令
func (s *AntsServer) handleExpire(parts []string) string {
	if len(parts) != 3 {
		return "-ERR wrong number of arguments for 'expire' command"
	}

	key := parts[1]
	seconds, err := strconv.Atoi(parts[2])
	if err != nil {
		return "-ERR value is not an integer or out of range"
	}

	duration := time.Duration(seconds) * time.Second
	success := s.redis.Expire(key, duration)

	if success {
		return ":1"
	}
	return ":0"
}

// handleTTL 处理 TTL 命令
func (s *AntsServer) handleTTL(parts []string) string {
	if len(parts) != 2 {
		return "-ERR wrong number of arguments for 'ttl' command"
	}

	key := parts[1]
	ttl, exists := s.redis.TTL(key)

	if !exists {
		return ":-2" // 键不存在
	}

	if ttl == -1 {
		return ":-1" // 永不过期
	}

	return fmt.Sprintf(":%d", int64(ttl.Seconds()))
}

// handleBGSave 处理 BGSAVE 命令
func (s *AntsServer) handleBGSave() string {
	s.redis.BackgroundSave()
	return "+Background saving started"
}

// handleBGRewriteAOF 处理 BGREWRITEAOF 命令
func (s *AntsServer) handleBGRewriteAOF() string {
	s.redis.RewriteAOF()
	return "+Background append only file rewriting started"
}

// handleLastSave 处理 LASTSAVE 命令
func (s *AntsServer) handleLastSave() string {
	changes, duration := s.redis.GetSaveStats()
	return fmt.Sprintf("+上次保存: %.0f秒前, 当前修改数: %d", duration.Seconds(), changes)
}

// Stop 停止服务器
func (s *AntsServer) Stop() error {
	s.isRunning = false

	if s.listener != nil {
		s.listener.Close()
	}

	// 等待所有任务完成并关闭协程池
	s.pool.Release()

	fmt.Println("Ants服务器已停止")
	return nil
}

// GetStats 获取服务器统计信息
func (s *AntsServer) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"active_connections": s.connCount,
		"pool_capacity":      s.pool.Cap(),
		"pool_running":       s.pool.Running(),
		"pool_free":          s.pool.Free(),
		"pool_waiting":       s.pool.Waiting(),
	}
}

// GetConnectionCount 获取当前连接数
func (s *AntsServer) GetConnectionCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connCount
}
