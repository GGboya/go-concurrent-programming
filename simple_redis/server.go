package simple_redis

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Server 表示 Redis 服务器
type Server struct {
	redis *Redis
}

// NewServer 创建新的服务器实例
func NewServer(redis *Redis) *Server {
	return &Server{redis: redis}
}

// Start 启动服务器
func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go s.handleConnection(conn)
	}
}

// handleConnection 处理客户端连接
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		response := s.processCommand(command)
		conn.Write([]byte(response + "\n"))
	}
}

// processCommand 处理命令
func (s *Server) processCommand(command string) string {
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
func (s *Server) handleSet(parts []string) string {
	if len(parts) < 3 {
		return "-ERR wrong number of arguments for 'set' command"
	}

	key := parts[1]
	value := strings.Join(parts[2:], " ")

	s.redis.Set(key, value)
	return "+OK"
}

// handleGet 处理 GET 命令
func (s *Server) handleGet(parts []string) string {
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
func (s *Server) handleHSet(parts []string) string {
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
func (s *Server) handleHGet(parts []string) string {
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
func (s *Server) handleHDel(parts []string) string {
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
func (s *Server) handleExpire(parts []string) string {
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
func (s *Server) handleTTL(parts []string) string {
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
func (s *Server) handleBGSave() string {
	s.redis.BackgroundSave()
	return "+Background saving started"
}

// handleBGRewriteAOF 处理 BGREWRITEAOF 命令
func (s *Server) handleBGRewriteAOF() string {
	s.redis.RewriteAOF()
	return "+Background append only file rewriting started"
}

// handleLastSave 处理 LASTSAVE 命令
func (s *Server) handleLastSave() string {
	changes, duration := s.redis.GetSaveStats()
	return fmt.Sprintf("+上次保存: %.0f秒前, 当前修改数: %d", duration.Seconds(), changes)
}
