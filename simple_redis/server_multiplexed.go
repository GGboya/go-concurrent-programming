package simple_redis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MultiplexedServer 使用IO多路复用的Redis服务器
type MultiplexedServer struct {
	redis       *Redis
	connections map[net.Conn]*ClientConnection
	connMutex   sync.RWMutex
	eventLoop   *EventLoop
}

// ClientConnection 表示客户端连接状态
type ClientConnection struct {
	conn       net.Conn
	reader     *bufio.Reader
	writeQueue chan string
	closed     bool
	mutex      sync.Mutex
}

// EventLoop 事件循环
type EventLoop struct {
	connections chan net.Conn
	events      chan *Event
	quit        chan bool
}

// Event 表示一个IO事件
type Event struct {
	conn      net.Conn
	eventType EventType
	data      []byte
}

// EventType 事件类型
type EventType int

const (
	EventRead EventType = iota
	EventWrite
	EventClose
)

// NewMultiplexedServer 创建新的多路复用服务器
func NewMultiplexedServer(redis *Redis) *MultiplexedServer {
	return &MultiplexedServer{
		redis:       redis,
		connections: make(map[net.Conn]*ClientConnection),
		eventLoop: &EventLoop{
			connections: make(chan net.Conn, 1000),
			events:      make(chan *Event, 10000),
			quit:        make(chan bool),
		},
	}
}

// Start 启动多路复用服务器
func (s *MultiplexedServer) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("多路复用Redis服务器启动在 %s\n", addr)
	fmt.Println("使用IO多路复用处理并发连接...")

	// 启动事件循环
	go s.runEventLoop()

	// 接受连接
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// 将新连接发送到事件循环
		select {
		case s.eventLoop.connections <- conn:
		default:
			conn.Close() // 如果队列满了，关闭连接
		}
	}
}

// runEventLoop 运行事件循环 - 这是IO多路复用的核心
func (s *MultiplexedServer) runEventLoop() {
	fmt.Println("事件循环启动...")

	for {
		select {
		case conn := <-s.eventLoop.connections:
			// 新连接
			s.handleNewConnection(conn)

		case event := <-s.eventLoop.events:
			// 处理IO事件
			s.handleEvent(event)

		case <-s.eventLoop.quit:
			// 退出事件循环
			return
		}
	}
}

// handleNewConnection 处理新连接
func (s *MultiplexedServer) handleNewConnection(conn net.Conn) {
	fmt.Printf("新连接: %s\n", conn.RemoteAddr())

	clientConn := &ClientConnection{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		writeQueue: make(chan string, 100),
		closed:     false,
	}

	s.connMutex.Lock()
	s.connections[conn] = clientConn
	s.connMutex.Unlock()

	// 启动读取goroutine
	go s.readFromConnection(clientConn)

	// 启动写入goroutine
	go s.writeToConnection(clientConn)
}

// readFromConnection 从连接读取数据
func (s *MultiplexedServer) readFromConnection(clientConn *ClientConnection) {
	defer s.closeConnection(clientConn.conn)

	for {
		// 设置读取超时
		clientConn.conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		line, err := clientConn.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Printf("客户端断开连接: %s\n", clientConn.conn.RemoteAddr())
			} else {
				fmt.Printf("读取错误: %v\n", err)
			}
			return
		}

		// 发送读取事件到事件循环
		event := &Event{
			conn:      clientConn.conn,
			eventType: EventRead,
			data:      []byte(strings.TrimSpace(line)),
		}

		select {
		case s.eventLoop.events <- event:
		default:
			// 事件队列满了，丢弃事件
			fmt.Println("事件队列满，丢弃事件")
		}
	}
}

// writeToConnection 向连接写入数据
func (s *MultiplexedServer) writeToConnection(clientConn *ClientConnection) {
	defer s.closeConnection(clientConn.conn)

	for response := range clientConn.writeQueue {
		clientConn.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		_, err := clientConn.conn.Write([]byte(response + "\n"))
		if err != nil {
			fmt.Printf("写入错误: %v\n", err)
			return
		}
	}
}

// handleEvent 处理IO事件
func (s *MultiplexedServer) handleEvent(event *Event) {
	switch event.eventType {
	case EventRead:
		s.handleReadEvent(event)
	case EventClose:
		s.closeConnection(event.conn)
	}
}

// handleReadEvent 处理读取事件
func (s *MultiplexedServer) handleReadEvent(event *Event) {
	command := string(event.data)
	if command == "" {
		return
	}

	// 处理命令（复用原有的命令处理逻辑）
	response := s.processCommand(command)

	// 将响应发送到写入队列
	s.connMutex.RLock()
	clientConn, exists := s.connections[event.conn]
	s.connMutex.RUnlock()

	if exists && !clientConn.closed {
		select {
		case clientConn.writeQueue <- response:
		default:
			// 写入队列满了，关闭连接
			s.closeConnection(event.conn)
		}
	}
}

// closeConnection 关闭连接
func (s *MultiplexedServer) closeConnection(conn net.Conn) {
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	if clientConn, exists := s.connections[conn]; exists {
		clientConn.mutex.Lock()
		if !clientConn.closed {
			clientConn.closed = true
			close(clientConn.writeQueue)
			conn.Close()
			fmt.Printf("连接已关闭: %s\n", conn.RemoteAddr())
		}
		clientConn.mutex.Unlock()
		delete(s.connections, conn)
	}
}

// GetConnectionCount 获取当前连接数
func (s *MultiplexedServer) GetConnectionCount() int {
	s.connMutex.RLock()
	defer s.connMutex.RUnlock()
	return len(s.connections)
}

// processCommand 处理命令（复用原有逻辑）
func (s *MultiplexedServer) processCommand(command string) string {
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
	case "INFO":
		return s.handleInfo()
	case "QUIT":
		return "+OK"
	default:
		return fmt.Sprintf("-ERR unknown command '%s'", cmd)
	}
}

// 以下是命令处理函数（复用原有逻辑）
func (s *MultiplexedServer) handleSet(parts []string) string {
	if len(parts) < 3 {
		return "-ERR wrong number of arguments for 'set' command"
	}
	key := parts[1]
	value := strings.Join(parts[2:], " ")
	s.redis.Set(key, value)
	return "+OK"
}

func (s *MultiplexedServer) handleGet(parts []string) string {
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

func (s *MultiplexedServer) handleHSet(parts []string) string {
	if len(parts) < 4 {
		return "-ERR wrong number of arguments for 'hset' command"
	}
	key := parts[1]
	field := parts[2]
	value := strings.Join(parts[3:], " ")
	s.redis.HSet(key, field, value)
	return ":1"
}

func (s *MultiplexedServer) handleHGet(parts []string) string {
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

func (s *MultiplexedServer) handleHDel(parts []string) string {
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

func (s *MultiplexedServer) handleExpire(parts []string) string {
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

func (s *MultiplexedServer) handleTTL(parts []string) string {
	if len(parts) != 2 {
		return "-ERR wrong number of arguments for 'ttl' command"
	}
	key := parts[1]
	ttl, exists := s.redis.TTL(key)
	if !exists {
		return ":-2"
	}
	if ttl == -1 {
		return ":-1"
	}
	return fmt.Sprintf(":%d", int(ttl.Seconds()))
}

func (s *MultiplexedServer) handleBGSave() string {
	go s.redis.SaveRDB()
	return "+Background saving started"
}

func (s *MultiplexedServer) handleBGRewriteAOF() string {
	if s.redis.aofEnabled {
		go s.redis.RewriteAOF()
		return "+Background append only file rewriting started"
	}
	return "+AOF is not enabled"
}

func (s *MultiplexedServer) handleLastSave() string {
	changes, timeSince := s.redis.GetSaveStats()
	return fmt.Sprintf("*4\r\n$9\r\nlastsave\r\n:%d\r\n$7\r\nchanges\r\n:%d",
		time.Now().Add(-timeSince).Unix(), changes)
}

func (s *MultiplexedServer) handleInfo() string {
	connCount := s.GetConnectionCount()
	changes, timeSince := s.redis.GetSaveStats()

	info := fmt.Sprintf(`# Server
redis_version:simple-redis-1.0
multiplexed:yes
connected_clients:%d

# Persistence  
rdb_changes_since_last_save:%d
rdb_last_save_time:%d
aof_enabled:%t

# Stats
total_connections_received:%d
total_commands_processed:unknown
keyspace_hits:unknown
keyspace_misses:unknown`,
		connCount,
		changes,
		time.Now().Add(-timeSince).Unix(),
		s.redis.aofEnabled,
		connCount)

	response := fmt.Sprintf("$%d\r\n%s", len(info), info)

	return response
}
