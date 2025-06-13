package simple_redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// Client Redis 客户端
type Client struct {
	conn net.Conn
}

// NewClient 创建新的客户端
func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

// SendCommand 发送命令
func (c *Client) SendCommand(command string) (string, error) {
	_, err := c.conn.Write([]byte(command + "\n"))
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(c.conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	response = strings.TrimSpace(response)

	// 处理 RESP 协议的批量字符串响应
	if strings.HasPrefix(response, "$") && response != "$-1" {
		// 读取实际的值（下一行）
		value, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		// 返回完整的响应：长度行+值行
		return response + "\n" + strings.TrimSpace(value), nil
	}

	return response, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.conn.Close()
}

// Interactive 交互式客户端
func (c *Client) Interactive() {
	fmt.Println("Simple Redis Client")
	fmt.Println("Type 'quit' to exit")
	fmt.Println("Available commands: SET, GET, HSET, HGET, HDEL, EXPIRE, TTL, BGSAVE, BGREWRITEAOF, PING")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("redis> ")
		if !scanner.Scan() {
			break
		}

		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		if strings.ToLower(command) == "quit" {
			break
		}

		response, err := c.SendCommand(command)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		fmt.Println(response)
	}
}
