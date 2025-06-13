# Simple Redis

一个用 Go 语言实现的简易版本 Redis，用于学习和对比原生 Go map 的性能。

## 功能特性

### 1. 单机版本
- 基于 TCP 协议的服务器
- 支持多客户端并发连接
- 简单的命令行协议

### 2. 数据结构支持
- **字符串 (String)**: 基本的键值对存储
- **哈希表 (Hash)**: 支持字段-值的映射存储

### 3. 过期机制
- 支持为键设置过期时间 (EXPIRE 命令)
- 自动清理过期键
- TTL 命令查看剩余生存时间

### 4. 持久化
- **RDB**: 快照持久化，保存当前数据状态
- **AOF**: 追加式文件，记录所有写命令

## 支持的命令

### 字符串操作
- `SET key value` - 设置键值对
- `GET key` - 获取键的值

### 哈希表操作
- `HSET key field value` - 设置哈希表字段
- `HGET key field` - 获取哈希表字段值
- `HDEL key field` - 删除哈希表字段

### 过期操作
- `EXPIRE key seconds` - 设置键的过期时间
- `TTL key` - 获取键的剩余生存时间

### 持久化操作
- `BGSAVE` - 后台保存 RDB 快照
- `BGREWRITEAOF` - 后台重写 AOF 文件

### 其他
- `PING` - 测试连接
- `QUIT` - 退出连接

## 使用方法

### 启动服务器
```bash
cd simple_redis
go run *.go
```

服务器将在 `:6379` 端口启动。

### 运行测试客户端
修改 `main.go`，取消注释客户端测试代码：
```go
func main() {
    // 启动测试客户端而不是服务器
    TestClient()
}
```

### 性能测试
```bash
# 运行基准测试
go test -bench=.

# 运行性能对比测试
go test -v -run=TestPerformanceComparison
```

### 使用 telnet 连接
```bash
telnet localhost 6379
```

然后可以输入命令：
```
SET name John
GET name
HSET user:1 name Alice
HGET user:1 name
EXPIRE name 10
TTL name
PING
```

## 项目结构

```
simple_redis/
├── main.go              # 主程序入口
├── redis.go             # Redis 核心实现
├── server.go            # TCP 服务器
├── client.go            # 测试客户端
├── benchmark_test.go    # 性能测试
├── go.mod              # Go 模块文件
└── README.md           # 项目说明
```

## 与原生 Go map 的对比

### 原生 Go map 优势：
- 性能极高，直接内存访问
- 内存占用少
- 简单易用

### Simple Redis 优势：
- **网络访问**: 支持远程客户端连接
- **持久化**: 数据可以保存到磁盘
- **过期机制**: 自动清理过期数据
- **并发安全**: 内置读写锁保护
- **扩展性**: 易于添加新功能

### 性能差异：
根据基准测试，Simple Redis 比原生 map 慢约 2-5 倍，这是因为：
1. 额外的锁开销
2. 过期检查逻辑
3. AOF 日志写入
4. 网络协议处理

## 实现细节

### 数据存储
- 使用 `map[string]*Value` 存储键值对
- 使用 `map[string]Hash` 存储哈希表
- 每个值包含数据和可选的过期时间

### 并发控制
- 使用 `sync.RWMutex` 保护数据结构
- 读操作使用读锁，写操作使用写锁

### 过期机制
- 惰性删除：访问时检查是否过期
- 定期删除：每秒扫描并清理过期键

### 持久化
- **RDB**: 使用 `encoding/gob` 序列化整个数据结构
- **AOF**: 记录每个写命令到文件

## 限制和简化

为了保持简单，本实现有以下限制：
1. 不支持 Redis 的所有数据类型（如 List、Set、ZSet）
2. 不支持事务
3. 不支持发布/订阅
4. 不支持集群模式
5. AOF 重放功能未完全实现
6. 没有内存管理和驱逐策略

## 扩展建议

可以考虑以下扩展：
1. 添加更多数据类型支持
2. 实现 Redis 协议 (RESP)
3. 添加配置文件支持
4. 实现事务功能
5. 添加监控和统计功能
6. 支持 Lua 脚本

## 许可证

MIT License 