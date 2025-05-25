# Go 并发编程实践项目

收录一些 Go 并发编程的实际应用案例和解决方案。

## 📁 项目列表

### 🏢 [电梯调度系统](./elevator_scheduler/)
**问题场景**：小明对电梯的调度策略很感兴趣。当小明要回家上楼的时候，他会把电梯按钮按一下。与此同时，可能其他楼层也会按电梯，那电梯到底是怎么调度的呢？

**🎯 简化版本（推荐学习）**：
- 🔧 **1部电梯** + **SCAN算法**详细展示
- 📊 **调度决策过程**完整输出
- 🧠 **算法思路**清晰可见
- 📚 **学习友好**，专注核心算法

**🚀 完整版本**：
- 🔧 **多电梯并发**运行 + 智能成本分配
- 📊 **实时监控**和性能统计
- 🎯 **优先级调度**（高级版本）
- 👥 **载客量管理**（高级版本）

**并发技术**：
- Goroutine 并发处理
- Channel 通信机制
- Mutex/RWMutex 同步控制
- Select 多路复用

**运行方式**：
```bash
# 简化版本（重点展示SCAN算法）
cd elevator_scheduler && go run main.go

# 高级版本（完整功能）
cd elevator_scheduler/advanced && go run main.go
```

### 🔄 [Goroutine 池](./goroutine_pool/)
工作池模式的实现和应用

### 📖 [读写锁应用](./reader_writer/)
读写锁在并发场景中的使用

### 🚦 [限流器](./rate_limiter/)
并发限流的实现策略

## 🎯 学习目标

通过这些实际项目，你将学会：

1. **并发编程模式**：生产者-消费者、工作池、发布-订阅
2. **同步原语使用**：Mutex、RWMutex、Channel、Select、WaitGroup
3. **算法实现**：调度算法、负载均衡、资源分配
4. **系统设计**：模块化、可扩展、高并发架构
5. **实际问题建模**：将现实问题抽象为程序模型

## 🚀 快速开始

```bash
git clone https://github.com/GGboya/go-concurrent-programming.git
cd go-concurrent-programming
```

每个项目都有独立的README文档，详细说明了实现原理和使用方法。

## 📚 推荐阅读顺序

1. **电梯调度系统** - 综合性强，涵盖多种并发技术
2. **Goroutine 池** - 理解工作池模式
3. **读写锁应用** - 掌握读写分离
4. **限流器** - 学习流量控制

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进这些项目！
