package main

import (
	"fmt"
	"sync"
	"time"
)

// Direction 电梯运行方向
type Direction int

const (
	Up Direction = iota
	Down
	Idle
)

func (d Direction) String() string {
	switch d {
	case Up:
		return "上行"
	case Down:
		return "下行"
	default:
		return "空闲"
	}
}

// Request 电梯请求
type Request struct {
	ID        int       // 请求ID
	Floor     int       // 请求楼层
	Direction Direction // 请求方向
	Timestamp time.Time // 请求时间
}

// Elevator 电梯结构
type Elevator struct {
	ID           int          // 电梯ID
	CurrentFloor int          // 当前楼层
	Direction    Direction    // 当前方向
	Requests     []Request    // 请求队列
	IsMoving     bool         // 是否在移动
	mutex        sync.Mutex   // 保护电梯状态的互斥锁
	requestChan  chan Request // 接收请求的通道
	stopChan     chan bool    // 停止信号
}

// NewElevator 创建新电梯
func NewElevator(id int) *Elevator {
	return &Elevator{
		ID:           id,
		CurrentFloor: 1,
		Direction:    Idle,
		Requests:     make([]Request, 0),
		IsMoving:     false,
		requestChan:  make(chan Request, 100),
		stopChan:     make(chan bool),
	}
}

// AddRequest 添加请求到电梯
func (e *Elevator) AddRequest(req Request) {
	e.requestChan <- req
}

// Run 电梯运行主循环
func (e *Elevator) Run() {
	fmt.Printf("电梯 %d 开始运行，当前在 %d 楼\n", e.ID, e.CurrentFloor)

	for {
		select {
		case req := <-e.requestChan:
			e.mutex.Lock()
			e.Requests = append(e.Requests, req)
			fmt.Printf("🔔 电梯 %d 收到请求 #%d：%d楼 %s\n", e.ID, req.ID, req.Floor, req.Direction)
			e.mutex.Unlock()

		case <-e.stopChan:
			fmt.Printf("电梯 %d 停止运行\n", e.ID)
			return

		default:
			e.processRequests()
			time.Sleep(500 * time.Millisecond) // 模拟电梯运行间隔
		}
	}
}

// processRequests 处理请求队列
func (e *Elevator) processRequests() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if len(e.Requests) == 0 {
		e.Direction = Idle
		e.IsMoving = false
		return
	}

	fmt.Printf("🧠 开始调度算法分析...\n")
	fmt.Printf("   当前位置: %d楼, 当前方向: %s\n", e.CurrentFloor, e.Direction)
	e.printRequestQueue()

	// 选择下一个目标楼层
	targetFloor := e.selectNextFloor()
	if targetFloor == -1 {
		return
	}

	fmt.Printf("   📍 调度决策: 前往 %d楼\n", targetFloor)

	// 移动到目标楼层
	e.moveToFloor(targetFloor)

	// 移除已到达的请求
	e.removeCompletedRequests(targetFloor)
}

// printRequestQueue 打印当前请求队列
func (e *Elevator) printRequestQueue() {
	if len(e.Requests) == 0 {
		return
	}

	fmt.Printf("   📋 当前请求队列: ")
	for i, req := range e.Requests {
		if i > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("#%d(%d楼%s)", req.ID, req.Floor, req.Direction)
	}
	fmt.Println()
}

// selectNextFloor 选择下一个目标楼层（SCAN算法）
func (e *Elevator) selectNextFloor() int {
	if len(e.Requests) == 0 {
		return -1
	}

	// 如果电梯空闲，选择最近的请求
	if e.Direction == Idle {
		fmt.Printf("   🎯 电梯空闲，选择最近的请求\n")
		minDistance := 999
		targetFloor := -1

		for _, req := range e.Requests {
			distance := abs(req.Floor - e.CurrentFloor)
			fmt.Printf("      - %d楼距离: %d\n", req.Floor, distance)
			if distance < minDistance {
				minDistance = distance
				targetFloor = req.Floor
			}
		}

		// 设置电梯方向
		if targetFloor > e.CurrentFloor {
			e.Direction = Up
			fmt.Printf("   ⬆️ 设置方向: 上行\n")
		} else if targetFloor < e.CurrentFloor {
			e.Direction = Down
			fmt.Printf("   ⬇️ 设置方向: 下行\n")
		}

		return targetFloor
	}

	// SCAN算法：继续当前方向，直到没有请求
	fmt.Printf("   🔄 使用SCAN算法，当前方向: %s\n", e.Direction)

	var sameDirection []int
	var oppositeDirection []int

	if e.Direction == Up {
		fmt.Printf("   📊 分析上行请求:\n")
		for _, req := range e.Requests {
			if req.Floor >= e.CurrentFloor {
				sameDirection = append(sameDirection, req.Floor)
				fmt.Printf("      - %d楼 (同方向)\n", req.Floor)
			} else {
				oppositeDirection = append(oppositeDirection, req.Floor)
				fmt.Printf("      - %d楼 (反方向)\n", req.Floor)
			}
		}

		if len(sameDirection) == 0 {
			fmt.Printf("   🔄 没有上行请求，改变方向为下行\n")
			e.Direction = Down
			if len(oppositeDirection) > 0 {
				// 选择最高的下行楼层
				maxFloor := -1
				for _, floor := range oppositeDirection {
					if floor > maxFloor {
						maxFloor = floor
					}
				}
				fmt.Printf("   ✅ 选择最高的下行楼层: %d\n", maxFloor)
				return maxFloor
			}
		} else {
			// 选择最近的上行楼层
			minFloor := 999
			for _, floor := range sameDirection {
				if floor >= e.CurrentFloor && floor < minFloor {
					minFloor = floor
				}
			}
			fmt.Printf("   ✅ 选择最近的上行楼层: %d\n", minFloor)
			return minFloor
		}
	} else if e.Direction == Down {
		fmt.Printf("   📊 分析下行请求:\n")
		for _, req := range e.Requests {
			if req.Floor <= e.CurrentFloor {
				sameDirection = append(sameDirection, req.Floor)
				fmt.Printf("      - %d楼 (同方向)\n", req.Floor)
			} else {
				oppositeDirection = append(oppositeDirection, req.Floor)
				fmt.Printf("      - %d楼 (反方向)\n", req.Floor)
			}
		}

		if len(sameDirection) == 0 {
			fmt.Printf("   🔄 没有下行请求，改变方向为上行\n")
			e.Direction = Up
			if len(oppositeDirection) > 0 {
				// 选择最低的上行楼层
				minFloor := 999
				for _, floor := range oppositeDirection {
					if floor < minFloor {
						minFloor = floor
					}
				}
				fmt.Printf("   ✅ 选择最低的上行楼层: %d\n", minFloor)
				return minFloor
			}
		} else {
			// 选择最近的下行楼层
			maxFloor := -1
			for _, floor := range sameDirection {
				if floor <= e.CurrentFloor && floor > maxFloor {
					maxFloor = floor
				}
			}
			fmt.Printf("   ✅ 选择最近的下行楼层: %d\n", maxFloor)
			return maxFloor
		}
	}

	return -1
}

// moveToFloor 移动到指定楼层
func (e *Elevator) moveToFloor(targetFloor int) {
	if targetFloor == e.CurrentFloor {
		return
	}

	e.IsMoving = true
	fmt.Printf("电梯 %d 从 %d楼 %s 到 %d楼\n", e.ID, e.CurrentFloor, e.Direction, targetFloor)

	// 模拟电梯移动
	for e.CurrentFloor != targetFloor {
		time.Sleep(1 * time.Second) // 模拟每层楼1秒

		if e.CurrentFloor < targetFloor {
			e.CurrentFloor++
		} else {
			e.CurrentFloor--
		}

		fmt.Printf("电梯 %d 到达 %d楼\n", e.ID, e.CurrentFloor)
	}

	e.IsMoving = false
	fmt.Printf("电梯 %d 在 %d楼 开门\n", e.ID, e.CurrentFloor)
	time.Sleep(2 * time.Second) // 模拟开门关门时间
}

// removeCompletedRequests 移除已完成的请求
func (e *Elevator) removeCompletedRequests(floor int) {
	var remainingRequests []Request

	for _, req := range e.Requests {
		if req.Floor != floor {
			remainingRequests = append(remainingRequests, req)
		} else {
			fmt.Printf("电梯 %d 完成请求：%d楼 %s\n", e.ID, req.Floor, req.Direction)
		}
	}

	e.Requests = remainingRequests
}

// Stop 停止电梯
func (e *Elevator) Stop() {
	e.stopChan <- true
}

// ElevatorScheduler 电梯调度器
type ElevatorScheduler struct {
	Elevators   []*Elevator
	RequestChan chan Request
	mutex       sync.Mutex
}

// NewElevatorScheduler 创建电梯调度器
func NewElevatorScheduler(numElevators int) *ElevatorScheduler {
	scheduler := &ElevatorScheduler{
		Elevators:   make([]*Elevator, numElevators),
		RequestChan: make(chan Request, 1000),
	}

	// 创建电梯
	for i := 0; i < numElevators; i++ {
		scheduler.Elevators[i] = NewElevator(i + 1)
	}

	return scheduler
}

// NewSimpleElevatorScheduler 创建简化的单电梯调度器
func NewSimpleElevatorScheduler() *ElevatorScheduler {
	return NewElevatorScheduler(1) // 只创建1部电梯
}

// Start 启动调度器
func (s *ElevatorScheduler) Start() {
	// 启动所有电梯
	for _, elevator := range s.Elevators {
		go elevator.Run()
	}

	// 启动请求分发器
	go s.dispatchRequests()
}

// dispatchRequests 分发请求到最合适的电梯
func (s *ElevatorScheduler) dispatchRequests() {
	for req := range s.RequestChan {
		bestElevator := s.selectBestElevator(req)
		if bestElevator != nil {
			bestElevator.AddRequest(req)
		}
	}
}

// selectBestElevator 选择最合适的电梯
func (s *ElevatorScheduler) selectBestElevator(req Request) *Elevator {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var bestElevator *Elevator
	minCost := 999999

	for _, elevator := range s.Elevators {
		elevator.mutex.Lock()
		cost := s.calculateCost(elevator, req)
		elevator.mutex.Unlock()

		if cost < minCost {
			minCost = cost
			bestElevator = elevator
		}
	}

	return bestElevator
}

// calculateCost 计算电梯服务请求的成本
func (s *ElevatorScheduler) calculateCost(elevator *Elevator, req Request) int {
	// 基础距离成本
	distance := abs(elevator.CurrentFloor - req.Floor)
	cost := distance * 10

	// 如果电梯正在移动且方向不一致，增加成本
	if elevator.IsMoving {
		if (elevator.Direction == Up && req.Floor < elevator.CurrentFloor) ||
			(elevator.Direction == Down && req.Floor > elevator.CurrentFloor) {
			cost += 50 // 方向不一致的惩罚
		}
	}

	// 请求队列长度成本
	cost += len(elevator.Requests) * 5

	return cost
}

// CallElevator 呼叫电梯
func (s *ElevatorScheduler) CallElevator(floor int, direction Direction) {
	// 生成请求ID
	s.mutex.Lock()
	requestID := len(s.Elevators[0].Requests) + 1
	s.mutex.Unlock()

	req := Request{
		ID:        requestID,
		Floor:     floor,
		Direction: direction,
		Timestamp: time.Now(),
	}

	fmt.Printf("🔔 有人在 %d楼 按了 %s 按钮\n", floor, direction)
	s.RequestChan <- req
}

// Stop 停止所有电梯
func (s *ElevatorScheduler) Stop() {
	for _, elevator := range s.Elevators {
		elevator.Stop()
	}
	close(s.RequestChan)
}

// abs 绝对值函数
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// 模拟小明和其他人使用电梯的场景
func simulateElevatorUsage(scheduler *ElevatorScheduler) {
	// 小明在1楼，要上楼，按了两个电梯的按钮
	fmt.Println("\n=== 小明要回家上楼 ===")
	scheduler.CallElevator(1, Up)
	scheduler.CallElevator(1, Up) // 小明按了两次（两个电梯按钮）

	time.Sleep(2 * time.Second)

	// 其他楼层的人也在使用电梯
	fmt.Println("\n=== 其他楼层的人也在使用电梯 ===")

	go func() {
		time.Sleep(3 * time.Second)
		scheduler.CallElevator(5, Down) // 5楼有人要下楼

		time.Sleep(2 * time.Second)
		scheduler.CallElevator(8, Up) // 8楼有人要上楼

		time.Sleep(3 * time.Second)
		scheduler.CallElevator(3, Down) // 3楼有人要下楼

		time.Sleep(4 * time.Second)
		scheduler.CallElevator(10, Down) // 10楼有人要下楼

		time.Sleep(2 * time.Second)
		scheduler.CallElevator(6, Up) // 6楼有人要上楼
	}()
}

func main() {
	fmt.Println("🏢 简化电梯调度系统启动")
	fmt.Println("📋 系统配置：1部电梯，10层楼")
	fmt.Println("🔧 调度算法：SCAN算法（电梯算法）")
	fmt.Println("🎯 重点展示：调度决策过程")
	fmt.Println("====================================================")

	// 创建有1部电梯的调度系统
	scheduler := NewElevatorScheduler(1)

	// 启动调度器
	scheduler.Start()

	// 等待系统初始化
	time.Sleep(1 * time.Second)

	// 模拟电梯使用场景
	simulateElevatorUsage(scheduler)

	// 运行25秒后停止
	time.Sleep(25 * time.Second)

	fmt.Println("\n🛑 系统停止运行")
	scheduler.Stop()
}
