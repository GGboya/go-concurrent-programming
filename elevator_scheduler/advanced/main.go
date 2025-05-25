package main

import (
	"fmt"
	"sort"
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

// ElevatorState 电梯状态
type ElevatorState int

const (
	Moving ElevatorState = iota
	Stopped
	DoorsOpen
	Maintenance
)

func (s ElevatorState) String() string {
	switch s {
	case Moving:
		return "运行中"
	case Stopped:
		return "停止"
	case DoorsOpen:
		return "开门"
	case Maintenance:
		return "维护"
	default:
		return "未知"
	}
}

// Request 电梯请求
type Request struct {
	ID        int       // 请求ID
	Floor     int       // 请求楼层
	Direction Direction // 请求方向
	Timestamp time.Time // 请求时间
	Priority  int       // 优先级 (1-10, 10最高)
}

// Passenger 乘客信息
type Passenger struct {
	ID        int
	FromFloor int
	ToFloor   int
	WaitTime  time.Duration
	RideTime  time.Duration
	EnterTime time.Time
}

// AdvancedElevator 高级电梯结构
type AdvancedElevator struct {
	ID              int                 // 电梯ID
	CurrentFloor    int                 // 当前楼层
	Direction       Direction           // 当前方向
	State           ElevatorState       // 电梯状态
	Requests        []Request           // 请求队列
	Passengers      []Passenger         // 当前乘客
	MaxCapacity     int                 // 最大载客量
	CurrentLoad     int                 // 当前载客量
	TotalDistance   int                 // 总运行距离
	TotalRequests   int                 // 总服务请求数
	AverageWaitTime time.Duration       // 平均等待时间
	mutex           sync.RWMutex        // 读写锁
	requestChan     chan Request        // 接收请求的通道
	passengerChan   chan Passenger      // 接收乘客的通道
	stopChan        chan bool           // 停止信号
	statusChan      chan ElevatorStatus // 状态报告通道
}

// ElevatorStatus 电梯状态报告
type ElevatorStatus struct {
	ElevatorID   int
	CurrentFloor int
	Direction    Direction
	State        ElevatorState
	Load         int
	Capacity     int
	QueueLength  int
	Timestamp    time.Time
}

// NewAdvancedElevator 创建新的高级电梯
func NewAdvancedElevator(id int, capacity int) *AdvancedElevator {
	return &AdvancedElevator{
		ID:            id,
		CurrentFloor:  1,
		Direction:     Idle,
		State:         Stopped,
		Requests:      make([]Request, 0),
		Passengers:    make([]Passenger, 0),
		MaxCapacity:   capacity,
		CurrentLoad:   0,
		requestChan:   make(chan Request, 100),
		passengerChan: make(chan Passenger, 100),
		stopChan:      make(chan bool),
		statusChan:    make(chan ElevatorStatus, 10),
	}
}

// AddRequest 添加请求到电梯
func (e *AdvancedElevator) AddRequest(req Request) {
	e.requestChan <- req
}

// AddPassenger 添加乘客到电梯
func (e *AdvancedElevator) AddPassenger(passenger Passenger) {
	e.passengerChan <- passenger
}

// GetStatus 获取电梯状态
func (e *AdvancedElevator) GetStatus() ElevatorStatus {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return ElevatorStatus{
		ElevatorID:   e.ID,
		CurrentFloor: e.CurrentFloor,
		Direction:    e.Direction,
		State:        e.State,
		Load:         e.CurrentLoad,
		Capacity:     e.MaxCapacity,
		QueueLength:  len(e.Requests),
		Timestamp:    time.Now(),
	}
}

// Run 电梯运行主循环
func (e *AdvancedElevator) Run() {
	fmt.Printf("🚀 高级电梯 %d 开始运行，载客量: %d人，当前在 %d 楼\n", e.ID, e.MaxCapacity, e.CurrentFloor)

	// 定期发送状态报告
	statusTicker := time.NewTicker(8 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case req := <-e.requestChan:
			e.mutex.Lock()
			e.Requests = append(e.Requests, req)
			e.sortRequestsByPriority()
			fmt.Printf("🔔 电梯 %d 收到请求：%d楼 %s (优先级: %d)\n", e.ID, req.Floor, req.Direction, req.Priority)
			e.mutex.Unlock()

		case passenger := <-e.passengerChan:
			e.mutex.Lock()
			if e.CurrentLoad < e.MaxCapacity {
				e.Passengers = append(e.Passengers, passenger)
				e.CurrentLoad++
				fmt.Printf("👤 乘客 %d 进入电梯 %d (从%d楼到%d楼)\n", passenger.ID, e.ID, passenger.FromFloor, passenger.ToFloor)
			} else {
				fmt.Printf("⚠️ 电梯 %d 已满载，乘客 %d 无法进入\n", e.ID, passenger.ID)
			}
			e.mutex.Unlock()

		case <-statusTicker.C:
			status := e.GetStatus()
			select {
			case e.statusChan <- status:
			default:
				// 如果状态通道满了，跳过这次状态报告
			}

		case <-e.stopChan:
			fmt.Printf("🛑 电梯 %d 停止运行\n", e.ID)
			return

		default:
			e.processRequests()
			time.Sleep(300 * time.Millisecond)
		}
	}
}

// sortRequestsByPriority 按优先级排序请求
func (e *AdvancedElevator) sortRequestsByPriority() {
	sort.Slice(e.Requests, func(i, j int) bool {
		// 优先级高的在前，优先级相同的按时间排序
		if e.Requests[i].Priority == e.Requests[j].Priority {
			return e.Requests[i].Timestamp.Before(e.Requests[j].Timestamp)
		}
		return e.Requests[i].Priority > e.Requests[j].Priority
	})
}

// processRequests 处理请求队列
func (e *AdvancedElevator) processRequests() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if len(e.Requests) == 0 && len(e.Passengers) == 0 {
		e.Direction = Idle
		e.State = Stopped
		return
	}

	// 选择下一个目标楼层
	targetFloor := e.selectNextFloorAdvanced()
	if targetFloor == -1 {
		return
	}

	// 移动到目标楼层
	e.moveToFloorAdvanced(targetFloor)

	// 处理到达楼层的乘客和请求
	e.handleArrivals(targetFloor)
}

// selectNextFloorAdvanced 高级楼层选择算法
func (e *AdvancedElevator) selectNextFloorAdvanced() int {
	var candidates []int

	// 收集乘客目标楼层
	for _, passenger := range e.Passengers {
		candidates = append(candidates, passenger.ToFloor)
	}

	// 收集请求楼层
	for _, req := range e.Requests {
		candidates = append(candidates, req.Floor)
	}

	if len(candidates) == 0 {
		return -1
	}

	// 如果电梯空闲，选择最高优先级的请求
	if e.Direction == Idle {
		if len(e.Requests) > 0 {
			// 已经按优先级排序，选择第一个
			targetFloor := e.Requests[0].Floor
			if targetFloor > e.CurrentFloor {
				e.Direction = Up
			} else if targetFloor < e.CurrentFloor {
				e.Direction = Down
			}
			return targetFloor
		}

		// 没有外部请求，处理乘客
		if len(e.Passengers) > 0 {
			targetFloor := e.Passengers[0].ToFloor
			if targetFloor > e.CurrentFloor {
				e.Direction = Up
			} else if targetFloor < e.CurrentFloor {
				e.Direction = Down
			}
			return targetFloor
		}
	}

	// SCAN算法的改进版本
	var sameDirection []int
	var oppositeDirection []int

	for _, floor := range candidates {
		if e.Direction == Up {
			if floor >= e.CurrentFloor {
				sameDirection = append(sameDirection, floor)
			} else {
				oppositeDirection = append(oppositeDirection, floor)
			}
		} else if e.Direction == Down {
			if floor <= e.CurrentFloor {
				sameDirection = append(sameDirection, floor)
			} else {
				oppositeDirection = append(oppositeDirection, floor)
			}
		}
	}

	// 优先处理同方向的楼层
	if len(sameDirection) > 0 {
		if e.Direction == Up {
			sort.Ints(sameDirection)
			return sameDirection[0] // 最近的上行楼层
		} else {
			sort.Sort(sort.Reverse(sort.IntSlice(sameDirection)))
			return sameDirection[0] // 最近的下行楼层
		}
	}

	// 改变方向处理反方向的楼层
	if len(oppositeDirection) > 0 {
		if e.Direction == Up {
			e.Direction = Down
			sort.Sort(sort.Reverse(sort.IntSlice(oppositeDirection)))
			return oppositeDirection[0]
		} else {
			e.Direction = Up
			sort.Ints(oppositeDirection)
			return oppositeDirection[0]
		}
	}

	return -1
}

// moveToFloorAdvanced 高级移动到指定楼层
func (e *AdvancedElevator) moveToFloorAdvanced(targetFloor int) {
	if targetFloor == e.CurrentFloor {
		return
	}

	e.State = Moving
	distance := abs(targetFloor - e.CurrentFloor)
	e.TotalDistance += distance

	fmt.Printf("🚀 电梯 %d 从 %d楼 %s 到 %d楼 (载客: %d/%d)\n",
		e.ID, e.CurrentFloor, e.Direction, targetFloor, e.CurrentLoad, e.MaxCapacity)

	// 模拟电梯移动
	for e.CurrentFloor != targetFloor {
		time.Sleep(600 * time.Millisecond) // 稍微快一点的移动速度

		if e.CurrentFloor < targetFloor {
			e.CurrentFloor++
		} else {
			e.CurrentFloor--
		}

		fmt.Printf("📍 电梯 %d 到达 %d楼\n", e.ID, e.CurrentFloor)
	}

	e.State = DoorsOpen
	fmt.Printf("🚪 电梯 %d 在 %d楼 开门\n", e.ID, e.CurrentFloor)
	time.Sleep(1200 * time.Millisecond) // 开门关门时间
}

// handleArrivals 处理到达楼层的乘客和请求
func (e *AdvancedElevator) handleArrivals(floor int) {
	// 处理下车的乘客
	var remainingPassengers []Passenger
	for _, passenger := range e.Passengers {
		if passenger.ToFloor == floor {
			e.CurrentLoad--
			passenger.RideTime = time.Since(passenger.EnterTime)
			fmt.Printf("👋 乘客 %d 在 %d楼 下车 (乘坐时间: %.1f秒)\n",
				passenger.ID, floor, passenger.RideTime.Seconds())
		} else {
			remainingPassengers = append(remainingPassengers, passenger)
		}
	}
	e.Passengers = remainingPassengers

	// 处理完成的请求
	var remainingRequests []Request
	for _, req := range e.Requests {
		if req.Floor == floor {
			e.TotalRequests++
			waitTime := time.Since(req.Timestamp)
			e.updateAverageWaitTime(waitTime)
			fmt.Printf("✅ 电梯 %d 完成请求：%d楼 %s (等待时间: %.1f秒)\n",
				e.ID, req.Floor, req.Direction, waitTime.Seconds())
		} else {
			remainingRequests = append(remainingRequests, req)
		}
	}
	e.Requests = remainingRequests

	e.State = Stopped
}

// updateAverageWaitTime 更新平均等待时间
func (e *AdvancedElevator) updateAverageWaitTime(waitTime time.Duration) {
	if e.TotalRequests == 1 {
		e.AverageWaitTime = waitTime
	} else {
		// 计算移动平均
		e.AverageWaitTime = (e.AverageWaitTime*time.Duration(e.TotalRequests-1) + waitTime) / time.Duration(e.TotalRequests)
	}
}

// Stop 停止电梯
func (e *AdvancedElevator) Stop() {
	e.stopChan <- true
}

// AdvancedElevatorScheduler 高级电梯调度器
type AdvancedElevatorScheduler struct {
	Elevators     []*AdvancedElevator
	RequestChan   chan Request
	PassengerChan chan Passenger
	StatusChan    chan ElevatorStatus
	mutex         sync.RWMutex
	requestID     int
	passengerID   int
}

// NewAdvancedElevatorScheduler 创建高级电梯调度器
func NewAdvancedElevatorScheduler(numElevators int, capacity int) *AdvancedElevatorScheduler {
	scheduler := &AdvancedElevatorScheduler{
		Elevators:     make([]*AdvancedElevator, numElevators),
		RequestChan:   make(chan Request, 1000),
		PassengerChan: make(chan Passenger, 1000),
		StatusChan:    make(chan ElevatorStatus, 100),
	}

	// 创建电梯
	for i := 0; i < numElevators; i++ {
		scheduler.Elevators[i] = NewAdvancedElevator(i+1, capacity)
	}

	return scheduler
}

// Start 启动调度器
func (s *AdvancedElevatorScheduler) Start() {
	// 启动所有电梯
	for _, elevator := range s.Elevators {
		go elevator.Run()
	}

	// 启动请求分发器
	go s.dispatchRequests()

	// 启动乘客分发器
	go s.dispatchPassengers()

	// 启动状态监控器
	go s.monitorStatus()
}

// dispatchRequests 分发请求到最合适的电梯
func (s *AdvancedElevatorScheduler) dispatchRequests() {
	for req := range s.RequestChan {
		bestElevator := s.selectBestElevatorAdvanced(req)
		if bestElevator != nil {
			bestElevator.AddRequest(req)
		}
	}
}

// dispatchPassengers 分发乘客到最合适的电梯
func (s *AdvancedElevatorScheduler) dispatchPassengers() {
	for passenger := range s.PassengerChan {
		// 简单策略：选择当前在乘客楼层且有空间的电梯
		assigned := false
		for _, elevator := range s.Elevators {
			status := elevator.GetStatus()
			if status.CurrentFloor == passenger.FromFloor && status.Load < status.Capacity {
				elevator.AddPassenger(passenger)
				assigned = true
				break
			}
		}
		if !assigned {
			fmt.Printf("⚠️ 乘客 %d 暂时无法分配到电梯\n", passenger.ID)
		}
	}
}

// monitorStatus 监控电梯状态
func (s *AdvancedElevatorScheduler) monitorStatus() {
	for status := range s.StatusChan {
		fmt.Printf("📊 电梯 %d 状态: %d楼 %s %s 载客:%d/%d 队列:%d\n",
			status.ElevatorID, status.CurrentFloor, status.Direction,
			status.State, status.Load, status.Capacity, status.QueueLength)
	}
}

// selectBestElevatorAdvanced 高级电梯选择算法
func (s *AdvancedElevatorScheduler) selectBestElevatorAdvanced(req Request) *AdvancedElevator {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var bestElevator *AdvancedElevator
	minCost := float64(999999)

	for _, elevator := range s.Elevators {
		elevator.mutex.RLock()
		cost := s.calculateAdvancedCost(elevator, req)
		elevator.mutex.RUnlock()

		if cost < minCost {
			minCost = cost
			bestElevator = elevator
		}
	}

	return bestElevator
}

// calculateAdvancedCost 计算高级成本
func (s *AdvancedElevatorScheduler) calculateAdvancedCost(elevator *AdvancedElevator, req Request) float64 {
	// 基础距离成本
	distance := float64(abs(elevator.CurrentFloor - req.Floor))
	cost := distance * 10.0

	// 方向一致性奖励/惩罚
	if elevator.State == Moving {
		if (elevator.Direction == Up && req.Floor > elevator.CurrentFloor && req.Direction == Up) ||
			(elevator.Direction == Down && req.Floor < elevator.CurrentFloor && req.Direction == Down) {
			cost *= 0.7 // 方向一致的奖励
		} else {
			cost *= 1.5 // 方向不一致的惩罚
		}
	}

	// 载客量影响
	loadFactor := float64(elevator.CurrentLoad) / float64(elevator.MaxCapacity)
	cost *= (1.0 + loadFactor*0.5)

	// 队列长度影响
	cost += float64(len(elevator.Requests)) * 3.0

	// 优先级影响
	priorityFactor := float64(11-req.Priority) / 10.0
	cost *= priorityFactor

	return cost
}

// CallElevator 呼叫电梯（带优先级）
func (s *AdvancedElevatorScheduler) CallElevator(floor int, direction Direction, priority int) {
	s.requestID++
	req := Request{
		ID:        s.requestID,
		Floor:     floor,
		Direction: direction,
		Priority:  priority,
		Timestamp: time.Now(),
	}

	fmt.Printf("🔔 请求 #%d: %d楼 %s (优先级: %d)\n", req.ID, floor, direction, priority)
	s.RequestChan <- req
}

// AddPassenger 添加乘客
func (s *AdvancedElevatorScheduler) AddPassenger(fromFloor, toFloor int) {
	s.passengerID++
	passenger := Passenger{
		ID:        s.passengerID,
		FromFloor: fromFloor,
		ToFloor:   toFloor,
		EnterTime: time.Now(),
	}

	fmt.Printf("👤 乘客 #%d: 从 %d楼 到 %d楼\n", passenger.ID, fromFloor, toFloor)
	s.PassengerChan <- passenger
}

// PrintStatistics 打印统计信息
func (s *AdvancedElevatorScheduler) PrintStatistics() {
	fmt.Println("\n📈 系统统计信息:")
	fmt.Println("================")

	for _, elevator := range s.Elevators {
		elevator.mutex.RLock()
		fmt.Printf("电梯 %d: 总距离=%d楼层, 总请求=%d, 平均等待=%.1f秒\n",
			elevator.ID, elevator.TotalDistance, elevator.TotalRequests,
			elevator.AverageWaitTime.Seconds())
		elevator.mutex.RUnlock()
	}
}

// Stop 停止所有电梯
func (s *AdvancedElevatorScheduler) Stop() {
	for _, elevator := range s.Elevators {
		elevator.Stop()
	}
	close(s.RequestChan)
	close(s.PassengerChan)
	close(s.StatusChan)
}

// abs 绝对值函数
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// 模拟复杂的电梯使用场景
func simulateAdvancedElevatorUsage(scheduler *AdvancedElevatorScheduler) {
	// 小明的场景
	fmt.Println("\n=== 小明要回家上楼 ===")
	scheduler.CallElevator(1, Up, 5) // 普通优先级
	scheduler.CallElevator(1, Up, 5) // 小明按了两个按钮

	time.Sleep(1 * time.Second)

	// 紧急情况
	fmt.Println("\n=== 紧急情况 ===")
	scheduler.CallElevator(8, Down, 10) // 最高优先级（可能是紧急情况）

	time.Sleep(2 * time.Second)

	// 正常使用
	fmt.Println("\n=== 正常使用场景 ===")
	go func() {
		requests := []struct {
			floor     int
			direction Direction
			priority  int
			delay     time.Duration
		}{
			{5, Down, 3, 2 * time.Second},
			{3, Up, 4, 3 * time.Second},
			{10, Down, 2, 1 * time.Second},
			{6, Up, 6, 4 * time.Second},
			{2, Up, 1, 2 * time.Second},
			{9, Down, 7, 3 * time.Second},
		}

		for _, req := range requests {
			time.Sleep(req.delay)
			scheduler.CallElevator(req.floor, req.direction, req.priority)
		}
	}()

	// 模拟乘客
	go func() {
		time.Sleep(5 * time.Second)
		scheduler.AddPassenger(1, 8) // 乘客从1楼到8楼
		time.Sleep(3 * time.Second)
		scheduler.AddPassenger(5, 2) // 乘客从5楼到2楼
		time.Sleep(4 * time.Second)
		scheduler.AddPassenger(3, 10) // 乘客从3楼到10楼
	}()
}

func main() {
	fmt.Println("🏢 高级电梯调度系统启动")
	fmt.Println("📋 系统配置：2部电梯，每部载客8人，10层楼")
	fmt.Println("🔧 调度算法：优先级SCAN算法 + 智能成本分配")
	fmt.Println("🎯 新功能：载客量管理、优先级调度、实时监控")
	fmt.Println("====================================================")

	// 创建有2部电梯的高级调度系统，每部电梯载客8人
	scheduler := NewAdvancedElevatorScheduler(2, 8)

	// 启动调度器
	scheduler.Start()

	// 等待系统初始化
	time.Sleep(1 * time.Second)

	// 模拟复杂的电梯使用场景
	simulateAdvancedElevatorUsage(scheduler)

	// 运行35秒后停止
	time.Sleep(35 * time.Second)

	fmt.Println("\n🛑 系统停止运行")
	scheduler.PrintStatistics()
	scheduler.Stop()
}
