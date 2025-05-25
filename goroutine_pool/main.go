package main

import (
	"fmt"
	"sync"
	"time"
)

type goroutinePool struct {
	workerChan chan *Worker
	taskQueue  chan *Task
	wg         sync.WaitGroup
}

type Worker struct {
	ID int // worker ID
}

func (w *Worker) Run(task *Task) {
	// 执行 task
	fmt.Printf("worker %d 正在执行 task %d\n", w.ID, task.ID)
	task.exeute()
}

type Task struct {
	ID     int    // task ID
	exeute func() // 真正要执行的方法
}

func NewPool(size int) *goroutinePool {
	// 初始化协程池
	pool := &goroutinePool{
		workerChan: make(chan *Worker, size),
		taskQueue:  make(chan *Task, 1000),
		wg:         sync.WaitGroup{},
	}

	// 填充 worker
	for i := 0; i < size; i++ {
		worker := &Worker{
			ID: i,
		}
		pool.workerChan <- worker
	}

	// 异步监听 task
	go pool.Run()
	return pool
}

func (g *goroutinePool) Run() {
	for task := range g.taskQueue {
		// 获取 worker
		w := <-g.workerChan
		go func(w *Worker, task *Task) {
			defer func() {
				// 任务执行完，回收 worker
				g.workerChan <- w
				g.wg.Done()
			}()

			// 执行任务
			w.Run(task)
		}(w, task)
	}
}

func (g *goroutinePool) Submit(task *Task) {
	// 注意，g.wg.Add(1) 需要放在这里，而不能放在 g.Run() 方法中
	// 当 main 调用 pool.Wait() 时，Run 协程有可能还没有执行任何一个任务，就返回了。导致整个进程退出，控制台没有任何输出。
	// 可以在 main 中添加一个 time.sleep()，来验证下这个结论。挺有意思的。
	g.wg.Add(1)
	g.taskQueue <- task
}

func (g *goroutinePool) Wait() {
	g.wg.Wait()
}

func main() {
	// 创建一个容量为100的协程池
	pool := NewPool(100)

	// 模拟小美的追求者发送表白消息
	for i := 1; i <= 1000; i++ {
		task := &Task{
			ID: i,
			exeute: func() {
				// 模拟任务处理时间
				time.Sleep(100 * time.Millisecond)
			},
		}
		pool.Submit(task)
	}

	// 等待所有任务完成
	pool.Wait()
	fmt.Println("All tasks completed!")
}
