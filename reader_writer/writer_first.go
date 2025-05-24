package main

import (
	"fmt"
	"sync"
	"time"
)

// WritePreferredRWMutex 改进后的写优先读写锁
type WritePreferredRWMutex struct {
	mu             sync.Mutex
	readerCond     *sync.Cond
	writerCond     *sync.Cond
	readers        int  // 正在读的读者
	isWriting      bool // 是否有写者在写
	waitingWriters int  // 等待写的写者
}

func NewWritePreferredRWMutex() *WritePreferredRWMutex {
	m := &WritePreferredRWMutex{}
	m.readerCond = sync.NewCond(&m.mu)
	m.writerCond = sync.NewCond(&m.mu)
	return m
}

func (rw *WritePreferredRWMutex) RLock() {
	// 对读请求上锁
	rw.mu.Lock()
	defer rw.mu.Unlock()

	// 如果有写者正在写，或者正在等待，读请求要被阻塞
	for rw.isWriting || rw.waitingWriters > 0 {
		rw.readerCond.Wait()
	}

	// 不能放在 for 循环之前进行 ++ 的操作，会产生死锁
	/*
		先看死锁的情形， 也就是放在 for 循环之前
		读者 A 上锁，readers = 1, 然后处于正在读的状态
		写者 B 上锁，waitingWriters = 1， 发现有读者在读，于是陷入 waitcond.wait(), 此时释放锁
		读者 C 上锁，正是因为读者和写者用的同一把锁，所以读者能拿到。然后 readers = 2，发现有写者在等待，陷入 readcond.wait()
		读者 A 读完了，readers = 1, 不会唤醒写者，因此陷入死锁。

		那为什么放在 for 循环下面不会死锁呢，同样的情形
		读者 A 上锁，readers = 1, 然后处于正在读的状态
		写者 B 上锁，waitingWriters = 1， 发现有读者在读，于是陷入 waitcond.wait(), 此时释放锁
		读者 C 上锁，正是因为读者和写者用的同一把锁，所以读者能拿到。然后 readers 仍然为 1，发现有写者在等待，陷入 readcond.wait()
		读者 A 读完了，readers = 0, 唤醒写者，waitingWriters = 0。写者 B 写入完成后，唤醒读者 C 避免了死锁
	*/
	rw.readers++
}

func (rw *WritePreferredRWMutex) RUnLock() {
	// 对读请求解锁
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.readers--
	// 当前没有读者正在读了，通知写者可以进行写了
	if rw.readers == 0 {
		rw.writerCond.Broadcast()
	}
}

func (rw *WritePreferredRWMutex) WLock() {
	// 对写请求上锁
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.waitingWriters++
	// 写入之前，需要判断是否已经有写者正在写，或者是否已经有读者正在读
	for rw.isWriting || rw.readers > 0 {
		// 陷入阻塞，直到被唤醒。读者可以唤醒，写入完成也可以唤醒
		rw.writerCond.Wait()
	}
	rw.waitingWriters--
	rw.isWriting = true
	// 即将释放锁，进入写入的流程，所以等待数量--
}

func (rw *WritePreferredRWMutex) WUnLock() {
	// 对写请求解锁
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.isWriting = false

	// 写入完成了，如果有写者在等待，唤醒写者。没有等待的写者，再唤醒读者。实现写优先
	if rw.waitingWriters > 0 {
		rw.writerCond.Broadcast()
	} else {
		rw.readerCond.Broadcast()
	}
}

// MyISAM 表结构
type MyISAM struct {
	content string
	mu      *WritePreferredRWMutex
}

func NewMyISAM() *MyISAM {
	return &MyISAM{
		mu: NewWritePreferredRWMutex(),
	}
}

func (m *MyISAM) Read() (string, time.Duration) {
	start := time.Now()
	m.mu.RLock()
	defer m.mu.RUnLock()

	// 模拟读操作，比较快
	time.Sleep(10 * time.Millisecond)
	elapsed := time.Since(start)
	return m.content, elapsed
}

func (m *MyISAM) Write(content string) {
	m.mu.WLock()
	defer m.mu.WUnLock()

	fmt.Println("正在写", content)
	// 模拟写操作，比较耗时
	time.Sleep(500 * time.Millisecond)
	m.content = content
}

func main() {
	db := NewMyISAM()
	var wg sync.WaitGroup

	// 测试是否是写优先
	wg.Add(20)
	for i := 0; i < 20; i++ {
		if i > 5 && i < 10 {
			time.Sleep(time.Millisecond)
			go func(i int) {
				defer wg.Done()
				db.Write(fmt.Sprintf("%d ", i))
			}(i)
		} else {
			go func() {
				defer wg.Done()
				cotent, t := db.Read()
				fmt.Println("读取内容", cotent, "耗时", t)
			}()
		}
	}
	wg.Wait()
}
