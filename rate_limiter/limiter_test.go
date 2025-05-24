package limiter

import (
	"testing"
	"time"
)

// 测试基本的功能
func TestRateLimiter_Basic(t *testing.T) {
	rate := 10
	limiter := NewRateLimiter(rate)

	// 测试初始可以立即获取rate个令牌
	start := time.Now()
	for i := 0; i < rate; i++ {
		limiter.Take()
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Errorf("获取初始令牌耗时过长: %v", elapsed)
	}
}

// 测试限流效果
func TestRateLimiter_Rate(t *testing.T) {
	rate := 10
	limiter := NewRateLimiter(rate)

	// 先消耗掉所有令牌
	for i := 0; i < rate; i++ {
		limiter.Take()
	}

	// 测试限流效果
	start := time.Now()

	// elapsed 拿到令牌耗费的时间
	elapsed := limiter.Take().Sub(start)

	// expected 理论上应该等待 1/rate 秒
	expected := time.Second / time.Duration(rate)

	// 允许10%的误差
	if elapsed < time.Duration(float64(expected)*0.9) {
		t.Errorf("限流不足，期望至少等待 %v，实际等待 %v", expected, elapsed)
	}
}

// 测试并发性能
func TestRateLimiter_Concurrent(t *testing.T) {
	rate := 10
	limiter := NewRateLimiter(rate)
	count := 50
	done := make(chan bool, count) // 使用带缓冲的channel避免goroutine泄漏

	start := time.Now()

	// 并发获取令牌
	for i := 0; i < count; i++ {
		go func() {
			limiter.Take()
			done <- true
		}()
	}

	// 等待所有goroutine完成
	for i := 0; i < count; i++ {
		<-done
	}

	elapsed := time.Since(start)
	// 理论上至少需要 (count-rate)/rate 秒
	minExpected := time.Duration(float64(count-rate) / float64(rate) * float64(time.Second))
	if elapsed < time.Duration(float64(minExpected)*0.9) {
		t.Errorf("处理速度过快，期望至少 %v，实际 %v", minExpected, elapsed)
	}
}
