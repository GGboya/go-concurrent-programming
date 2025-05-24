package limiter

import (
	"time"
)

// 限流器
type RateLimiter struct {
	ch   chan struct{} // 模拟令牌桶
	rate int           // 每秒允许的请求数
}

func NewRateLimiter(rate int) *RateLimiter {
	res := &RateLimiter{
		ch:   make(chan struct{}, rate),
		rate: rate,
	}
	// 填充令牌
	for i := 0; i < rate; i++ {
		res.ch <- struct{}{}
	}

	// 开启协程，每秒放10个令牌
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(rate))
		defer ticker.Stop()
		for range ticker.C {
			select {
			case res.ch <- struct{}{}:
			default:
			}
		}
	}()
	return res
}

// 返回用户拿到令牌桶的时间
func (r *RateLimiter) Take() time.Time {
	<-r.ch
	return time.Now()
}
