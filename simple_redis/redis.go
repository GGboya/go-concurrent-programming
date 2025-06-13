package simple_redis

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"time"
)

// Value 表示存储的值和过期时间
type Value struct {
	Data      interface{}
	ExpiresAt *time.Time
}

// Hash 表示哈希表结构
type Hash map[string]interface{}

// SaveRule 保存规则
type SaveRule struct {
	Seconds int // 时间窗口（秒）
	Changes int // 最少修改次数
}

// Redis 核心结构
type Redis struct {
	mu         sync.RWMutex
	data       map[string]*Value // 主要数据存储
	hashes     map[string]Hash   // 哈希表存储
	aofFile    *os.File          // AOF 文件
	rdbPath    string            // RDB 文件路径
	aofPath    string            // AOF 文件路径
	aofEnabled bool              // AOF 开关，默认关闭（符合真实 Redis）
	closed     bool

	// RDB保存规则相关
	saveRules            []SaveRule // 保存规则列表
	changesSinceLastSave int        // 上次保存以来的修改次数
	lastSaveTime         time.Time  // 上次保存时间
	saveMutex            sync.Mutex // 保存操作锁
}

// NewRedis 创建新的 Redis 实例
func NewRedis() *Redis {
	redis := &Redis{
		data:         make(map[string]*Value),
		hashes:       make(map[string]Hash),
		rdbPath:      "dump.rdb",
		aofPath:      "appendonly.aof",
		aofEnabled:   false, // 默认关闭 AOF，符合真实 Redis
		lastSaveTime: time.Now(),
		// 默认的Redis保存规则
		saveRules: []SaveRule{
			{Seconds: 900, Changes: 1},    // 15分钟内至少1个修改
			{Seconds: 300, Changes: 10},   // 5分钟内至少10个修改
			{Seconds: 60, Changes: 10000}, // 1分钟内至少10000个修改
		},
	}

	// 启动时加载持久化数据
	redis.LoadRDB()

	// 只有在 AOF 开启时才加载和打开文件
	if redis.aofEnabled {
		redis.LoadAOF()
		var err error
		redis.aofFile, err = os.OpenFile(redis.aofPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Warning: Could not open AOF file: %v\n", err)
		}
	}

	return redis
}

// EnableAOF 开启 AOF 功能
func (r *Redis) EnableAOF() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.aofEnabled {
		return nil // 已经开启
	}

	r.aofEnabled = true
	var err error
	r.aofFile, err = os.OpenFile(r.aofPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		r.aofEnabled = false
		return err
	}

	fmt.Println("AOF enabled")
	return nil
}

// DisableAOF 关闭 AOF 功能
func (r *Redis) DisableAOF() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.aofEnabled {
		return
	}

	r.aofEnabled = false
	if r.aofFile != nil {
		r.aofFile.Close()
		r.aofFile = nil
	}

	fmt.Println("AOF disabled")
}

// incrementChanges 增加修改计数
func (r *Redis) incrementChanges() {
	r.saveMutex.Lock()
	defer r.saveMutex.Unlock()
	r.changesSinceLastSave++
}

// resetSaveStats 重置保存统计信息
func (r *Redis) resetSaveStats() {
	r.saveMutex.Lock()
	defer r.saveMutex.Unlock()
	r.changesSinceLastSave = 0
	r.lastSaveTime = time.Now()
}

// ShouldSave 检查是否应该执行保存
func (r *Redis) ShouldSave() bool {
	r.saveMutex.Lock()
	defer r.saveMutex.Unlock()

	if r.changesSinceLastSave == 0 {
		return false // 没有任何修改
	}

	timeSinceLastSave := time.Since(r.lastSaveTime)

	for _, rule := range r.saveRules {
		if timeSinceLastSave >= time.Duration(rule.Seconds)*time.Second &&
			r.changesSinceLastSave >= rule.Changes {
			fmt.Printf("触发保存规则: %d秒内%d次修改 (实际: %.0f秒内%d次修改)\n",
				rule.Seconds, rule.Changes,
				timeSinceLastSave.Seconds(), r.changesSinceLastSave)
			return true
		}
	}

	return false
}

// GetSaveStats 获取保存统计信息
func (r *Redis) GetSaveStats() (int, time.Duration) {
	r.saveMutex.Lock()
	defer r.saveMutex.Unlock()
	return r.changesSinceLastSave, time.Since(r.lastSaveTime)
}

// Set 设置键值对
func (r *Redis) Set(key string, value interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.data[key] = &Value{Data: value}
	r.incrementChanges() // 增加修改计数

	// 只有在 AOF 开启时才写入日志
	if r.aofEnabled {
		r.writeAOF(fmt.Sprintf("SET %s %v", key, value))
	}
}

// Get 获取值
func (r *Redis) Get(key string) (interface{}, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	val, exists := r.data[key]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if val.ExpiresAt != nil && time.Now().After(*val.ExpiresAt) {
		delete(r.data, key)
		return nil, false
	}

	return val.Data, true
}

// HSet 设置哈希表字段
func (r *Redis) HSet(key, field string, value interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.hashes[key] == nil {
		r.hashes[key] = make(Hash)
	}
	r.hashes[key][field] = value
	r.incrementChanges() // 增加修改计数

	if r.aofEnabled {
		r.writeAOF(fmt.Sprintf("HSET %s %s %v", key, field, value))
	}
}

// HGet 获取哈希表字段值
func (r *Redis) HGet(key, field string) (interface{}, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	hash, exists := r.hashes[key]
	if !exists {
		return nil, false
	}

	value, exists := hash[field]
	return value, exists
}

// HDel 删除哈希表字段
func (r *Redis) HDel(key, field string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	hash, exists := r.hashes[key]
	if !exists {
		return false
	}

	_, exists = hash[field]
	if exists {
		delete(hash, field)
		if len(hash) == 0 {
			delete(r.hashes, key)
		}
		r.incrementChanges() // 增加修改计数

		if r.aofEnabled {
			r.writeAOF(fmt.Sprintf("HDEL %s %s", key, field))
		}
	}

	return exists
}

// Expire 设置键的过期时间
func (r *Redis) Expire(key string, duration time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	val, exists := r.data[key]
	if !exists {
		return false
	}

	expiresAt := time.Now().Add(duration)
	val.ExpiresAt = &expiresAt
	r.incrementChanges() // 增加修改计数

	if r.aofEnabled {
		r.writeAOF(fmt.Sprintf("EXPIRE %s %d", key, int64(duration.Seconds())))
	}

	return true
}

// TTL 获取键的剩余生存时间
func (r *Redis) TTL(key string) (time.Duration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	val, exists := r.data[key]
	if !exists {
		return 0, false
	}

	if val.ExpiresAt == nil {
		return -1, true // -1 表示永不过期
	}

	remaining := time.Until(*val.ExpiresAt)
	if remaining < 0 {
		return 0, false // 已过期
	}

	return remaining, true
}

// StartExpiredKeyCleaner 启动过期键清理器
func (r *Redis) StartExpiredKeyCleaner() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanExpiredKeys()
		}

		if r.closed {
			return
		}
	}
}

// cleanExpiredKeys 清理过期的键
func (r *Redis) cleanExpiredKeys() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for key, val := range r.data {
		if val.ExpiresAt != nil && now.After(*val.ExpiresAt) {
			delete(r.data, key)
		}
	}
}

// writeAOF 写入 AOF 日志
func (r *Redis) writeAOF(command string) {
	if r.aofFile != nil {
		r.aofFile.WriteString(command + "\n")
		r.aofFile.Sync()
	}
}

// SaveRDB 保存 RDB 快照（同步版本）
func (r *Redis) SaveRDB() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	file, err := os.Create(r.rdbPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	// 保存数据结构
	snapshot := struct {
		Data   map[string]*Value
		Hashes map[string]Hash
	}{
		Data:   r.data,
		Hashes: r.hashes,
	}

	err = encoder.Encode(snapshot)
	if err == nil {
		r.resetSaveStats() // 保存成功后重置统计信息
		fmt.Println("RDB保存完成，重置修改计数")
	}

	return err
}

// LoadRDB 加载 RDB 快照
func (r *Redis) LoadRDB() error {
	file, err := os.Open(r.rdbPath)
	if err != nil {
		return err // 文件不存在是正常的
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	var snapshot struct {
		Data   map[string]*Value
		Hashes map[string]Hash
	}

	if err := decoder.Decode(&snapshot); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.data = snapshot.Data
	r.hashes = snapshot.Hashes

	return nil
}

// LoadAOF 加载 AOF 日志
func (r *Redis) LoadAOF() error {
	file, err := os.Open(r.aofPath)
	if err != nil {
		return err // 文件不存在是正常的
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// 这里可以解析 AOF 命令并重新执行
		// 为简化起见，这里只是占位符
		_ = line
	}

	return scanner.Err()
}

// BackgroundSave 后台保存 RDB
func (r *Redis) BackgroundSave() {
	go func() {
		if err := r.SaveRDB(); err != nil {
			fmt.Printf("Background save failed: %v\n", err)
		} else {
			fmt.Println("Background save completed")
		}
	}()
}

// RewriteAOF 重写 AOF 文件
func (r *Redis) RewriteAOF() {
	go func() {
		r.mu.RLock()

		newAOFPath := r.aofPath + ".new"
		file, err := os.Create(newAOFPath)
		if err != nil {
			r.mu.RUnlock()
			fmt.Printf("AOF rewrite failed: %v\n", err)
			return
		}

		// 重写所有当前数据
		for key, val := range r.data {
			file.WriteString(fmt.Sprintf("SET %s %v\n", key, val.Data))
			if val.ExpiresAt != nil {
				remaining := time.Until(*val.ExpiresAt)
				if remaining > 0 {
					file.WriteString(fmt.Sprintf("EXPIRE %s %d\n", key, int64(remaining.Seconds())))
				}
			}
		}

		for key, hash := range r.hashes {
			for field, value := range hash {
				file.WriteString(fmt.Sprintf("HSET %s %s %v\n", key, field, value))
			}
		}

		r.mu.RUnlock()

		file.Close()

		// 替换原文件
		if err := os.Rename(newAOFPath, r.aofPath); err != nil {
			fmt.Printf("AOF rewrite failed: %v\n", err)
			return
		}

		// 重新打开 AOF 文件
		r.aofFile.Close()
		r.aofFile, err = os.OpenFile(r.aofPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Failed to reopen AOF file: %v\n", err)
		}

		fmt.Println("AOF rewrite completed")
	}()
}

// Close 关闭 Redis 实例
func (r *Redis) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	if r.aofFile != nil {
		r.aofFile.Close()
	}
}
