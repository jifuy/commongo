package redisClient

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
)

func (r *RedisInfo) Exists(key string) bool {
	conn := r.Redis.Get()
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return false
	}
	return exists
}

func (r *RedisInfo) Get(key string) ([]byte, error) {
	conn := r.Redis.Get()
	defer conn.Close()
	reply, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return reply, err
	}
	return reply, nil
}

func (r *RedisInfo) Del(key string) error {
	conn := r.Redis.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisInfo) Hset(key, mapkey, field string) error {
	conn := r.Redis.Get()
	defer conn.Close()
	_, err := redis.Int(conn.Do("HSET", key, mapkey, field))
	if err != nil {
		return err
	}
	return err
}

func (r *RedisInfo) Hmset(key string, fieldsAndValues map[string]interface{}) error {
	conn := r.Redis.Get()
	defer conn.Close()
	args := make([]interface{}, 0, len(fieldsAndValues)*2)
	args = append(args, key)
	for k, v := range fieldsAndValues {
		args = append(args, k, v)
	}
	_, err := conn.Do("HMSET", args...)
	if err != nil {
		return err
	}
	return err
}

func (r *RedisInfo) Hdel(key string, fields ...string) error {
	conn := r.Redis.Get()
	defer conn.Close()
	args := make([]interface{}, 0, len(fields))
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}

	_, err := conn.Do("HDEL", args...)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisInfo) Hget(key, field string) ([]byte, error) {
	// 从池里获取连接
	conn := r.Redis.Get()
	// 用完后将连接放回连接池
	defer conn.Close()
	reply, err := redis.Bytes(conn.Do("HGET", key, field))
	if err != nil {
		return reply, err
	}
	return reply, nil
}

func (r *RedisInfo) Hmget(key string, fields ...string) (map[string]string, error) {
	// Get a connection from the pool
	conn := r.Redis.Get()
	// Ensure the connection is closed after we're done
	defer conn.Close()

	// Prepare arguments for the HMGET command
	args := make([]interface{}, 0)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}

	// Execute the HMGET command
	values, err := redis.Values(conn.Do("HMGET", args...))
	if err != nil {
		return nil, err
	}
	// Create a map to store the results
	resultMap := make(map[string]string)

	// Iterate over the fields and values to populate the resultMap
	for i, field := range fields {
		if i < len(values) {
			value, ok := values[i].([]byte)
			if !ok {
				continue
				//return nil, fmt.Errorf("unexpected value type for field %s", field)
			}
			resultMap[field] = string(value)
		} else {
			// If there are more fields than values, set the value to an empty string
			resultMap[field] = ""
		}
	}

	return resultMap, nil
}

func (r *RedisInfo) Hgetall(key string) (map[string]string, error) {
	// 从池里获取连接
	conn := r.Redis.Get()
	// 用完后将连接放回连接池
	defer conn.Close()
	AllMap, err := redis.StringMap(conn.Do("HGETALL", key))
	if err != nil {
		return AllMap, err
	}
	return AllMap, nil
}

func (r *RedisInfo) Hexists(key, field string) (bool, error) {
	// 从池里获取连接
	conn := r.Redis.Get()
	// 用完后将连接放回连接池
	defer conn.Close()
	num, err := redis.Int(conn.Do("HEXISTS", key, field))
	if err != nil {
		return false, err
	}
	if num != 1 {
		return false, nil
	}
	return true, nil
}

func (r *RedisInfo) Keys(likekey string) ([]string, error) {
	conn := r.Redis.Get()
	defer conn.Close()
	keys, err := redis.Strings(conn.Do("KEYS", likekey))
	if err != nil {
		return keys, err
	}
	return keys, nil
}

func (r *RedisInfo) ScanKeys(match string, count int) ([]string, error) {
	conn := r.Redis.Get()
	defer conn.Close()

	cursor := uint64(0)
	var keys []string

	for {
		values, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", match, "COUNT", count))

		if err != nil {
			return nil, err
		}

		// Convert the first value to a cursor (uint64)
		cursor, err = redis.Uint64(values[0], nil)
		if err != nil {
			return nil, err
		}

		// Convert the second value to a slice of strings
		scanKeys, err := redis.Strings(values[1], nil)
		if err != nil {
			return nil, err
		}

		keys = append(keys, scanKeys...)
		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

func (r *RedisInfo) Expireat(key string, timeAt int64) error {
	conn := r.Redis.Get()
	defer conn.Close()
	num, err := redis.Int(conn.Do("EXPIREAT", key, timeAt))
	if err != nil {
		return err
	}
	if num != 1 {
		return fmt.Errorf("redis Expireat error: no key (%s) exist", key)
	}
	return nil
}

func (r *RedisInfo) Eval(script string, keys []string, args ...interface{}) (interface{}, error) {
	conn := r.Redis.Get()
	defer conn.Close()
	cmdargs := make([]interface{}, 0)
	//EVAL script numkeys key [key ...] arg [arg ...]
	cmdargs = append(cmdargs, script)
	cmdargs = append(cmdargs, len(keys))
	for _, key := range keys {
		cmdargs = append(cmdargs, key)
	}
	cmdargs = append(cmdargs, args...)
	v, err := conn.Do("EVAL", cmdargs...)
	if err != nil {
		return v, err
	}
	return v, nil
}

func (r *RedisInfo) AddLock(key, value string, ex int) bool {
	conn := r.Redis.Get()
	defer conn.Close()
	luaScript := `	
	local val = redis.call("GET", KEYS[1])
	if not val then
		redis.call("setex", KEYS[1], ARGV[2], ARGV[1])
		return 2
	elseif val == ARGV[1] then
		return redis.call("expire", KEYS[1], ARGV[2])
	else
		return 0
	end`
	//EVAL script numkeys key [key ...] arg [arg ...]
	v, err := redis.Int(conn.Do("EVAL", luaScript, 1, key, value, ex))
	if err != nil {
		fmt.Println("EVAL error", err)
		return false
	}
	if v == 0 {
		return false
	}
	return true
}

func (r *RedisInfo) DelLock(key, value string) bool {
	conn := r.Redis.Get()
	defer conn.Close()
	luaScript := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	//EVAL script numkeys key [key ...] arg [arg ...]
	v, err := redis.Int(conn.Do("EVAL", luaScript, 1, key, value))
	if err != nil {
		fmt.Println("EVAL error", err)
		return false
	}
	if v == 0 {
		return false
	}
	return true
}

func (r *RedisInfo) ZrangeWithScores(key string, start, end int) (map[string]string, error) {
	// 从池里获取连接
	conn := r.Redis.Get()
	// 用完后将连接放回连接池
	defer conn.Close()
	AllMap, err := redis.StringMap(conn.Do("ZRANGE", key, start, end, "withscores"))
	if err != nil {
		return AllMap, err
	}
	return AllMap, nil
}

func (r *RedisInfo) ZrangebyscoreWithScores(key string, start, end int64) (map[string]string, error) {
	// 从池里获取连接
	conn := r.Redis.Get()
	// 用完后将连接放回连接池
	defer conn.Close()
	AllMap, err := redis.StringMap(conn.Do("ZRANGEBYSCORE", key, start, end, "WITHSCORES"))
	if err != nil {
		return AllMap, err
	}
	return AllMap, nil
}

func (r *RedisInfo) Zadd(key string, score int64, value string) error {
	conn := r.Redis.Get()
	defer conn.Close()
	_, err := conn.Do("ZADD", key, score, value)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisInfo) Zrem(key string, value string) (int, error) {
	conn := r.Redis.Get()
	defer conn.Close()
	num, err := redis.Int(conn.Do("ZREM", key, value))
	if err != nil {
		return num, err
	}
	return num, nil
}
