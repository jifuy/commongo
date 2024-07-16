package redisClient

import (
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
	_, err := conn.Do("Del", key)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisInfo) Hset(key, mapkey, fields string) error {
	conn := r.Redis.Get()
	defer conn.Close()
	_, err := redis.Int(conn.Do("hSet", key, mapkey, fields))
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

	_, err := conn.Do("hDel", args...)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisInfo) Hget(key, fields string) ([]byte, error) {
	// 从池里获取连接
	conn := r.Redis.Get()
	// 用完后将连接放回连接池
	defer conn.Close()
	reply, err := redis.Bytes(conn.Do("hGet", key, fields))
	if err != nil {
		return reply, err
	}
	return reply, nil
}

func (r *RedisInfo) Hgetall(key string) (map[string]string, error) {
	var AllMap = make(map[string]string, 0)
	// 从池里获取连接
	conn := r.Redis.Get()
	// 用完后将连接放回连接池
	defer conn.Close()
	AllMap, err := redis.StringMap(conn.Do("hGetAll", key))
	if err != nil {
		return AllMap, err
	}
	return AllMap, nil
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
