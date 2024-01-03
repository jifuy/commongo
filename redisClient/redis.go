package redisClient

import (
	"log"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

type RedisConf struct {
	Type     string
	Host     string
	Port     string
	PassWord string
	Db       int
	PoolSize int
	Address  string
}

type RedCLi interface {
	Get() redis.Conn
	Close() error
}

func NewRedCLi(r RedisConf) RedCLi {
	if r.Type == "cluster" {
		clu := NewRedisCliClu(r)
		return clu
	}
	node := NewRedisCliNode(r)
	return node
}

func NewRedisCliNode(r RedisConf) *redis.Pool {
	address := r.Host + ":" + r.Port
	if r.Address != "" {
		address = r.Address
	}
	if r.PoolSize == 0 {
		r.PoolSize = 100
	}
	RedisPool := &redis.Pool{
		MaxIdle:     r.PoolSize,        //最初的连接数量
		MaxActive:   0,                 //最大连接数量,0不限制
		IdleTimeout: time.Duration(60), //连接关闭时间 60秒 （60秒不使用自动关闭）
		Dial: func() (redis.Conn, error) { //要连接的redis数据库
			conn, err := redis.Dial(
				"tcp",
				address,
				redis.DialReadTimeout(time.Duration(5)*time.Second),
				redis.DialWriteTimeout(time.Duration(3)*time.Second),
				redis.DialConnectTimeout(time.Duration(3)*time.Second),
			)
			if err != nil {
				return nil, err
			}
			if r.PassWord != "" {
				if _, err = conn.Do("AUTH", r.PassWord); err != nil {
					log.Fatalf("AUTH failed: %v", err)
					conn.Close()
					return nil, err
				}
			}
			if _, err = conn.Do("SELECT", r.Db); err != nil {
				log.Fatalf("SELECT Db failed: %v", err)
				conn.Close()
				return nil, err
			}
			return conn, err
		},
	}
	return RedisPool
}

func NewRedisCliClu(r RedisConf) *redisc.Cluster {
	cluster := &redisc.Cluster{
		StartupNodes: strings.Split(r.Address, ","),
		DialOptions: []redis.DialOption{
			redis.DialReadTimeout(time.Duration(5) * time.Second),
			redis.DialWriteTimeout(time.Duration(3) * time.Second),
			redis.DialConnectTimeout(5 * time.Second),
			redis.DialPassword(r.PassWord)},
		CreatePool: createPool,
	}
	// 初始化集群
	if err := cluster.Refresh(); err != nil {
		log.Fatalf("Refresh failed: %v", err)
	}
	return cluster
}

func createPool(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   0,
		IdleTimeout: time.Minute,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}, nil
}
