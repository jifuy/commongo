package redisClient

import (
	"fmt"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

type RedCLi interface {
	Get() redis.Conn
	Close() error
}

type RedisInfo struct {
	Type     string
	Host     string
	Port     string
	PassWord string
	Db       int
	PoolSize int
	Address  string

	Redis RedCLi
}

func NewRedCLi(r RedisInfo) (RedCLi, error) {
	if r.Type == "cluster" {
		clu, err := NewRedisCliClu(r)
		return clu, err
	}
	node, err := NewRedisCliNode(r)
	return node, err
}

func NewRedisCliNode(r RedisInfo) (*redis.Pool, error) {
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
					conn.Close()
					return nil, err
				}
			}
			if _, err = conn.Do("SELECT", r.Db); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, err
		},
	}
	c := RedisPool.Get()
	reply, err := c.Do("PING")
	if err != nil {
		fmt.Println(reply, err)
		return RedisPool, err
	}
	return RedisPool, nil
}

func NewRedisCliClu(r RedisInfo) (*redisc.Cluster, error) {
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
		return nil, err
	}
	c := cluster.Get()
	reply, err := c.Do("PING")
	if err != nil {
		fmt.Println(reply, err)
		return nil, err
	}
	return cluster, nil
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
