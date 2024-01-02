package dbClient

import (
	"database/sql"
	"fmt"
	"time"
)

var DbClients = make(map[string]*sql.DB, 0)

// 客户端对象
type DbInfo struct {
	DbName      string //连接名
	DbType      string
	Host        string
	Port        string
	UserName    string
	PassWord    string
	DataBase    string //连接的库名
	MaxOpenConn int
	MaxIdleConn int
}

func SetUpDb(m DbInfo) (*sql.DB, error) {
	var openUrl string
	switch m.DbType {
	case "mysql":
		openUrl = fmt.Sprintf("%s:%s@%s(%s:%s)/%s", m.UserName, m.PassWord, "tcp", m.Host, m.Port, m.DataBase) + "?charset=utf8mb4&parseTime=true&loc=Local"
	case "dm":
		openUrl = fmt.Sprintf("%s://%s:%s@%s:%s", "dm", m.UserName, m.PassWord, m.Host, m.Port)
	}

	sDb, err := sql.Open(m.DbType, openUrl)
	if err != nil {
		return nil, nil
	}
	sDb.SetConnMaxLifetime(time.Second * 20)
	if m.MaxOpenConn < 1 {
		m.MaxOpenConn = 10
	}
	if m.MaxIdleConn < 1 {
		m.MaxIdleConn = 5
	}

	if err = sDb.Ping(); err != nil {
		return nil, err
	}
	DbClients[m.DbName] = sDb
	return nil, nil
}
