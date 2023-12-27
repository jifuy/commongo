package main

import (
	"fmt"
	"github.com/jifuy/commongo/dbClient"
	"sync"
	"time"
)

var lock sync.Mutex

func main() {

	db := dbClient.DbInfo{DbType: "mysql", DbName: "db1", Host: "192.168.5.136", Port: "3306", UserName: "root", PassWord: "zyuc123"}
	dbClient.SetUpDb(db)

	time.Sleep(-10 * time.Second)
	fmt.Println("111")

	//go xxx()
	go func() {
		var i = 0
		for {
			select {
			case <-time.After(2 * time.Second):
				i += 1
				fmt.Println("555555555555-", i, time.Now())
				lock.Lock()
				time.Sleep(1 * time.Second)
				lock.Unlock()
				fmt.Println("-------------", i, time.Now())
			}
		}
	}()

	select {}
}

func xxx() {
	lock.Lock()
	fmt.Println("222")
	lock.Unlock()
}

func xxx1() {
	lock.Lock()
	time.Sleep(3 * time.Second)
	fmt.Println("32333")
	lock.Unlock()
}
