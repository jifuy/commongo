package es

import (
	"fmt"
	"github.com/jifuy/commongo/es/esutil"
	"log"
	"testing"
	"time"
)

func TestNewEs(t *testing.T) {
	config := esutil.EsCfg{
		//Addresses: []string{"http://192.168.5.188:9200"},
		Addresses: []string{"http://127.0.0.1:9200"},
		UserName:  "",
		PassWord:  "",
		Version:   7,
		DocType:   "_doc",
	}
	cli, _ := NewEsClient(config)
	josn := `{"k_rule_id":"secondarySplittingfault2","k_father_alarm_id":"89761365ed78a51937782c1948d775ca","k_child_alarm_id":[""],"t_relation_time":"2024-05-23 10:39:31"}`
	for i := 0; i < 100; i++ {
		err := cli.EsPost("alarm_correlation_20240618", "89761365ed78a51937782c1948d775ca", josn)
		fmt.Println(err)
		time.Sleep(time.Second)
	}

	t.Log("hello world")
}

func TestNewEs2(t *testing.T) {
	config := esutil.EsCfg{
		Addresses: []string{"192.168.1.1:9200"},
		UserName:  "elastic",
		PassWord:  "123456",
		Version:   7,
		DocType:   "_doc",
	}
	cli, _ := NewEsClient(config)

	cli.EsSearch([]string{"xsqindex"}, "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}")
	t.Log("hello world")
}

func TestNewEsBatch(t *testing.T) {
	config := esutil.EsCfg{
		Addresses: []string{"192.168.1.1:9200"},
		UserName:  "elastic",
		PassWord:  "123456",
		Version:   7,
		DocType:   "_doc",
	}
	cli, _ := NewEsClient(config)

	msg := map[string]string{
		"123": `{"message" : "Hello World5"}`,
		"234": `{"message" : "Hello World6"}`,
	}
	cli.BatchSend("xsqindex", "", msg)
	t.Log("hello world")
}

func TestNewBatchDEL(t *testing.T) {
	config := esutil.EsCfg{
		Addresses: []string{"http://127.0.0.1:9200"},
		//UserName:  "elastic",
		//PassWord:  "123456",
		Version: 7,
		DocType: "_doc",
	}
	cli, _ := NewEsClient(config)

	err := cli.DeleteIndexesByPattern("my_index_202507")
	t.Log("hello world", err)
}

func TestNewBatchDELDOC(t *testing.T) {
	config := esutil.EsCfg{
		Addresses: []string{"http://127.0.0.1:9200"},
		//UserName:  "elastic",
		//PassWord:  "123456",
		Version: 7,
		DocType: "_doc",
	}
	cli, _ := NewEsClient(config)

	err := cli.DeleteDuplicateDoc("my_index", "keysq")
	t.Log("hello world", err)
}

func TestMergeIndexes(t *testing.T) {
	config := esutil.EsCfg{
		Addresses: []string{"http://127.0.0.1:9200"},
		//UserName:  "elastic",
		//PassWord:  "123456",
		Version: 7,
		DocType: "_doc",
	}
	cli, _ := NewEsClient(config)

	err := cli.MergeIndexes([]string{"my_index_2025072*"}, "my_index_202507")
	if err != nil {
		log.Fatalf("Merge failed: %v", err)
	}
}
