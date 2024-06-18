package es

import (
	"github.com/jifuy/commongo/es/esutil"
	"testing"
)

func TestNewEs(t *testing.T) {
	config := esutil.EsCfg{
		Addresses: []string{"http://localhost:9200"},
		UserName:  "elastic",
		PassWord:  "123456",
		Version:   7,
		DocType:   "_doc",
	}
	cli := NewEsClient(config)

	cli.EsPost("xsqindex", "", `{"message" : "Hello World22"}`)
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
	cli := NewEsClient(config)

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
	cli := NewEsClient(config)

	msg := map[string]string{
		"123": `{"message" : "Hello World5"}`,
		"234": `{"message" : "Hello World6"}`,
	}
	cli.BatchSend("xsqindex", "", msg)
	t.Log("hello world")
}
