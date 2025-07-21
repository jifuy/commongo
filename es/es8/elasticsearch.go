package es8

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/jifuy/commongo/es/esutil"
	"github.com/tidwall/sjson"
	"io"
	"strings"
)

var ESClient *Esearch

type Esearch struct {
	Client *elasticsearch.Client
}

func NewEsClient(config esutil.EsCfg) (*Esearch, error) {
	cfg := elasticsearch.Config{
		Addresses:  config.Addresses,
		MaxRetries: 3,
	}
	cfg.Username = config.UserName
	cfg.Password = config.PassWord
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	q1, err := es.Info()
	if err != nil {
		return nil, err
	}
	fmt.Println("ES连接成功！", q1)
	ESClient = &Esearch{
		Client: es,
	}
	return ESClient, nil
}

func (e *Esearch) EsSearch(indexes []string, query string) (esutil.ResponseBody, error) {
	var rsp esutil.ResponseBody
	res, err := e.Client.Search(
		e.Client.Search.WithIndex(indexes...),
		e.Client.Search.WithBody(bytes.NewReader([]byte(query))),
		e.Client.Search.WithPretty(),
	)
	if err != nil {
		return rsp, err
	}
	defer res.Body.Close()
	rspBody, err := io.ReadAll(res.Body)

	if err := json.Unmarshal(rspBody, &rsp); err != nil {
		return rsp, fmt.Errorf("failed to unmarshal response body: %w", err)
	}
	fmt.Println("rsp---------", string(rspBody))
	return rsp, nil
}

func (e *Esearch) EsPost(index, id, content string) error {
	if e.Client == nil {
		return fmt.Errorf("elasticsearch client is nil")
	}
	// 向 Elasticsearch 发送请求示例
	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: id, //没有ID就新增 输入id就更新
		Body:       bytes.NewReader([]byte(content)),
	}
	// 执行 Index 请求
	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("Index request failed: %s", res.Status())
	}
	// 获取 Index 请求的响应结果
	fmt.Println(" 获取 Index 请求的响应结果: ", res.String())
	return nil
}

func (e *Esearch) BatchSend(index string, docType string, content map[string]string) error {
	var buffer bytes.Buffer
	for id, v := range content {
		if docType == "" {
			buffer.WriteString(fmt.Sprintf(`{ "update" : {"_id" : "%s", "_index" : "%s"} }`,
				id,
				index,
			))
		} else {
			buffer.WriteString(fmt.Sprintf(`{ "update" : {"_id" : "%s", "_index" : "%s","_type":"%s"} }`,
				id,
				index,
				docType, //_doc
			))
		}
		doc := `{"doc_as_upsert":true}`
		doc, _ = sjson.SetRaw(doc, "doc", v)
		buffer.WriteString("\n")
		buffer.WriteString(doc)
		buffer.WriteString("\n")
		buffer.WriteString("\n")
	}
	//向 Elasticsearch 发送批量操作（Bulk Request）
	req := esapi.BulkRequest{
		Index:        index,
		Body:         strings.NewReader(buffer.String()),
		DocumentType: "",
		Refresh:      "true",
	}

	// 执行 Index 请求
	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("Index request failed: %s", res.Status())
	}
	// 获取 Index 请求的响应结果
	fmt.Println(" 获取 Index 请求的响应结果: ", res.String())
	return nil
}

func (e *Esearch) BatchInsert(index string, content map[string]string) error {
	var buffer bytes.Buffer

	for _, doc := range content {
		// 每个doc必须在一行中，中间不能出现换行
		doc = strings.Replace(doc, "\n", "", -1)
		buffer.WriteString(`{"index":{}}`)
		buffer.WriteString("\n")
		buffer.WriteString(doc)
		buffer.WriteString("\n")
	}

	// 调用 Bulk API（无需多余路径或参数）
	res, err := e.Client.Bulk(bytes.NewReader(buffer.Bytes()), e.Client.Bulk.WithIndex(index))
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	////向 Elasticsearch 发送批量操作（Bulk Request）
	//req := esapi.BulkRequest{
	//	Index:        index,
	//	Body:         strings.NewReader(buffer.String()),
	//	Refresh: "true",
	//}
	//// 执行 Index 请求
	//res, err := req.Do(context.Background(), e.Client)
	//if err != nil {
	//	return err
	//}
	//defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("Index request failed: %s", res.Status())
	}
	// 获取 Index 请求的响应结果
	fmt.Println(" 获取 Index 请求的响应结果: ", res.String())
	return nil
}

// DeleteIndexesByPattern 批量模糊删除索引，pattern 支持通配符（如 "logs-*"）
func (e *Esearch) DeleteIndexesByPattern(pattern ...string) error {
	if e.Client == nil {
		return fmt.Errorf("elasticsearch client is nil")
	}

	// 构造 DeleteIndex 请求
	req := esapi.IndicesDeleteRequest{
		Index: pattern,
	}

	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return fmt.Errorf("failed to send delete index request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("delete index failed with status: %s", res.Status())
	}

	fmt.Println("Successfully deleted indexes matching pattern:", pattern)
	return nil
}

func (e *Esearch) DeleteDuplicateDoc(index, field string) error {
	return nil
}
