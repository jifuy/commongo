package es7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
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

// ListIndexesByPattern 模糊查询匹配的索引列表，pattern 支持通配符，如 "test-202506*"
func (e *Esearch) ListIndexesByPattern(pattern string) ([]string, error) {
	if e.Client == nil {
		return nil, fmt.Errorf("elasticsearch client is nil")
	}

	// 构造 CatIndicesRequest 请求，并设置 index 参数为 pattern
	req := esapi.CatIndicesRequest{
		Format: "json",
		H:      []string{"index"},
		Index:  []string{pattern}, // 支持通配符匹配
	}

	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to send list indexes request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("list indexes failed with status: %s", res.Status())
	}

	var indices []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		return nil, fmt.Errorf("failed to decode response body: %w", err)
	}

	indexNames := make([]string, 0, len(indices))
	for _, idx := range indices {
		if index, ok := idx["index"].(string); ok {
			indexNames = append(indexNames, index)
		}
	}
	return indexNames, nil
}

// DeleteIndexesByPattern 根据模式匹配并删除索引
func (e *Esearch) DeleteIndexesByPattern(pattern ...string) error {
	if e.Client == nil {
		return fmt.Errorf("elasticsearch client is nil")
	}

	// 1. 获取所有索引
	var allIndexes []string
	for _, part := range pattern {
		if strings.Contains(part, "*") {
			partIndexes, err := e.ListIndexesByPattern(part)
			if err != nil {
				return fmt.Errorf("failed to list all indexes: %s,%w", part, err)
			}
			allIndexes = append(allIndexes, partIndexes...)
		} else {
			allIndexes = append(allIndexes, part)
		}
	}

	if len(allIndexes) == 0 {
		fmt.Println("No indexes matched the pattern:", pattern)
		return nil
	}

	// 3. 批量删除匹配的索引
	req := esapi.IndicesDeleteRequest{
		Index: allIndexes,
	}

	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return fmt.Errorf("failed to send delete index request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("delete index failed with status: %s", res.Status())
	}

	fmt.Printf("Successfully deleted indexes matching pattern '%s': %v\n", pattern, allIndexes)
	return nil
}
