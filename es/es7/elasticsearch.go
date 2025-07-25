package es7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/jifuy/commongo/es/esutil"
	"github.com/jifuy/commongo/loging"
	"github.com/tidwall/sjson"
	"io"
	"strings"
	"time"
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
	loging.Debug("ES连接成功！", q1)
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
	loging.Debug("resp:", string(rspBody))
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
func (e *Esearch) ListIndexesByPattern(pattern []string) ([]string, error) {
	if e.Client == nil {
		return nil, fmt.Errorf("elasticsearch client is nil")
	}

	// 构造 CatIndicesRequest 请求，并设置 index 参数为 pattern
	req := esapi.CatIndicesRequest{
		Format: "json",
		H:      []string{"index"},
		Index:  pattern, // 支持通配符匹配
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
			partIndexes, err := e.ListIndexesByPattern([]string{part})
			if err != nil {
				return fmt.Errorf("failed to list all indexes: %s,%w", part, err)
			}
			allIndexes = append(allIndexes, partIndexes...)
		} else {
			allIndexes = append(allIndexes, part)
		}
	}

	if len(allIndexes) == 0 {
		loging.Debug("No indexes matched the pattern:", pattern)
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
		// 如果错误是 "index_not_found_exception"（HTTP 404），则不报错
		if res.StatusCode == 404 {
			loging.Warn("Index not found, skip deletion.")
			return nil
		}
		// 其他错误仍然返回
		return fmt.Errorf("delete index failed with status: %v", res)
	}

	loging.Debugf("Successfully deleted indexes matching pattern '%s': %v", pattern, allIndexes)
	return nil
}

func (e *Esearch) DeleteDuplicateDoc(index, field string) error {
	duplicateMap, err := e.FindDuplicateDocs(index, field)
	if err != nil {
		return err
	}
	var buffer bytes.Buffer
	for key, ids := range duplicateMap {
		keys := strings.Split(key, "::")
		_index := keys[0]
		for _, id := range ids {
			meta := fmt.Sprintf(`{ "delete" : { "_index" : "%s", "_id" : "%s" } }%s`, _index, id, "\n")
			buffer.WriteString(meta)
		}
	}
	err = e.BulkReq(buffer.String())
	if err != nil {
		return err
	}
	return nil
}

func (e *Esearch) FindDuplicateDocs(index, field string) (map[string][]string, error) {
	// 找出所有重复的 doc_id 值
	query := fmt.Sprintf(`{"size": 0,"aggs":{"duplicate_doc_ids":{"terms":{"field":"%s.keyword","min_doc_count":2,"size":100000}}}}`, field)
	loging.Debug("duplicates req:", query)
	duplicates, err := e.EsSearch([]string{index}, query)
	if err != nil {
		return nil, err
	}
	docids := []string{}
	for _, d := range duplicates.Aggregations.DuplicateDocIds.Buckets {
		docids = append(docids, d.Key)
	}
	if len(docids) == 0 {
		return nil, fmt.Errorf("no duplicate docs found")
	}
	// 按重复的 doc_id 分组，保留最新文档
	query2 := fmt.Sprintf(`{"query":{"terms":{"%s":[]}},"sort":[{"timestamp":{"order":"desc"}}]}`, field)
	query3, err := sjson.Set(query2, fmt.Sprintf("query.terms.%s", field), docids)
	if err != nil {
		return nil, err
	}
	loging.Debug("duplicates docs req:", query3)
	resp, err := e.EsSearch([]string{index}, query3)

	duplicateMap := make(map[string][]string, 0)
	for _, d := range resp.Hits.Hits {
		val := d.Source[field]
		id := d.Id
		key := d.Index + "::" + fmt.Sprintf("%v", val)
		duplicateMap[key] = append(duplicateMap[key], id)
	}
	if len(duplicateMap) == 0 {
		return nil, fmt.Errorf("no duplicateMap found")
	}
	// 保留最新
	for k, v := range duplicateMap {
		duplicateMap[k] = v[1:]
	}
	loging.Debug("duplicateMap:", duplicateMap)
	return duplicateMap, nil
}

func (e *Esearch) BulkReq(content string) error {
	// Step 3: 发送 Bulk Delete 请求
	req := esapi.BulkRequest{
		Body: strings.NewReader(content),
	}
	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return fmt.Errorf("bulk delete failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk delete request failed with status: %s", res.Status())
	}

	loging.Debug("Successfully deleted duplicate documents.")
	return nil
}

// MergeIndexes 合并多个源索引到一个目标索引
func (e *Esearch) MergeIndexes(sourceIndexes []string, targetIndex string, startTime, endTime string) error {
	if e.Client == nil {
		return fmt.Errorf("elasticsearch client is nil")
	}

	// 构建 Reindex body
	body := map[string]interface{}{
		"source": map[string]interface{}{
			"index": sourceIndexes,
			"query": map[string]interface{}{
				"range": map[string]interface{}{
					"timestamp": map[string]string{
						"gte": startTime,
						"lt":  endTime,
					},
				},
			},
		},
		"dest": map[string]interface{}{
			"index": targetIndex,
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return fmt.Errorf("failed to encode reindex body: %w", err)
	}

	// 发送 Reindex 请求
	res, err := e.Client.Reindex(
		&buf,
		e.Client.Reindex.WithRefresh(true),
	)
	if err != nil {
		return fmt.Errorf("reindex request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("reindex failed with status: %v", res)
	}

	loging.Debugf("Successfully merged indexes %v into %s", sourceIndexes, targetIndex)
	return nil
}

// SplitIndexByTimestampRange 根据 timestamp 字段的时间范围拆分数据到不同索引
func (e *Esearch) SplitIndexByTimestampRange(sourceIndex string, targetIndexes []string) error {
	if e.Client == nil {
		return fmt.Errorf("elasticsearch client is nil")
	}

	for _, targetIndex := range targetIndexes {
		// 从目标索引名提取日期，例如 my_index_20250801 → 2025-08-01
		dateStr := strings.TrimPrefix(targetIndex, "my_index_") // 假设前缀是固定的
		if len(dateStr) != 8 {
			return fmt.Errorf("invalid target index name format: %s", targetIndex)
		}
		startDate := dateStr[:4] + "-" + dateStr[4:6] + "-" + dateStr[6:] // → 2025-08-01

		// 计算第二天的日期
		nextDate, err := time.Parse("2006-01-02", startDate)
		if err != nil {
			return fmt.Errorf("failed to parse date: %w", err)
		}
		endDate := nextDate.AddDate(0, 0, 1).Format("2006-01-02") // → 2025-08-02

		// 构建带 range 查询的 reindex body
		body := map[string]interface{}{
			"source": map[string]interface{}{
				"index": []string{sourceIndex},
				"query": map[string]interface{}{
					"range": map[string]interface{}{
						"timestamp": map[string]string{
							"gte": startDate + "T00:00:00",
							"lt":  endDate + "T00:00:00",
						},
					},
				},
			},
			"dest": map[string]interface{}{
				"index": targetIndex,
			},
		}

		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return fmt.Errorf("failed to encode reindex body: %w", err)
		}

		// 发送 Reindex 请求
		res, err := e.Client.Reindex(
			&buf,
			e.Client.Reindex.WithRefresh(true),
		)
		if err != nil {
			return fmt.Errorf("reindex request failed for target %s: %w", targetIndex, err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return fmt.Errorf("reindex failed for target %s with status: %s", targetIndex, res.Status())
		}

		fmt.Printf("Successfully split index %s into %s with timestamp >= %sT00:00:00 and < %sT00:00\n",
			sourceIndex, targetIndex, startDate, endDate)
	}

	return nil
}
