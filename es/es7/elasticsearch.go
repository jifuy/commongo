package es7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/jifuy/commongo/es/esutil"
	logging "github.com/jifuy/commongo/loging"
	"github.com/tidwall/sjson"
	"io"
	"strings"
)

var ESClient *Esearch

type Esearch struct {
	Client *elasticsearch.Client
}

func NewEsClient(config esutil.EsCfg) *Esearch {
	cfg := elasticsearch.Config{
		Addresses:  config.Addresses,
		MaxRetries: 3,
	}
	cfg.Username = config.UserName
	cfg.Password = config.PassWord
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		logging.Error("ES连接失败！", err)
		return nil
	}
	q1, err := es.Info()
	logging.Debug("ES连接成功！", q1)
	ESClient = &Esearch{
		Client: es,
	}
	return ESClient
}

func (e *Esearch) EsSearch(indexes []string, query string) (esutil.ResponseBody, error) {
	var rsp esutil.ResponseBody
	res, err := e.Client.Search(
		e.Client.Search.WithIndex(indexes...),
		e.Client.Search.WithBody(bytes.NewReader([]byte(query))),
		e.Client.Search.WithPretty(),
	)
	if err != nil {
		logging.ErrorF("搜索失败：%s ", err)
		return rsp, err
	}
	defer res.Body.Close()
	rspBody, err := io.ReadAll(res.Body)

	if err := json.Unmarshal(rspBody, &rsp); err != nil {
		return rsp, fmt.Errorf("failed to unmarshal response body: %w", err)
	}
	logging.Debug("rsp---------", string(rspBody))
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
		logging.Debug("执行 Index 请求", err)
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("Index request failed: %s", res.Status())
	}
	// 获取 Index 请求的响应结果
	logging.Debug(" 获取 Index 请求的响应结果: ", res.String())
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
		logging.Debug("执行 Index 请求", err)
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("Index request failed: %s", res.Status())
	}
	// 获取 Index 请求的响应结果
	logging.Debug(" 获取 Index 请求的响应结果: ", res.String())
	return nil
}
