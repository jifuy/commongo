package es

import (
	"github.com/jifuy/commongo/es/es7"
	"github.com/jifuy/commongo/es/es8"
	"github.com/jifuy/commongo/es/esutil"
)

type EsCli interface {
	EsSearch(indexes []string, query string) (esutil.ResponseBody, error)
	EsPost(index string, id string, content string) error
	BatchSend(index string, docType string, content map[string]string) error
}

func NewEsClient(config esutil.EsCfg) (EsCli, error) {
	switch config.Version {
	case 7:
		return es7.NewEsClient(config)
	case 8:
		return es8.NewEsClient(config)
	default:
		return es7.NewEsClient(config)
	}
}
