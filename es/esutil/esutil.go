package esutil

import "encoding/json"

type ResponseBody struct {
	Hits         DocObjects                            `json:"hits"`
	Aggregations map[string]map[string]json.RawMessage `json:"aggregations,omitempty"`
	Status       int                                   `json:"status,omitempty"`
	Error        map[string]interface{}                `json:"error,omitempty"`
}
type DocObjects struct {
	Hits []SourceObject `json:"hits"`
}
type SourceObject struct {
	Source map[string]interface{} `json:"_source"`
}

type EsCfg struct {
	Addresses []string
	UserName  string
	PassWord  string
	Version   int
	DocType   string
}
