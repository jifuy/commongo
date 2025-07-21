package esutil

type ResponseBody struct {
	Hits         DocObjects             `json:"hits"`
	Aggregations Aggregations           `json:"aggregations,omitempty"`
	Status       int                    `json:"status,omitempty"`
	Error        map[string]interface{} `json:"error,omitempty"`
}

type DocObjects struct {
	Hits []SourceObject `json:"hits"`
}
type SourceObject struct {
	Index  string                 `json:"_index"`
	Id     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

type Aggregations struct {
	DuplicateDocIds DocIds `json:"duplicate_doc_ids"`
}

type DocIds struct {
	Buckets []aggregationBucket `json:"buckets"`
}

type aggregationBucket struct {
	Key   string `json:"key"`
	Count int    `json:"doc_count"`
}

type EsCfg struct {
	Addresses []string
	UserName  string
	PassWord  string
	Version   int
	DocType   string
}
