package model

type ConsumerMsg struct {
	Value     []byte
	Partition int32
	Offset    int64
}
