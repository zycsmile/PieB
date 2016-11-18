/* data.go - the definition for data and block */
/*
modification history
--------------------
2015/4/22, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package types

type Block struct {
	Data       *Data
	BlockIndex int

	Size int64 // in Bytes
	Md5  uint32
}

type Data struct {
	DataId uint64

	OriginAgentName string
	OriginPath      string

	Size       int64 // in Bytes
	BlockSize  int64
	BlockCount int
	Blocks     []*Block
}
