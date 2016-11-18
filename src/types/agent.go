/* agent.go - the definition and interface for agent */
/*
modification history
--------------------
2015/4/22, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package types

type AgentType int

const (
	AGENT_TYPE_HOST AgentType = iota
	AGENT_TYPE_HDFS
	AGENT_TYPE_NFS
	AGENT_TYPE_UNKNOWN
)

type AgentStatus int

const (
	AGENT_STATUS_LIVE AgentStatus = iota
	AGENT_STATUS_DEAD
)

type AgentDistanceType int

const (
	AGENT_DISTANCE_SAMEIDC AgentDistanceType = iota
	AGENT_DISTANCE_SAMEREGION
	AGENT_DISTANCE_CROSSREGION
	AGENT_DISTANCE_UNKNOWN
)

type Agent struct {
	AgentId   uint64 // unique identifier
	AgentName string // hostname or url path

	AgentType AgentType // 0: host; 1: hdfs; 2: nfs

	ServePort   int // the port to upload data
	ConfigPort  int // the port to config
	MonitorPort int // the port to collect state

	Idc *Idc // which IDC the agent locates in

	UploadLimit   int64 // upload rate limit, in KB/s
	UploadFree    int64 // upload rate free, in KB/s
	DownloadLimit int64 // download rate limit, in KB/s
	DownloadFree  int64 // download rate free, in KB/s

	Status AgentStatus // live or dead
}

func NewAgent() *Agent {
	a := new(Agent)

	return a
}
