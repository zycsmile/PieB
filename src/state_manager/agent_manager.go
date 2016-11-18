/* agent_manager.go - manager of agents*/
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package state_manager

import (
	"fmt"
	"strings"
	"sync"
)

import (
	. "types"
	"util"
)

var (
	DefaultAgentServePort  = 6666
	DefaultAgentConfigPort = 6668
	DefaultUploadLimit     = int64(6 * 1024)  // KB/s
	DefaultDownloadLimit   = int64(6 * 1024) // KB/s
)

type AgentManager struct {
	idMaplock  sync.RWMutex
	agentIdMap map[uint64]*Agent // agent id => agent

	nameMaplock  sync.RWMutex
	agentNameMap map[string]*Agent // agent name => agent
}

func NewAgentManager() *AgentManager {
	m := new(AgentManager)

	m.agentIdMap = make(map[uint64]*Agent)
	m.agentNameMap = make(map[string]*Agent)

	return m
}

func (am *AgentManager) getAgentByName(agentName string) *Agent {
	am.nameMaplock.RLock()
	defer am.nameMaplock.RUnlock()

	agent, _ := am.agentNameMap[agentName]

	return agent
}

func GetAgentById(agentId uint64) *Agent {
	agent, _ := stateManager.agentManager.agentIdMap[agentId]

	return agent
}

func GetAgentByName(agentName string) *Agent {
	agent, _ := stateManager.agentManager.agentNameMap[agentName]

	return agent
}

func (am *AgentManager) hasAgent(agentId uint64) bool {
	am.idMaplock.RLock()
	defer am.idMaplock.RUnlock()

	_, exist := am.agentIdMap[agentId]

	return exist
}

func (am *AgentManager) addAgent(agent *Agent) {
	am.idMaplock.Lock()
	am.agentIdMap[agent.AgentId] = agent
	am.idMaplock.Unlock()

	am.nameMaplock.Lock()
	am.agentNameMap[agent.AgentName] = agent
	am.nameMaplock.Unlock()
}

// TODO: load the config for default agent
func CreateAgentWithDefaultConfig(agentname string, agentType AgentType) (*Agent, error) {
	am := stateManager.agentManager

	agent := NewAgent()

	agent.AgentName = agentname

	// Generate an id
	agent.AgentId = util.GenerateUid()
	for am.hasAgent(agent.AgentId) {
		agent.AgentId = util.GenerateUid()
	}

	// Ports
	agent.ServePort = DefaultAgentServePort
	agent.ConfigPort = DefaultAgentConfigPort

	// Rate Limit
	agent.UploadLimit = DefaultUploadLimit
	agent.UploadFree = DefaultUploadLimit
	agent.DownloadLimit = DefaultDownloadLimit
	agent.DownloadFree = DefaultDownloadLimit

	// Agent type
	agent.AgentType = agentType

	// Agent idc
	agent.Idc = parseIdcFromAgentname(agent.AgentName, agent.AgentType)
	if agent.Idc == nil {
		return nil, fmt.Errorf("unknown idc for the agent")
	}

	// Add to map
	stateManager.agentManager.addAgent(agent)
	stateManager.mapsManager.addIdc2Agent(agent.Idc, agent)

	return agent, nil
}

func ParseAgentTypeFromPath(path string) AgentType {
	pathStrs := strings.Split(path, ":")
	switch pathStrs[0] {
	case "hdfs":
		return AGENT_TYPE_HDFS
	case "nfs":
		return AGENT_TYPE_NFS
	// TODO: not really correct
	case "gko", "http", "ftp":
		return AGENT_TYPE_HOST
	default:
		return AGENT_TYPE_UNKNOWN
	}
}
