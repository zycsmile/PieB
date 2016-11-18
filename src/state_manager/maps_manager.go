package state_manager

import (
	"sync"
)
import (
    "www.baidu.com/golang-lib/log"
)
import (
	. "types"
)

type MapsManager struct {
	agent2TaskMap            map[uint64]map[uint64]*Task          // agent id => task id => task
	agent2UploadSubtaskMap   map[uint64]map[uint64]*Subtask       // agent id => upload subtask id => subtask
	agent2DownloadSubtaskMap map[uint64]map[uint64]*Subtask       // agent id => download subtask id => subtask
	agent2BlockMap           map[uint64]map[uint64]map[int]*Block // agent id => data id => block index => block

	idc2AgentMap map[string]map[uint64]*Agent // idc name => agent id => agent

	task2SubtaskMap map[uint64]map[uint64]*Subtask // task id=> subtask id=> subtask

	link2SubtaskMap map[string]map[uint64]*Subtask // link name => subtask id => subtask

	blockMapLock   sync.RWMutex
	block2AgentMap map[uint64]map[int]map[uint64]*Agent // data id => block index => agentid => agent
}

func NewMapsManager() *MapsManager {
	mm := new(MapsManager)

	mm.agent2TaskMap = make(map[uint64]map[uint64]*Task)
	mm.agent2UploadSubtaskMap = make(map[uint64]map[uint64]*Subtask)
	mm.agent2DownloadSubtaskMap = make(map[uint64]map[uint64]*Subtask)
	mm.agent2BlockMap = make(map[uint64]map[uint64]map[int]*Block)

	mm.idc2AgentMap = make(map[string]map[uint64]*Agent)

	mm.task2SubtaskMap = make(map[uint64]map[uint64]*Subtask)

	mm.link2SubtaskMap = make(map[string]map[uint64]*Subtask)

	mm.block2AgentMap = make(map[uint64]map[int]map[uint64]*Agent)

	return mm
}

func GetAgentUploadSubtasks(agentId uint64) []*Subtask {
	stateManager.mapsManager.blockMapLock.RLock()
	defer stateManager.mapsManager.blockMapLock.RUnlock()
	if _, exist := stateManager.mapsManager.agent2UploadSubtaskMap[agentId]; !exist {
		stateManager.mapsManager.agent2UploadSubtaskMap[agentId] = make(map[uint64]*Subtask)
	}

	// TODO: should be optimized
	ret := make([]*Subtask, 0)
	for _, subtask := range stateManager.mapsManager.agent2UploadSubtaskMap[agentId] {
		ret = append(ret, subtask)
	}

	return ret
}

func GetAgentDownloadSubtasks(agentId uint64) []*Subtask {
	stateManager.mapsManager.blockMapLock.RLock()
	defer stateManager.mapsManager.blockMapLock.RUnlock()
	ret := make([]*Subtask, 0)
	if _, exist := stateManager.mapsManager.agent2DownloadSubtaskMap[agentId]; !exist {
		stateManager.mapsManager.agent2DownloadSubtaskMap[agentId] = make(map[uint64]*Subtask)
		return  ret
	}

	// TODO: should be optimized
	log.Logger.Info("stateManager.mapsManager.agent2DownloadSubtaskMap len %d",len(stateManager.mapsManager.agent2DownloadSubtaskMap[agentId]))
	for _, subtask := range stateManager.mapsManager.agent2DownloadSubtaskMap[agentId] {
		ret = append(ret, subtask)
	}
	return ret
}

func GetLinkSubtasks(link *IdcLink) map[uint64]*Subtask {
	stateManager.mapsManager.blockMapLock.RLock()
	defer stateManager.mapsManager.blockMapLock.RUnlock()
	if _, exist := stateManager.mapsManager.link2SubtaskMap[link.LinkName]; !exist {
		stateManager.mapsManager.link2SubtaskMap[link.LinkName] = make(map[uint64]*Subtask)
	}
	return stateManager.mapsManager.link2SubtaskMap[link.LinkName]
}

func (mm *MapsManager) addIdc2Agent(idc *Idc, agent *Agent) {
	mm.blockMapLock.Lock()
    defer mm.blockMapLock.Unlock()
	_, exist := mm.idc2AgentMap[idc.IdcName]
	if !exist {
		mm.idc2AgentMap[idc.IdcName] = make(map[uint64]*Agent)
	}

	mm.idc2AgentMap[idc.IdcName][agent.AgentId] = agent
}

func (mm *MapsManager) addAgent2Task(agent *Agent, task *Task) {
	mm.blockMapLock.Lock()
    defer mm.blockMapLock.Unlock()
	_, exist := mm.agent2TaskMap[agent.AgentId]
	if !exist {
		mm.agent2TaskMap[agent.AgentId] = make(map[uint64]*Task)
	}

	mm.agent2TaskMap[agent.AgentId][task.TaskId] = task
}

func (mm *MapsManager) recordSubtaskMaps(subtask *Subtask) {
	mm.blockMapLock.Lock()
    defer mm.blockMapLock.Unlock()
	// agent 2 upload subtasks
	_, exist := mm.agent2UploadSubtaskMap[subtask.Src.AgentId]
	if !exist {
		mm.agent2UploadSubtaskMap[subtask.Src.AgentId] = make(map[uint64]*Subtask)
	}
	mm.agent2UploadSubtaskMap[subtask.Src.AgentId][subtask.SubtaskId] = subtask

	// agent 2 download subtasks
	_, exist = mm.agent2DownloadSubtaskMap[subtask.Dst.AgentId]
	if !exist {
		mm.agent2DownloadSubtaskMap[subtask.Dst.AgentId] = make(map[uint64]*Subtask)
	}
	mm.agent2DownloadSubtaskMap[subtask.Dst.AgentId][subtask.SubtaskId] = subtask

	// task 2 subtasks
	_, exist = mm.task2SubtaskMap[subtask.Task.TaskId]
	if !exist {
		mm.task2SubtaskMap[subtask.Task.TaskId] = make(map[uint64]*Subtask)
	}
	mm.task2SubtaskMap[subtask.Task.TaskId][subtask.SubtaskId] = subtask

	// link 2 subtasks
	for _, link := range subtask.Links {
		_, exist = mm.link2SubtaskMap[link.LinkName]
		if !exist {
			mm.link2SubtaskMap[link.LinkName] = make(map[uint64]*Subtask)
		}
		mm.link2SubtaskMap[link.LinkName][subtask.SubtaskId] = subtask
	}
}

func (mm *MapsManager) clearSubtaskMaps(subtask *Subtask) {
	// agent 2 upload subtasks
	mm.blockMapLock.RLock()
    defer mm.blockMapLock.RUnlock()
	delete(mm.agent2UploadSubtaskMap[subtask.Src.AgentId], subtask.SubtaskId)

	// agent 2 download subtasks
	delete(mm.agent2DownloadSubtaskMap[subtask.Src.AgentId], subtask.SubtaskId)

	// task 2 subtasks
	delete(mm.task2SubtaskMap[subtask.Task.TaskId], subtask.SubtaskId)

	// link 2 subtasks
	for _, link := range subtask.Links {
		delete(mm.link2SubtaskMap[link.LinkName], subtask.SubtaskId)
	}
}

// map all the blocks of a data to an agent
func (m *MapsManager) addDataLocation(data *Data, agent *Agent) {
	m.blockMapLock.Lock()
	defer m.blockMapLock.Unlock()

	// fill data id in block2AgentMap if not exist
	if _, exist := m.block2AgentMap[data.DataId]; !exist {
		m.block2AgentMap[data.DataId] = make(map[int]map[uint64]*Agent)
//        log.Logger.Info("make map for DataId:%d on agent:%s",data.DataId, agent.AgentName)
	}

	// add block locations
	for i := 0; i < data.BlockCount; i++ {
		_, exist := m.block2AgentMap[data.DataId][i]
		if !exist {
			m.block2AgentMap[data.DataId][i] = make(map[uint64]*Agent)
		}
		m.block2AgentMap[data.DataId][i][agent.AgentId] = agent
	}
}

func AddBlockLocation(dataId uint64, blockIndex int, agent *Agent) {
	m := stateManager.mapsManager

	m.blockMapLock.RLock()
	defer m.blockMapLock.RUnlock()

	// fill data id in block2AgentMap if not exist
	if _, exist := m.block2AgentMap[dataId]; !exist {
		m.block2AgentMap[dataId] = make(map[int]map[uint64]*Agent)
	}

	// fill block key
	if _, exist := m.block2AgentMap[dataId][blockIndex]; !exist {
		m.block2AgentMap[dataId][blockIndex] = make(map[uint64]*Agent)
	}

	// add block locations
	m.block2AgentMap[dataId][blockIndex][agent.AgentId] = agent
}

func GetBlockLocations(dataId uint64, blockIndex int) []*Agent {
	stateManager.mapsManager.blockMapLock.RLock()
	defer stateManager.mapsManager.blockMapLock.RUnlock()

	// Check if data in map
	block2AgentMap, exist := stateManager.mapsManager.block2AgentMap[dataId]
	if !exist {
		return nil
	}

	// Check if block in map
	agentMap, exist := block2AgentMap[blockIndex]
	if !exist {
		return nil
	}

	// Prepare the result
	agents := make([]*Agent, 0)
	for _, agent := range agentMap {
		agents = append(agents, agent)
	}

	return agents
}
