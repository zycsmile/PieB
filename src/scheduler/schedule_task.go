/* schedule_task.go - func to schedule task*/
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
This includes a choose with heuristic
*/
package scheduler

import (
	"container/list"
	"fmt"
	"math"
	"time"
)

import (
	"www.baidu.com/golang-lib/log"
)

import (
	"meta_service"
	"state_manager"
	. "types"
)

const (
	BW_BUFF_FACTOR = 0.1
)

func (s *Scheduler) processPendingTasks(pendingTaskQ *list.List) {
	for e := pendingTaskQ.Front(); e != nil; e = e.Next() {
		event := e.Value.(NewTaskEvent)
		fmt.Printf("process new subtask event at %v: host %s requires data %s     before %v\n", event.ArriveTime, event.DstHostname, event.SrcUrl, event.Deadline)
		log.Logger.Info("process new subtask event at %v: host %s requires data %s before %v", event.ArriveTime, event.DstHostname, event.SrcUrl, event.Deadline)
		t, err := s.preProcessNewTaskEvent(event)
		if err != nil {
			// TODO: do something here
			log.Logger.Error("err in pre-process new task event:%s", err.Error())
			continue
		}
		s.processPendingTask(t)
	}
}

func (s *Scheduler) processPendingTask(task *Task) {
	dataId := task.Data.DataId
	blks := task.RemainBlocks
	var err error

	for _, blk := range blks {
		// Generate a new subtask
		st := state_manager.CreateSubtask(task)
		st.RemainBlocks = []*Block{blk}
		st.ReqBW = calcReqBW(blk.Size, st.Deadline)
		st.Laxity = st.Deadline - int64(blk.Size/st.Dst.DownloadLimit)

		// Get all the candidates
		candidates := state_manager.GetBlockLocations(dataId, blk.BlockIndex)
		if len(candidates) == 0 {
			// TODO: Add pending subtask
			log.Logger.Error("no candidate found for data[%d], block[%d]", dataId, blk.BlockIndex)
			continue
		}

		// pick a src
		src := s.pickSource(st, candidates)
		if src == nil {
			// TODO: do something here
			log.Logger.Error("no available src found for data[%d], block[%d]", dataId, blk.BlockIndex)
			continue
		}
		st.Src = src

		// set links
		if st.Src.Idc != st.Dst.Idc {
			st.Links, err = state_manager.GetIdcPath(st.Src.Idc, st.Dst.Idc)
			if err != nil {
				log.Logger.Error("err in get idc path:%s", err.Error())
				continue
			}
		}

		// record a new pending subtask
		if _, exist := s.pendingSubtasks[st.Src.AgentId]; !exist {
			s.pendingSubtasks[st.Src.AgentId] = make([]*Subtask, 0)
		}
		s.pendingSubtasks[st.Src.AgentId] = append(s.pendingSubtasks[st.Src.AgentId], st)
		// s.pendingSubtasks[st.SubtaskId] = st
	}

	return
}

func (s *Scheduler) pickSource(st *Subtask, candidates []*Agent) (src *Agent) {
	// classify srcs by distance
	sameIdcSrcs, sameRegionSrcs, crossRegionSrcs := s.classifyCandidateByDistance(st.Dst, candidates)

	// Pick same idc srcs
	if len(sameIdcSrcs) > 0 {
		return s.findMostFreeCandidate(sameIdcSrcs)
	}

	// Pick same region srcs
	if len(sameRegionSrcs) > 0 {
		return s.findMostFreeCandidate(sameRegionSrcs)
	}

	// Check cross region srcs
	if len(crossRegionSrcs) > 0 {
		return s.findMostFreeCandidate(crossRegionSrcs)
	}

	return nil
}

func (s *Scheduler) findMostFreeCandidate(candidates map[uint64]*Agent) *Agent {
	var bestSrc *Agent
	bestSrcNum := math.MaxInt32

	if len(candidates) > 0 {
		for _, src := range candidates {
			existSts := state_manager.GetAgentUploadSubtasks(src.AgentId)
			pendingSts, _ := s.pendingSubtasks[src.AgentId]
			totalStsNum := len(existSts) + len(pendingSts)

//            log.Logger.Info("totalStsNum of %s is:%d", src.AgentName, totalStsNum)

			if totalStsNum < bestSrcNum {
				bestSrcNum = totalStsNum
				bestSrc = src
			}
		}

//        log.Logger.Info("bestSrc is:%s", bestSrc.AgentName)

		return bestSrc
	}

	return nil
}

// record data, src agent, dst agent if neccessary, and create new task
func (s *Scheduler) preProcessNewTaskEvent(e NewTaskEvent) (*Task, error) {
	// Get meta of the data pointed by the srcUrl
	dataMeta, err := meta_service.GetDataMeta(e.SrcUrl)
	if err != nil {
		return nil, err
	}

	// If srcUrl points to a new src node, add the src node as agent
	srcAgent := state_manager.GetAgentByName(dataMeta.OriginAgentName)
	if srcAgent == nil {
		// create agent
		agentType := state_manager.ParseAgentTypeFromPath(dataMeta.OriginPath)
		srcAgent, err = state_manager.CreateAgentWithDefaultConfig(dataMeta.OriginAgentName, agentType)
		if err != nil {
			return nil, fmt.Errorf("err in create agent for %s: %s", dataMeta.OriginAgentName, err.Error())
		}
	}

	// If the dst is a new host, add the dst host as agent
	dstAgent := state_manager.GetAgentByName(e.DstHostname)
	if dstAgent == nil {
		// create agent
		dstAgent, err = state_manager.CreateAgentWithDefaultConfig(e.DstHostname, AGENT_TYPE_HOST)
		if err != nil {
			return nil, fmt.Errorf("err in create agent for %s: %s", e.DstHostname, err.Error())
		}
	}

	// If this is a new data, register data
	if !state_manager.HasData(dataMeta.DataId) {
		state_manager.AddData(dataMeta, srcAgent)
	}

	// Create task
	task := state_manager.CreateTask(e, dataMeta, dstAgent)

	return task, nil
}

// classify candidates by distance type
func (s *Scheduler) classifyCandidateByDistance(dstAgent *Agent, candidates []*Agent) (sameIdcSrcs map[uint64]*Agent,
	sameRegionSrcs map[uint64]*Agent, crossRegionSrcs map[uint64]*Agent) {

	sameIdcSrcs = make(map[uint64]*Agent)
	sameRegionSrcs = make(map[uint64]*Agent)
	crossRegionSrcs = make(map[uint64]*Agent)

	for _, srcAgent := range candidates {
		distanceType := state_manager.GetAgentsDistanceType(dstAgent, srcAgent)

		switch distanceType {
		case AGENT_DISTANCE_SAMEIDC:
			sameIdcSrcs[srcAgent.AgentId] = srcAgent
		case AGENT_DISTANCE_SAMEREGION:
			sameRegionSrcs[srcAgent.AgentId] = srcAgent
		case AGENT_DISTANCE_CROSSREGION:
			crossRegionSrcs[srcAgent.AgentId] = srcAgent
		}
	}

	return
}

func calcReqBW(size int64, deadline int64) int64 {
	remainTime := deadline - time.Now().Unix()
	if remainTime <= 0 {
		remainTime = 1
	}

	// size is in Bytes, rate is in KB/s
	reqBw := int64(float64(size/remainTime/1024) * (1 + BW_BUFF_FACTOR))

	if size > 0 && reqBw == 0 {
		reqBw = 1
	}

	return reqBw
}
