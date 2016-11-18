/* task_api.go - api for tasks */
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
*/

package api_server

import (
	"time"
)

import (
	"scheduler"
	"state_manager"
	. "types"
	"www.baidu.com/golang-lib/log"
)

type SubmitTaskRequest struct {
	SrcUrl       string
	DstHostname  string
	Deadline     time.Time
	Mode         int
	Cmd          int
	Priority     int
	Product      string
	IsSensitive  bool
	IsCompressed bool
	TimeToLive   time.Time
}

type SubmitTaskResponse struct {
	RetCode int // 0: OK; non-0 otherwise
	ErrMsg  string
}

type QueryConfRequest struct {
	AgentId		uint64
	AgentName	string
//	TimeToLive	time.Time
}

type QueryConfResponse struct {
	SubTaskSet	[]*Subtask
	ErrMsg		string
	ControllerState int
}

type StateReportRequest struct {
	AgentId		uint64
	AgentName	string
	finishBlks	map[uint64]map[int]bool
	finishTime	map[uint64]time.Time //subtaskId-->Finish Timestamp
//	TimeToLive	time.Time
}

type StateReportResponse struct {
	ErrMsg  string
}
// a new task event will be added to the pending queue of the scheduler
func (server *ApiServer) SubmitTask(req *SubmitTaskRequest, res *SubmitTaskResponse) error {
	// create the new task event
	log.Logger.Info("Receive an event from %s",req.DstHostname)
	e := NewTaskEvent{
		ArriveTime:   time.Now(),
		SrcUrl:       req.SrcUrl,
		DstHostname:  req.DstHostname,
		Deadline:     req.Deadline,
		Mode:         req.Mode,
		Cmd:          req.Cmd,
		Priority:     req.Priority,
		Product:      req.Product,
		IsSensitive:  req.IsSensitive,
		IsCompressed: req.IsCompressed,
		TimeToLive:   req.TimeToLive,
	}

	err := scheduler.AppendEvent(e)
	if err == nil {
		res.RetCode = 0
		res.ErrMsg = ""
	} else {
		res.RetCode = 1
		res.ErrMsg = err.Error()
	}

	return nil
}

func (server *ApiServer) QueryConf(req *QueryConfRequest, res *QueryConfResponse) error {
	log.Logger.Info("Receive an requst from %s",req.AgentName)
	agent := state_manager.GetAgentByName(req.AgentName)
	if agent == nil{
		log.Logger.Info("QueryConf agent nil")
		res.ErrMsg = "agent nil"
		return nil
	}
	agentId := agent.AgentId
	log.Logger.Info("QueryConf agentId %d ", agentId)
//	log.Logger.Info("The QuiryConf agent is %s, its ID is: %d", req.AgentName, agentId)
	existSubtasks := state_manager.GetAgentDownloadSubtasks(agentId)
	res.SubTaskSet = existSubtasks
	res.ControllerState = state_manager.GetState()
	log.Logger.Info("#######################")
/*
	err := scheduler.AppendEvent(e)
	if err == nil {
		res.RetCode = 0
		res.ErrMsg = ""
	} else {
		res.RetCode = 1
		res.ErrMsg = err.Error()
	}
*/
	return nil
}
func (server *ApiServer) StateReport(req *StateReportRequest, res *StateReportResponse) error {
	for subtaskId, subtasks := range req.finishBlks {
		event := SubtaskFinishBlocksEvent{req.finishTime[subtaskId], subtaskId, subtasks}
		err := scheduler.AppendEvent(event)
		if err != nil {
			res.ErrMsg = err.Error()
		}
	}
	return nil
}
