/* task_manager.go - manager of tasks*/
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
	"sync"
	//  "time"
)

import (
//"www.baidu.com/golang-lib/log"
)

import (
	. "types"
	"util"
)

type TaskManager struct {
	taskLock sync.RWMutex
	tasks    map[uint64]*Task // task id => task
}

func NewTaskManager() *TaskManager {
	m := new(TaskManager)

	m.tasks = make(map[uint64]*Task)

	return m
}

func (tm *TaskManager) hasTask(id uint64) bool {
	tm.taskLock.RLock()
	defer tm.taskLock.RUnlock()

	_, exist := tm.tasks[id]
	return exist
}

func (tm *TaskManager) addTask(t *Task) {
	tm.taskLock.Lock()
	defer tm.taskLock.Unlock()

	tm.tasks[t.TaskId] = t
}

func CreateTask(e NewTaskEvent, data *Data, dst *Agent) *Task {
	newtask := NewTask(e.SrcUrl, dst, e.Mode, e.Cmd, e.Product, e.Priority, e.Deadline,
		e.IsSensitive, e.IsCompressed, e.TimeToLive, data)

	newtask.Origin = GetAgentByName(data.OriginAgentName)

	// Set taskid
	uid := util.GenerateUid()
	for stateManager.taskManager.hasTask(uid) {
		uid = util.GenerateUid()
	}
	newtask.TaskId = uid

	// Store in the table
	stateManager.taskManager.addTask(newtask)
	stateManager.mapsManager.addAgent2Task(dst, newtask)

	return newtask
}
