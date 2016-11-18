/* subtask_manager.go - manager of subtasks*/
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
)

import (
	. "types"
	"util"
)

type SubtaskManager struct {
	stLock   sync.Mutex
	subtasks map[uint64]*Subtask // all the subtasks

	pendingStLock   sync.Mutex
	pendingSubtasks map[uint64]*Subtask // all the pending subtasks

	underAllocStLock   sync.Mutex
	underAllocSubtasks map[uint64]*Subtask // all the under alloc subtasks

	toUpdateStLock   sync.Mutex
	toUpdateSubtasks map[uint64]*Subtask // all the to update subtasks
}

func NewSubtaskManager() *SubtaskManager {
	m := new(SubtaskManager)

	m.subtasks = make(map[uint64]*Subtask)
	m.pendingSubtasks = make(map[uint64]*Subtask)
	m.underAllocSubtasks = make(map[uint64]*Subtask)

	return m
}

func HasSubtask(subtaskId uint64) bool {
	_, exist := stateManager.subtaskManager.subtasks[subtaskId]

	return exist
}

func CreateSubtask(task *Task) *Subtask {
	st := NewSubtask(task)

	st.Task = task

	// Generate an id
	st.SubtaskId = util.GenerateUid()
	for HasSubtask(st.SubtaskId) {
		st.SubtaskId = util.GenerateUid()
	}

	return st
}

func GetAllSubtasks() map[uint64]*Subtask {
	return stateManager.subtaskManager.subtasks
}

func AddSubtasks(subtasks map[uint64]*Subtask) {
	for id, subtask := range subtasks {
		// all subtasks table
		stateManager.subtaskManager.subtasks[id] = subtask

		// record maps
		stateManager.mapsManager.recordSubtaskMaps(subtask)
	}
}

func SubmitFinishSubtask(subtask *Subtask) {
	// delete from all subtasks index
	delete(stateManager.subtaskManager.subtasks, subtask.SubtaskId)

	// remove all the maps
	stateManager.mapsManager.clearSubtaskMaps(subtask)
}

func GetSubtaskById(subtaskId uint64) *Subtask {
	subtask, _ := stateManager.subtaskManager.subtasks[subtaskId]

	return subtask
}
