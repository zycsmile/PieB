/* merge_pending_subtasks.go - merge pending subtaks*/
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
Merge the blocks and alloc BW of pending subtasks
*/
package scheduler

import (
	"fmt"
	"math/rand"
)

import (
//"www.baidu.com/golang-lib/log"
)

import (
	"state_manager"
	. "types"
)

func (s *Scheduler) mergePendingSubtasks() {
	// group pending subtasks by key=[src, data, dst]
	pendingSubtaskGroups := groupPendingSubtasks(s.pendingSubtasks)

	// get existing subtasks which has the same [src, data, dst] as some pending subtask
	existingSubtaskGroups := s.getExistingSameGroupSubtask(pendingSubtaskGroups)

	for srcId, srcGroup := range pendingSubtaskGroups {
		for dataId, dataGroup := range srcGroup {
			for dstId, dstGroup := range dataGroup {
				// check whether has an existing subtask in the same group
				key := fmt.Sprintf("%d-%d-%d", srcId, dataId, dstId)
				if existSubtask, exist := existingSubtaskGroups[key]; exist {
					// merge all subtasks to existing subtask
					for i := 0; i < len(dstGroup); i++ {
						toRemoveSubtask := dstGroup[i]
						existSubtask.RemainBlocks = append(existSubtask.RemainBlocks, toRemoveSubtask.RemainBlocks[0])
						existSubtask.ReqBW += toRemoveSubtask.ReqBW
					}
				} else {
					// if no matching existing subtask, only keep the first subtask
					keptSubtask := dstGroup[0]
					for i := 1; i < len(dstGroup); i++ {
						toRemoveSubtask := dstGroup[i]
						keptSubtask.RemainBlocks = append(keptSubtask.RemainBlocks, toRemoveSubtask.RemainBlocks[0])
						keptSubtask.ReqBW += toRemoveSubtask.ReqBW
					}

					Shuffle(keptSubtask.RemainBlocks)

					s.mergedPendingSubtasks[keptSubtask.SubtaskId] = keptSubtask
				}
			}
		}
	}
}

func groupPendingSubtasks(pendingSubtasks map[uint64][]*Subtask) map[uint64]map[uint64]map[uint64][]*Subtask {
	subtaskGroups := make(map[uint64]map[uint64]map[uint64][]*Subtask)

	for srcAgentId, pendingSubtasks := range pendingSubtasks {
		subtaskGroups[srcAgentId] = make(map[uint64]map[uint64][]*Subtask)
		// group pending subtasks on the same upload agent by data and dst
		for _, subtask := range pendingSubtasks {
			if _, exist := subtaskGroups[srcAgentId][subtask.Task.Data.DataId]; !exist {
				subtaskGroups[srcAgentId][subtask.Task.Data.DataId] = make(map[uint64][]*Subtask)
			}

			if _, exist := subtaskGroups[srcAgentId][subtask.Task.Data.DataId][subtask.Dst.AgentId]; !exist {
				subtaskGroups[srcAgentId][subtask.Task.Data.DataId][subtask.Dst.AgentId] = make([]*Subtask, 0)
			}

			subtaskGroups[srcAgentId][subtask.Task.Data.DataId][subtask.Dst.AgentId] =
				append(subtaskGroups[srcAgentId][subtask.Task.Data.DataId][subtask.Dst.AgentId], subtask)
		}
	}

	return subtaskGroups
}

func (s *Scheduler) getExistingSameGroupSubtask(pendingSubtaskGroup map[uint64]map[uint64]map[uint64][]*Subtask) map[string]*Subtask {
	// use "src_agent_id-data_id-dst_agent_id" for easy lookup
	ret := make(map[string]*Subtask)

	for srcId, srcGroup := range pendingSubtaskGroup {
		existingSubtasks := state_manager.GetAgentUploadSubtasks(srcId)
		if len(existingSubtasks) == 0 {
			continue
		}

		for _, subtask := range existingSubtasks {
			// check if data in group
			if dataGroup, exist := srcGroup[subtask.Task.Data.DataId]; exist {
				if _, exist := dataGroup[subtask.Dst.AgentId]; exist {
					// a matching existing subtask found, record it
					key := fmt.Sprintf("%d-%d-%d", subtask.Src.AgentId, subtask.Task.Data.DataId, subtask.Dst.AgentId)
					// we should only get one subtask keyed by the same src-data-dst
					ret[key] = subtask
				}
			}
		}
	}

	return ret
}

func Shuffle(a []*Block) {
	for i := range a {
		j := rand.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}
