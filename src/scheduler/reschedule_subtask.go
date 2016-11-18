/* reschedule_subtask.go - func to reschedule subtask*/
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package scheduler

import (
	"www.baidu.com/golang-lib/log"
)

import (
	"state_manager"
	. "types"
)

const (
	SCHEDULE_LATENCY = 10
	CONFIG_LATENCY   = 10
)

// choose a better src for remain blocks of existing cross region/idc subtask
func (s *Scheduler) rescheduleSubtasks() {
	subtasks := state_manager.GetAllSubtasks()

	for _, subtask := range subtasks {
		if subtask.Src.Idc != subtask.Dst.Idc {
			subtask.ToRemoveBlocks = make([]*Block, 0)
			blocks := inferRemainBlocks(subtask)
			toRemoveIndices := make(map[int]bool)
			for _, block := range blocks {
				newSrc := s.findBetterSrc(subtask, block)
				if newSrc != nil {
					// if a better src is found, create a new subtask
					newSt := state_manager.CreateSubtask(subtask.Task)

					newSt.Src = newSrc
					newSt.RemainBlocks = []*Block{block}
					newSt.ReqBW = calcReqBW(block.Size, subtask.Deadline)
					newSt.Laxity = subtask.Deadline - block.Size/subtask.Dst.DownloadLimit

					// add to pending subtasks
					if _, exist := s.pendingSubtasks[newSrc.AgentId]; !exist {
						s.pendingSubtasks[newSrc.AgentId] = make([]*Subtask, 0)
					}
					s.pendingSubtasks[newSrc.AgentId] = append(s.pendingSubtasks[newSrc.AgentId], newSt)

					// record the to-remove block from the original subtask
					subtask.ToRemoveBlocks = append(subtask.ToRemoveBlocks, block)
					s.shortenSubtasks[subtask.SubtaskId] = subtask
					toRemoveIndices[block.BlockIndex] = true
				}
			}

			if len(toRemoveIndices) > 0 {
				// update the req bw of to-shorten subtasks
				// not accurate estimation
				subtask.ReqBW = subtask.ReqBW * int64(1.0-len(subtask.ToRemoveBlocks)*1.0/len(subtask.RemainBlocks))
				newRemainBlocks := make([]*Block, 0)
				for _, block := range subtask.RemainBlocks {
					if _, exist := toRemoveIndices[block.BlockIndex]; !exist {
						newRemainBlocks = append(newRemainBlocks, block)
					}
				}

				log.Logger.Info("in re-schedule, the remain blocks of subtask[%d] is changed from %s to %s, toremove %+v",
					subtask.SubtaskId, blockSliceStr(subtask.RemainBlocks), blockSliceStr(newRemainBlocks), toRemoveIndices)
				subtask.RemainBlocks = newRemainBlocks
			}
		}
	}
}

// infer the remain blocks of a subtask
func inferRemainBlocks(st *Subtask) []*Block {
	blocks := st.RemainBlocks

	// by pass the blocks that may be finished before the new configuration takes effect
	// the data size can be finished before configuration
	// NOTE: we do not require very accurate inference
	finishSize := st.AllocBW * (SCHEDULE_LATENCY + CONFIG_LATENCY) * 1024 // in Bytes
	startIndex := 0
	for index, block := range blocks {
		finishSize = finishSize - block.Size
		if finishSize <= 0 {
			startIndex = index
			break
		}
	}

	// at least by pass the first one
	if startIndex == 0 {
		startIndex = 1
	}

	if startIndex == len(blocks) {
		return nil
	}

	return blocks[startIndex:]
}

// check whether there is a better src for the block;
// if no better src is found, nil will be returned
// TODO: nearer src is not always better
func (s *Scheduler) findBetterSrc(st *Subtask, block *Block) *Agent {
	candidates := state_manager.GetBlockLocations(st.Task.Data.DataId, block.BlockIndex)

//zyc    
//    log.Logger.Info("candidates for Block %d are:", block.BlockIndex)
//    for  _, src := range candidates {
//        log.Logger.Info("%s ", src.AgentName)
//    }
//    log.Logger.Info("\n")


	// classify srcs by distance
	sameIdcSrcs, sameRegionSrcs, crossRegionSrcs := s.classifyCandidateByDistance(st.Dst, candidates)

	if len(sameIdcSrcs) > 0 {
		newSrc := s.findMostFreeCandidate(sameIdcSrcs)
		return newSrc
	}

	if len(sameRegionSrcs) > 0 {
		newSrc := s.findMostFreeCandidate(sameRegionSrcs)
		if newSrc != st.Src {
			return newSrc
		} else {
			return nil
		}
	}

	if len(crossRegionSrcs) > 0 {
		newSrc := s.findMostFreeCandidate(crossRegionSrcs)
		if newSrc != st.Src {
			return newSrc
		} else {
			return nil
		}
	}

	return nil
}
