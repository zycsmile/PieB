/* agent_state_collector.go - collect state from agent*/
/*
modification history
--------------------
2015/7/23, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package agent_state_collector

import (
	"time"
)

import (
	"www.baidu.com/golang-lib/log"
)

import (
	"scheduler"
	"state_manager"
	. "types"
)

type AgentStateCollector struct {
}

var agentStateCollector *AgentStateCollector

func Init() {
	agentStateCollector = new(AgentStateCollector)
}

// TODO: currently this is a simulated interface
func Start() {
	go func() {
		log.Logger.Info("Agent_state_collector start")
        ///////////////////////////
        // finishedByteMap
        finishedByteMap := make(map[uint64]int64)//subtask id => finished bytes
        /////////////////////////////////////////////////////////////////////////////
		for {

//            finishedByteMap := make(map[uint64]int64)//subtask id => finished bytes
			// randomly select subtasks from all the subtasks
			subtasks := state_manager.GetAllSubtasks()
			for _, subtask := range subtasks {
				if len(subtask.RemainBlocks) > 0 {
					// finish a number of blocks
					finishedSize := subtask.AllocBW * 10

                    ////////////////////////
                    // check non existing
                    finishedBytes, exist := finishedByteMap[subtask.SubtaskId]
                    if !exist {
//                        finishedByteMap = append(finishedByteMap, finishedByteMap[subtask.SubtaskId])
                        finishedByteMap[subtask.SubtaskId] = 0
                        finishedBytes = finishedByteMap[subtask.SubtaskId]
                    }
                    finishedSize += finishedBytes
                    finishedByteMap[subtask.SubtaskId] = finishedSize
                    ////////////////////////////////////////////////////////////////////

					finishBlocks := make(map[int]bool)
					for _, block := range subtask.RemainBlocks {
						if block.Size <= finishedSize {
							finishBlocks[block.BlockIndex] = true
                            finishedSize -= block.Size
						}
//						finishedSize -= block.Size
					}
                    ////////////////
//                    finishedByteMap[subtask.SubtaskId] = finishedSize
                    ///////////////////////////////////////////////////////////

					if len(finishBlocks) > 0 {
						event := SubtaskFinishBlocksEvent{time.Now(), subtask.SubtaskId, finishBlocks}
						err := scheduler.AppendEvent(event)
						if err != nil {
							log.Logger.Error("err happen when add new block finish event: %v", err)
						}
						//log.Logger.Info("block finish event %+v generated", event)
					}
				}
			}

			// wait for a schedule cycle
			<-time.After(time.Second * 10)
		}
	}()
}
