/* scheduler.go - scheduler*/
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
	//"math"
	"fmt"
	"time"
)

import (
	"www.baidu.com/golang-lib/log"
)

import (
	"controller_conf"
	"state_manager"
	. "types"
)

const (
	PENDING_QUEUE_MAX_LEN = 65535
)

type Scheduler struct {
	pendingEventQueue *PendingEventQueue

	scheduleCycle int64 // in seconds

	// all the subtasks whose alloc BW is changed in each schedule cycle
	reAllocSubtasks map[uint64]*Subtask

	// all the subtasks which have been scheduled but not configed in each schedule cycle
	// indexed by src agent to make get the num of pending subtasks on each agent easier
	pendingSubtasks map[uint64][]*Subtask // upload agent id => subtasks

	// pending subtasks will be merged
	mergedPendingSubtasks map[uint64]*Subtask // subtask id => subtask

	// all the subtasks whose remaining blocks will be shorten due to reschedule
	shortenSubtasks map[uint64]*Subtask // subtask id => subtask

	// all the subtasks finished in last cycle
	finishedSubtasks map[uint64]*Subtask // subtask id => subtask
}

// use singleton model
var scheduler *Scheduler

func Init(cfg controller_conf.ControllerConfig) {
	scheduler = newScheduler()
	scheduler.scheduleCycle = cfg.Main.ScheduleCycle
}

func newScheduler() *Scheduler {
	s := new(Scheduler)

	s.pendingEventQueue = NewPendingEventQueue(PENDING_QUEUE_MAX_LEN)
	s.pendingSubtasks = make(map[uint64][]*Subtask)
	s.shortenSubtasks = make(map[uint64]*Subtask)
	s.reAllocSubtasks = make(map[uint64]*Subtask)
	s.mergedPendingSubtasks = make(map[uint64]*Subtask)
	s.finishedSubtasks = make(map[uint64]*Subtask)

	return s
}

// the main routine of scheduler
// TODO: how to control scheduling cycle?
func Start() {
	go func() {
		log.Logger.Info("scheduler start")
		for {
			//cycleBeginTime := time.Now().Unix()

			// clear the records of the last cycle
			scheduler.pendingSubtasks = make(map[uint64][]*Subtask)
			scheduler.shortenSubtasks = make(map[uint64]*Subtask)
			scheduler.reAllocSubtasks = make(map[uint64]*Subtask)
			scheduler.mergedPendingSubtasks = make(map[uint64]*Subtask)
			scheduler.finishedSubtasks = make(map[uint64]*Subtask)

			// take all the events arrived by now
			log.Logger.Info("Begin to take events from pendingQueue when ")
			pendingQ := scheduler.pendingEventQueue.Take(time.Now().Unix())

			// process finished blocks
			log.Logger.Info("processFinishedBlocks begins")
			scheduler.processFinishedBlocks(pendingQ.finishedBlocksQueue)

			// process finished subtasks
			log.Logger.Info("processFinishedSubtasks begins")
			scheduler.processFinishedSubtasks()

			// process pending tasks
			// pendingTasks only select the most-free nearest source for each block
			log.Logger.Info("processPendingTasks begins")
			scheduler.processPendingTasks(pendingQ.newTaskEventQueue)
			// a number of new subtasks will be generated

			// choose a better src for existing cross region/idc subtask
			scheduler.rescheduleSubtasks()
			// a number of new subtasks will be generated;
			// and the original subtasks will be modified, with a number of to-remove blocks

			// iteratively find a better selection
			// TODO: use simulated annealing?
			/*
				lastScore := math.MaxInt64
				for {
					// try to alloc BW based on current selection
					scheduler.tryAlloc()

					// evaluate the selection
					score := scheduler.evaluateTrialSelection()

					// determine whether to finish iteration or continue
					// 1. not good enough; 2. not converged; 3. in time limit
					if score > 0 && score - lastScore <0 && time.Now().Unix() < cycleBeginTime+scheduler.scheduleCycle {
						// generate a local swap
						scheduler.shuffleSelection()
					} else {
						// end iteration
						break
					}
				}*/

			/*for _, subtasks := range scheduler.pendingSubtasks {
				for _, subtask := range subtasks {
					log.Logger.Info("all pending subtasks:%+v", subtask)
				}
			}*/

			// merge pending subtask from the same src to the same dst,
			// transfering blocks belonging to the same data
			scheduler.mergePendingSubtasks()

			// final alloc
			scheduler.finalAlloc()
			// BW will be alloc to new subtasks
			// BW of existing subtasks will be changed

			// generate config
			// add the new subtasks and modify existing subtasks
			//scheduler.generateConfig()

			// submit new subtasks and subtask changes to the state_manager
			scheduler.submitSubtaskUpdates()

			// print all the subtasks
			sts := state_manager.GetAllSubtasks()
			if len(sts) > 0 {
				log.Logger.Info("All subtasks:")
				for _, subtask := range sts {
					log.Logger.Info("%s", logSubtaskStr(subtask))
				}
			} else {
				log.Logger.Info("No running subtask")
			}
			state_manager.UpdateState()
			// wait
			time.Sleep(time.Duration(scheduler.scheduleCycle)*time.Second)
			//time.Sleep(10*time.Second)
		}
	}()
}

// add new event to the pending queue
func AppendEvent(e Event) error {
	return scheduler.pendingEventQueue.Push(e)
}

// generate a string for log
func logSubtaskStr(subtask *Subtask) string {
	return fmt.Sprintf("subtask[%d]: [%s->%s]: transfer %s %s with rate %d KB/s, req %d KB/s",
		subtask.SubtaskId,
		subtask.Src.AgentName,
		subtask.Dst.AgentName,
		subtask.Task.Data.OriginPath,
		blockSliceStr(subtask.RemainBlocks),
		subtask.AllocBW,
		subtask.ReqBW,
	)
}

// generate a string for block array
func blockSliceStr(blocks []*Block) string {
	blockStr := "["
	for _, block := range blocks {
		blockStr = blockStr + fmt.Sprintf("%d ", block.BlockIndex)
	}
	blockStr = blockStr + "]"

	return blockStr
}
