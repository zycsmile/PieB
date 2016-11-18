/* process_finished_blocks.go - process finished blocks of subtasks*/
/*
modification history
--------------------
2015/7/24, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package scheduler

import (
	"container/list"
	"time"
    "strconv"
    "fmt"
    "os"
)

import (
	"www.baidu.com/golang-lib/log"
)

import (
	"state_manager"
	. "types"
)

func check(e error){
    if e != nil {
        panic(e)
    }
}

func (s *Scheduler) processFinishedBlocks(finishedBlocksQueue *list.List) {
	for e := finishedBlocksQueue.Front(); e != nil; e = e.Next() {
		event := e.Value.(SubtaskFinishBlocksEvent)
		subtaskId := event.SubtaskId
		finishBlocks := event.FinishedBlocks

		log.Logger.Info("process finished block event: %+v", event)

		// get subtask
		subtask := state_manager.GetSubtaskById(subtaskId)
		if subtask == nil {
			log.Logger.Error("err in GetSubtaskById: unknown subtask id: %d", subtaskId)
			continue
		}

		// update task state in state_manager
		UpdateTaskFinishedBlocks(subtask.Task, subtask, finishBlocks)

		// update subtask state
		updateSubtaskFinishedBlocks(subtask, finishBlocks)

		// update block location
		for blockIndex, _ := range finishBlocks {
			state_manager.AddBlockLocation(subtask.Task.Data.DataId, blockIndex, subtask.Dst)
		}
	}
}

func FloatToString(input_num float64) string {
    return strconv.FormatFloat(input_num, 'f', 6, 64)
}

func UpdateTaskFinishedBlocks(t *Task, st *Subtask, finishedBlocks map[int]bool) {
	for index := range finishedBlocks {
		block := t.Data.Blocks[index]

		t.FinishedBlocks[index] = block
		delete(t.RemainBlocks, index)

		if st.Src.AgentName == t.Origin.AgentName {
			t.FinishedFromOrigin += 1
		}
	}

	if len(t.RemainBlocks) == 0 {
		log.Logger.Info("Task finished at %v: %s get data %s, %v from origin",time.Now(), t.Dst.AgentName, t.Data.OriginPath, float64(t.FinishedFromOrigin)/float64(t.Data.BlockCount))
        f, err := os.OpenFile("../main/test.txt",os.O_CREATE|os.O_APPEND|os.O_RDWR,0660)
//    f, err := os.Create("/home/work/zhangyuchao02/controller/main/TCT.txt")
        check(err)
        defer f.Close()
//        n1,err := f.WriteString("Finished time\n")
//        check(err)
//    start_time := time.Now().Unix()
        timestamp := time.Now().Unix()
        tm := time.Unix(timestamp,0)
        tt := tm.Format("2006-01-02 15:04:05")
        _,err1 := f.WriteString(t.Dst.AgentName + tt + "\n" + FloatToString(float64(t.FinishedFromOrigin)/float64(t.Data.BlockCount)) + "\n")
        check(err1)
//        log.Logger.Info("write task finish time to file %d letters: %s\n", n1)
        f.Sync()
        fmt.Printf("%s finished, %v from origin", t.Dst.AgentName, float64(t.FinishedFromOrigin)/float64(t.Data.BlockCount))
	}
}

// update the req bw and remain blocks of subtask
func updateSubtaskFinishedBlocks(subtask *Subtask, finishBlocks map[int]bool) error {
	// update remain blocks
	newRemainBlocks := make([]*Block, 0)
	totalSize := int64(0)
	for _, block := range subtask.RemainBlocks {
		if _, exist := finishBlocks[block.BlockIndex]; !exist {
			newRemainBlocks = append(newRemainBlocks, block)
			totalSize += block.Size
		}
	}
	log.Logger.Info("subtask[%d] has finish some blocks, the remain blocks has been changed to %s", subtask.SubtaskId, blockSliceStr(newRemainBlocks))

	subtask.RemainBlocks = newRemainBlocks

	// if subtask has been finished, submit finished subtask
	if len(newRemainBlocks) == 0 {
		log.Logger.Info("Subtask[%d] finished", subtask.SubtaskId)
		scheduler.finishedSubtasks[subtask.SubtaskId] = subtask
	}

	// update req BW
	subtask.ReqBW = calcReqBW(totalSize, subtask.Deadline)

	// TBD: should we update Laxity?

	return nil
}
