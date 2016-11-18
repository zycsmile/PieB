/* task.go - definition and interfaces for task */
/*
modification history
--------------------
2015/6/4, by Guang Yao, create
*/
/*
DESCRIPTION
*/

package types

import (
	"time"
)

type callbackHandler interface {
	HandleFailure() // handler after failure
	HandleFinish()  // handler after finish
	HandleTimeout() // handler after timeout
}

type TaskStatus int

const (
	TASKPENDING TaskStatus = iota
	RUNNING
	STOP
)

type TaskAction int

const (
	TASK_ACTION_START TaskAction = iota
	TASK_ACTION_PAUSE
	TASK_ACTION_DELETE
)

type Task struct {
	SrcUrl string // parameters for the source
	Dst    *Agent // parameters for the destination
	Origin *Agent

	Mode      int              // publisher-subscribe: 0; multicast: 1
	Cmd       int              // register: 0; transfer: 1
	Product   string           // name of the product
	Priority  int              // 0 for high pri; 1 for normal pri
	Deadline  int64            // deadline date of the task
	Callbacks *callbackHandler // call back handlers

	IsSensitive  bool  // is the data sensitive
	IsCompressed bool  // is the data compressed
	TimeToLive   int64 // the data can be removed from the buffer after this time

	TaskId uint64 // unique id for the task

	Data *Data // the data to transfer

	FinishedBlocks map[int]*Block // finished blocks, block index => block
	RemainBlocks   map[int]*Block // remain blocks, block index => block

	Status   TaskStatus // pending: 0; running: 1; stop: 2
	StopCode int        // finished: 0; fail: 1; deleted: 2; pause: 3
	ErrMsg   string     // err message in case of fail

	FinishedFromOrigin int64
}

func NewTask(srcUrl string, dst *Agent, mode int, cmd int, product string, priority int,
	deadline time.Time, isSensitive bool, compressed bool, timeToLive time.Time, data *Data) *Task {

	t := new(Task)

	t.SrcUrl = srcUrl
	t.Dst = dst
	t.Mode = mode
	t.Cmd = cmd
	t.Product = product
	t.Priority = priority
	t.Deadline = deadline.Unix()
	t.IsSensitive = isSensitive
	t.IsCompressed = compressed
	t.TimeToLive = timeToLive.Unix()

	t.Data = data
	t.FinishedBlocks = make(map[int]*Block)
	t.RemainBlocks = make(map[int]*Block)
	for i := 0; i < data.BlockCount; i++ {
		t.RemainBlocks[i] = data.Blocks[i]
	}

	t.FinishedFromOrigin = 0

	return t
}
