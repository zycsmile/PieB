/* event.go - the definition for event */
/*
modification history
--------------------
2015/4/22, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package types

import (
	"time"
)

const (
	EVENT_TASK_NEW              = "EVENT_TASK_NEW"
	EVENT_SUBTASK_FINISH_BLOCKS = "EVENT_SUBTASK_FINISH_BLOCKS"
)

type Event interface {
	EventType() string
}

type NewTaskEvent struct {
	ArriveTime   time.Time
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

func (e NewTaskEvent) EventType() string {
	return EVENT_TASK_NEW
}

type SubtaskFinishBlocksEvent struct {
	ArriveTime     time.Time
	SubtaskId      uint64
	FinishedBlocks map[int]bool
}

func (e SubtaskFinishBlocksEvent) EventType() string {
	return EVENT_SUBTASK_FINISH_BLOCKS
}
