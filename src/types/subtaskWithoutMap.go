/* subtaskWithoutMap.go - the definition and interface for RPC subtask */
/*
modification history
--------------------
2016/5/26, by zyc, create
*/
/*
DESCRIPTION
*/
package types

// type SubtaskAction int

// const (
// 	ADD_SUBTASK SubtaskAction = iota
// 	MODIFY_SUBTASK
// 	DELETE_SUBTASK
// 	CONTINUE_SUBTASK
// )

type SubtaskWithoutMap struct {
	SubtaskId uint64 // global unique id for the subtask

	Src *Agent // src agent
	// Dst *Agent // dst agent

	// Task *Task // task

	// Deadline int64 // deadline of the substask
	// Laxity   int64 // the latest time to start the subtask

	// Priority int    // 0 for high pri; 1 for normal pri
	// Product  string // name of the product line

	AllocBW int64 // alloc bandwidth; in KB/s
	// ReqBW   int64 // required bandwidth

	// DstMaxAllocBW  int64            // the max alloc dst download bandwidth;in KB/s
	// SrcMaxAllocBW  int64            // the max alloc src upload bandwidth;in KB/s
	// LinkMaxAllocBW map[string]int64 // the max alloc link bandwidth; linkname=>BW; in KB/s

	// PathType int        // TBD: 0: internal network; 1: external network
	// Links    []*IdcLink // idc links passed by in sequence

	// Reschedule     bool // should the subtask require reschedule after finish each block
	// RescheduleStep int  // reschedule after each rescheduleStep blocks finished

	RemainBlocks   []*Block // indices of the blocks to transfer
	// FinishedBlocks []*Block // indices of finished blocks
	// ToRemoveBlocks []*Block // indices of to-remove blocks

	// state    int // state of the subtask
	// stopCode int // if stopped, 0: finished; 1, failed; 2: deleted; 3: wait-reschedule

}