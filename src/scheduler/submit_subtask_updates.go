/* submit_subtask_updates.go - submit subtask changes to state_manager*/
/*
modification history
--------------------
2015/7/23, by Guang Yao, create
*/
/*
DESCRIPTION
*/
package scheduler

import (
	"state_manager"
)

func (s *Scheduler) submitSubtaskUpdates() {
	// submit merged pending subtasks as new subtasks
	state_manager.AddSubtasks(s.mergedPendingSubtasks)
}
