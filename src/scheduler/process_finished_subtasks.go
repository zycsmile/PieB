/* process_finished_subtasks.go - process finished subtasks*/
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
	"state_manager"
)

func (s *Scheduler) processFinishedSubtasks() {
	for _, subtask := range s.finishedSubtasks {
		// release the resource
		/*		subtask.Src.UploadFree += subtask.AllocBW
				subtask.Dst.DownloadFree += subtask.AllocBW
				for _, link := range subtask.Links {
					link.Free += subtask.AllocBW
				}*/

		// clear subtask from global index and maps
		state_manager.SubmitFinishSubtask(subtask)
	}
}
