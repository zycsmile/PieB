/* finalAlloc.go - alloc BW at last*/
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
//"www.baidu.com/golang-lib/log"
)

import (
	"state_manager"
	. "types"
	. "util"
    "strconv"
    "os"
)

func (s *Scheduler) finalAlloc() {
	// when reach here, we get a number of pending subtasks and to-shorten subtasks
	// group them by the resource they claim
	agentUploadGroup := make(map[uint64][]*Subtask)   // agent id => subtasks
	agentDownloadGroup := make(map[uint64][]*Subtask) // agent id => subtasks
	linkGroup := make(map[string][]*Subtask)          // linkname => subtasks

	// group pending subtasks
	for _, subtask := range s.mergedPendingSubtasks {
		// agent upload bw
		if _, exist := agentUploadGroup[subtask.Src.AgentId]; !exist {
			agentUploadGroup[subtask.Src.AgentId] = make([]*Subtask, 0)
		}
		agentUploadGroup[subtask.Src.AgentId] = append(agentUploadGroup[subtask.Src.AgentId], subtask)

		// agent download bw
		if _, exist := agentDownloadGroup[subtask.Dst.AgentId]; !exist {
			agentDownloadGroup[subtask.Dst.AgentId] = make([]*Subtask, 0)
		}
		agentDownloadGroup[subtask.Dst.AgentId] = append(agentDownloadGroup[subtask.Dst.AgentId], subtask)

		// link bw
		for _, link := range subtask.Links {
			if _, exist := linkGroup[link.LinkName]; !exist {
				linkGroup[link.LinkName] = make([]*Subtask, 0)
			}
			linkGroup[link.LinkName] = append(linkGroup[link.LinkName], subtask)
		}

	}

	// process to-shorten subtasks
	// To-shorten subtasks will not be append to the group here as they are existing subtasks
	// They will be appended when append existing subtasks
	// Here just mark the resource should be re-alloc due to to-shorten subtasks
	// will require less resource
	for _, subtask := range s.shortenSubtasks {
		// agent upload bw
		if _, exist := agentUploadGroup[subtask.Src.AgentId]; !exist {
			agentUploadGroup[subtask.Src.AgentId] = make([]*Subtask, 0)
		}

		// agent download bw
		if _, exist := agentDownloadGroup[subtask.Dst.AgentId]; !exist {
			agentUploadGroup[subtask.Dst.AgentId] = make([]*Subtask, 0)
		}

		// link bw
		for _, link := range subtask.Links {
			if _, exist := linkGroup[link.LinkName]; !exist {
				linkGroup[link.LinkName] = make([]*Subtask, 0)
			}
		}
	}

	// process finished subtasks
	// Here just mark the resource should be re-alloc due to finished subtasks
	// will release alloc resource
	// TODO: ugly implementation
	for _, subtask := range s.finishedSubtasks {
		// agent upload bw
		if _, exist := agentUploadGroup[subtask.Src.AgentId]; !exist {
			agentUploadGroup[subtask.Src.AgentId] = make([]*Subtask, 0)
		}

		// agent download bw
		if _, exist := agentDownloadGroup[subtask.Dst.AgentId]; !exist {
			agentUploadGroup[subtask.Dst.AgentId] = make([]*Subtask, 0)
		}

		// link bw
		for _, link := range subtask.Links {
			if _, exist := linkGroup[link.LinkName]; !exist {
				linkGroup[link.LinkName] = make([]*Subtask, 0)
			}
		}
	}

	// alloc each resource claimed between pending and existing subtasks
	for agentId, subtasks := range agentUploadGroup {
		agent := state_manager.GetAgentById(agentId)
		existSubtasks := state_manager.GetAgentUploadSubtasks(agentId)
		for _, subtask := range existSubtasks {
			// NOTE: the demand of exising subtasks should be updated
			// at the beginning of each cycle, when there are new finished blocks
			subtasks = append(subtasks, subtask)
			// add them to reAllocSubtasks
			s.reAllocSubtasks[subtask.SubtaskId] = subtask
		}

		// alloc BW
		allocMap := allocInterfaceBW(agent.UploadLimit, subtasks)

		// set new src upload limit for each subtask
		for _, subtask := range subtasks {
			allocBW, _ := allocMap[subtask.SubtaskId]
			subtask.SrcMaxAllocBW = allocBW
		}
	}

	for agentId, subtasks := range agentDownloadGroup {
		agent := state_manager.GetAgentById(agentId)
		existSubtasks := state_manager.GetAgentDownloadSubtasks(agentId)
		for _, subtask := range existSubtasks {
			subtasks = append(subtasks, subtask)
			// add them to reAllocSubtasks
			s.reAllocSubtasks[subtask.SubtaskId] = subtask
		}
		allocMap := allocInterfaceBW(agent.DownloadLimit, subtasks)

		// set new dst download limit for each subtask
		for _, subtask := range subtasks {
			allocBW, _ := allocMap[subtask.SubtaskId]
			subtask.DstMaxAllocBW = allocBW
		}
	}

	for linkname, subtasks := range linkGroup {
		link := state_manager.GetLinkByName(linkname)
		existSubtasks := state_manager.GetLinkSubtasks(link)
		for _, subtask := range existSubtasks {
			subtasks = append(subtasks, subtask)
			// add them to reAllocSubtasks
			s.reAllocSubtasks[subtask.SubtaskId] = subtask
		}
		allocMap := allocLinkBW(link.Quota, subtasks)

		// set new dst download limit for each subtask
		for _, subtask := range subtasks {
			allocBW, _ := allocMap[subtask.SubtaskId]
			subtask.LinkMaxAllocBW[linkname] = allocBW
		}
	}

	// set the rate of reAllocSubtasks
	for _, subtask := range s.reAllocSubtasks {
		// calc new bottleneck
		bottleneck := Min(subtask.SrcMaxAllocBW, subtask.DstMaxAllocBW)
		for _, linkBW := range subtask.LinkMaxAllocBW {
			if linkBW < bottleneck {
				bottleneck = linkBW
			}
		}

		// calc bw change, set new BW, and change free resource amount
		// NOTE: it will not cause problem if we increase the BW of existing subtasks
		// it share of the other resource is actually reserved
		// HOWEVER, if the free share are alloc to the other subtasks, we cannot
		// increase the BW directly
		change := subtask.AllocBW - bottleneck
		subtask.AllocBW = bottleneck
		subtask.Src.UploadFree += change
		subtask.Dst.DownloadFree += change
		for _, link := range subtask.Links {
			link.Free += change
		}
	}

	// set the rate of pendingSubtasks
	for _, subtask := range s.mergedPendingSubtasks {
		// calc new bottleneck
		bottleneck := Min(subtask.SrcMaxAllocBW, subtask.DstMaxAllocBW)
		for _, linkBW := range subtask.LinkMaxAllocBW {
			if linkBW < bottleneck {
				bottleneck = linkBW
			}
		}

		subtask.AllocBW = bottleneck
		subtask.Src.UploadFree -= bottleneck
		subtask.Dst.DownloadFree -= bottleneck
		for _, link := range subtask.Links {
			link.Free -= bottleneck
		}
	}
//    s.calcuteIDCFreeBW()
//    s.calAgentUploadBW()
/*
	//对于Pending的子任务，如果所有资源都还有剩余，则增加
	for _, subtask := range s.mergedPendingSubtasks {
		// calc new bottleneck
		free := Min(subtask.Src.UploadFree, subtask.Dst.DownloadFree)
		for _, link := range subtask.Links {
			if link.Free < free {
				free = link.Free
			}
		}

		if free > 0 {
			//log.Logger.Info("free: %v, %v, %v", free, subtask.Src.UploadFree, subtask.Dst.DownloadFree)
			subtask.AllocBW += free
			subtask.Src.UploadFree -= free
			subtask.Dst.DownloadFree -= free
			for _, link := range subtask.Links {
				link.Free -= free
			}
		}
	}
*/
	//TBD: should we alloc the free resource?

	//TBD: should we check whether there are bad alloc?
    f, err := os.OpenFile("../main/agentUpload.txt",os.O_CREATE|os.O_APPEND|os.O_RDWR,0660)
    check(err)
    defer f.Close()
    _,uperr := f.WriteString("round" + "\n")
    check(uperr)
    for agentId,_ := range agentUploadGroup {
        agent := state_manager.GetAgentById(agentId)
        Up := (float64)(agent.UploadLimit)
        Free := (float64)(agent.UploadFree)
        _,err1 := f.WriteString(agent.AgentName + ","  + strconv.FormatFloat(Up,'f',5,32) + "," + strconv.FormatFloat(Free,'f',5,32) + "," + strconv.FormatFloat(((Up-Free)/Up),'f',5,32)  + "\n")
        check(err1)
    }
    f.Sync()

    f, errs := os.OpenFile("../main/agentDownload.txt",os.O_CREATE|os.O_APPEND|os.O_RDWR,0660)
    check(errs)
    defer f.Close()
    _,downerr := f.WriteString("round" + "\n")
    check(downerr)
    for agentId,_ := range agentDownloadGroup {
        agent := state_manager.GetAgentById(agentId)
        Down := (float64)(agent.DownloadLimit)
        Free := (float64)(agent.DownloadFree)
        _,err1 := f.WriteString(agent.AgentName + ","  + strconv.FormatFloat(Down,'f',5,32) + "," + strconv.FormatFloat(Free,'f',5,32) + "," + strconv.FormatFloat(((Down-Free)/Down),'f',5,32) + "\n")
        check(err1)
    }
    f.Sync()
    
    f, errss := os.OpenFile("../main/idcLink.txt",os.O_CREATE|os.O_APPEND|os.O_RDWR,0660)
    check(errss)
    defer f.Close()
    _,linkerr := f.WriteString("round" + "\n")
    check(linkerr)
    for linkname,_ := range linkGroup {
        link := state_manager.GetLinkByName(linkname)
        quo := (float64)(link.Quota)
        Free := (float64)(link.Free)
        _,err1 := f.WriteString(link.LinkName + ","  + strconv.FormatFloat(quo,'f',5,32) + "," + strconv.FormatFloat(Free,'f',5,32) + "," + strconv.FormatFloat(((quo-Free)/quo),'f',5,32) + "\n")
        check(err1)
    }
    f.Sync()
}
/*
//20160222 added by zyc
func (s *Scheduler) calcuteIDCFreeBW() {
    targetLinkName := "szjjh-hd"
    srcLinkName := "yf-hb"
    targetLink := state_manager.GetLinkByName(targetLinkName)
    srcLink := state_manager.GetLinkByName(srcLinkName)
    f, err := os.OpenFile("../main/IDCFreeBW.txt",os.O_CREATE|os.O_APPEND|os.O_RDWR,0660)
    check(err)
    defer f.Close()
    _,err1 := f.WriteString(targetLink.LinkName + "," + strconv.FormatFloat((float64)(targetLink.Quota),'f',5,32) + "," + strconv.FormatFloat((float64)(targetLink.Free),'f',5,32) + "\n")
//    _,err1 := f.WriteString(targetLink.LinkName + ","  + strconv.FormatFloat(((float64)(targetLink.Quota-targetLink.Free)/(float64)(targetLink.Quota)),'f',5,32) + "\n")
    check(err1)
    _,err2 := f.WriteString(srcLink.LinkName + "," + strconv.FormatFloat((float64)(srcLink.Quota),'f',5,32) + "," + strconv.FormatFloat((float64)(srcLink.Free),'f',5,32) + "\n")
    check(err2)
    f.Sync()
}
*/
