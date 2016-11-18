package scheduler

import (
	"sort"
)

import (
//"www.baidu.com/golang-lib/log"
)

import (
	. "types"
)

func allocInterfaceBW(resource int64, sts []*Subtask) map[uint64]int64 {
	totalDemand := calcTotalDemand(sts)

	if totalDemand > resource {
		// We will not do max-min alloc for interface bandwidth
		return allocPreempt(resource, sts)
	} else {
		return allocPropotion(resource, sts, totalDemand)
	}
}

func allocLinkBW(resource int64, sts []*Subtask) map[uint64]int64 {
	totalDemand := calcTotalDemand(sts)

	if totalDemand > resource {
		// We will not do max-min alloc for interface bandwidth
		return allocPreempt(resource, sts)
	} else {
		return allocPropotion(resource, sts, totalDemand)
	}
}

func allocPreempt(resource int64, sts []*Subtask) map[uint64]int64 {
	allocMap := make(map[uint64]int64)

	stSlice := SubtaskSlice(sts)
	sort.Sort(stSlice)

	for _, st := range stSlice {
		if st.ReqBW < resource {
			allocMap[st.SubtaskId] = st.ReqBW
			resource -= st.ReqBW
		} else {
			allocMap[st.SubtaskId] = resource
			if resource > 0 {
				resource = 0
			}
		}
	}

	return allocMap
}

func allocPropotion(resource int64, sts []*Subtask, totalDemand int64) map[uint64]int64 {
	allocMap := make(map[uint64]int64)

	for _, st := range sts {
		if totalDemand != 0 {
			allocMap[st.SubtaskId] = st.ReqBW * resource / totalDemand
		}
	}

	return allocMap
}

func calcTotalDemand(sts []*Subtask) int64 {
	total := int64(0)
	for _, st := range sts {
		total += st.ReqBW
	}

	return total
}
