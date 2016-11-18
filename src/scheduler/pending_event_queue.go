package scheduler

import (
	"container/list"
	"fmt"
	"sync"
)

import (
	. "types"
    "www.baidu.com/golang-lib/log"
)

type PendingEventQueue struct {
	// lock for the queue
	lock sync.Mutex

	// maximum length
	maxLen int

	// current length
	curLen int

	// queues for all the events
	newTaskEventQueue   *list.List
	finishedBlocksQueue *list.List
}

func NewPendingEventQueue(maxLen int) *PendingEventQueue {
	q := new(PendingEventQueue)
//    log.Logger.Info("New a pending event queue.")

	q.maxLen = maxLen
	q.curLen = 0

	q.newTaskEventQueue = list.New()
	q.finishedBlocksQueue = list.New()

	return q
}

// push event to the queue
func (q *PendingEventQueue) Push(e Event) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	// check whether the queue has been full
	if q.curLen >= q.maxLen {
		log.Logger.Info("the pending event queue has been full")
		return fmt.Errorf("the pending event queue has been full")
	}

	// append the event to the corresponding queue
	switch {
	case e.EventType() == EVENT_TASK_NEW:
		q.newTaskEventQueue.PushBack(e)
	case e.EventType() == EVENT_SUBTASK_FINISH_BLOCKS:
		q.finishedBlocksQueue.PushBack(e)
	default:
		// TODO: do something here
	}

	q.curLen += 1
    log.Logger.Info("Finish pushing an event to pending event queue.")

	return nil
}

// take events by the given time
func (q *PendingEventQueue) Take(time int64) *PendingEventQueue {
	retQ := NewPendingEventQueue(q.maxLen)
	log.Logger.Info("Begin to take events from the pending queue!!!")

	q.lock.Lock()
	defer q.lock.Unlock()

	var next *list.Element

	for e := q.newTaskEventQueue.Front(); e != nil; e = next {
		// dequeue and add to retQ
		log.Logger.Info("ArriveTime:%d, TakeTime:%d.QueueLength is :%d",e.Value.(NewTaskEvent).ArriveTime.Unix(), time,q.curLen)
		if e.Value.(NewTaskEvent).ArriveTime.Unix() <= time {
			if err := retQ.Push(e.Value.(Event)); err != nil {
				log.Logger.Info("err in push: %v", err)
			}

			next = e.Next()
			q.newTaskEventQueue.Remove(e)
			q.curLen -= 1
		}
	}

	for e := q.finishedBlocksQueue.Front(); e != nil; e = next {
		// dequeue and add to retQ
		if e.Value.(SubtaskFinishBlocksEvent).ArriveTime.Unix() <= time {
			if err := retQ.Push(e.Value.(Event)); err != nil {
				fmt.Printf("err in push: %v", err)
			}

			next = e.Next()
			q.finishedBlocksQueue.Remove(e)
			q.curLen -= 1
		}
	}

	return retQ
}
