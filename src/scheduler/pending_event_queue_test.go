package scheduler

import (
	"testing"
	"time"
)

import (
	. "types"
)

func TestPendingEventQueue(t *testing.T) {
	q := NewPendingEventQueue(100)

	newTaskEvent1 := NewTaskEvent{
		ArriveTime:   time.Now(),
		SrcUrl:       "http://test.yf.baidu.com",
		DstHostname:  "test1.tc.baidu.com",
		Deadline:     time.Now().Add(time.Hour),
		Mode:         1,
		Cmd:          1,
		Priority:     1,
		Product:      "psop",
		IsSensitive:  true,
		IsCompressed: true,
		TimeToLive:   time.Now().Add(time.Hour * 24),
	}

	newTaskEvent2 := NewTaskEvent{
		ArriveTime:   time.Now(),
		SrcUrl:       "http://test.yf.baidu.com",
		DstHostname:  "test2.tc.baidu.com",
		Deadline:     time.Now().Add(time.Hour),
		Mode:         1,
		Cmd:          1,
		Priority:     1,
		Product:      "psop",
		IsSensitive:  true,
		IsCompressed: true,
		TimeToLive:   time.Now().Add(time.Hour * 24),
	}

	q.Push(newTaskEvent1)
	q.Push(newTaskEvent2)

	if q.newTaskEventQueue.Len() != 2 {
		t.Errorf("the new task queue len should be 2, but it is %d", q.newTaskEventQueue.Len())
	}

	retQ := q.Take(time.Now().Unix())

	if retQ.newTaskEventQueue.Len() != 2 {
		t.Errorf("the 2 new task should be taken, but it is %d", retQ.newTaskEventQueue.Len())
	}
}
