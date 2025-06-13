package leveldb

import (
	"container/list"
	"testing"
)

func TestPushBack(t *testing.T) {
	l := list.New()
	l.PushBack(1)
	l.PushBack(2)
	l.PushBack(3)
}
