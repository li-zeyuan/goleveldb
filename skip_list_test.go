package goleveldb

import "testing"

func TestSet(t *testing.T) {
	sl := NewSkipListInt()
	sl.Set(1, "one")
	sl.Set(2, "two")
	sl.Set(3, "three")
	sl.Set(4, "four")
	sl.Set(5, "five")
}
