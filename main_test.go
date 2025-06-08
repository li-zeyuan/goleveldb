package goleveldb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestLevelDb(t *testing.T) {
	db, err := leveldb.OpenFile("testdb", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	key1 := []byte("key_1")
	value1 := []byte("val_1")

	/*
	写入简要流程：
	1、flush函数：写入流量控制
	2、writeJournal写入WAL文件（未实现落盘）
	3、写入memory
	*/
	if err := db.Put(key1, value1, nil); err != nil {
		t.Fatal(err)
	}


	val, err := db.Get(key1, nil)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t,value1, val)
}
