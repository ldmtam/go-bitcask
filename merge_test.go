package gobitcask

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMergeData(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
		WithMergeOpt(&MergeOption{
			Interval: 6 * time.Hour,
		}),
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i)
		err = bc.Put([]byte(key), []byte(val))
		assert.Nil(t, err)
	}

	m := NewMerger(dirName, bc.keyDir, &MergeOption{Interval: 50 * time.Millisecond})
	go m.Start()

	<-time.After(100 * time.Millisecond)

	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i)
		fetchedVal, err := bc.Get([]byte(key))
		assert.Nil(t, err)
		assert.EqualValues(t, val, fetchedVal)
	}
}
