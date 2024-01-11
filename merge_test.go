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
			Interval: 300 * time.Millisecond,
		}),
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i)
		err = bc.Put([]byte(key), []byte(val))
		assert.Nil(t, err)
	}

	<-time.After(500 * time.Millisecond)

	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i)
		fetchedVal, err := bc.Get([]byte(key))
		assert.Nil(t, err)
		assert.EqualValues(t, val, fetchedVal)
	}
}

func TestSkipHintAndMergeFile(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
		WithMergeOpt(&MergeOption{
			Interval: 300 * time.Millisecond,
		}),
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i)
		err = bc.Put([]byte(key), []byte(val))
		assert.Nil(t, err)
	}

	// wait for 1st compaction
	<-time.After(500 * time.Millisecond)

	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("newkey%v", i), fmt.Sprintf("newval%v", i)
		err = bc.Put([]byte(key), []byte(val))
		assert.Nil(t, err)
	}

	// wait for 2nd compaction
	<-time.After(500 * time.Millisecond)

	err = bc.Close()
	assert.Nil(t, err)

	bc2, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
		WithMergeOpt(&MergeOption{
			Interval: 6 * time.Hour,
		}),
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc2)

	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i)
		fetchedVal, err := bc2.Get([]byte(key))
		assert.Nil(t, err)
		assert.EqualValues(t, val, fetchedVal)

		key, val = fmt.Sprintf("newkey%v", i), fmt.Sprintf("newval%v", i)
		fetchedVal, err = bc2.Get([]byte(key))
		assert.Nil(t, err)
		assert.EqualValues(t, val, fetchedVal)
	}
}
