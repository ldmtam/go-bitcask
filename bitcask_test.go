package gobitcask

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateNewBitcask(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(WithDirName(dirName))
	assert.Nil(t, err)
	assert.NotNil(t, bc)
}

func TestEncode(t *testing.T) {
	key := []byte("key1")
	val := []byte("val1")
	ts := uint32(time.Now().UnixNano())

	encodedData, err := encode(key, val, uint32ToBytes(ts))
	assert.Nil(t, err)
	assert.NotZero(t, len(encodedData))

	checksum, decodedTs, decodedKey, decodedVal := decode(encodedData)
	assert.EqualValues(t, key, decodedKey)
	assert.EqualValues(t, val, decodedVal)
	assert.EqualValues(t, ts, decodedTs)
	assert.NotZero(t, checksum)
}

func TestSimplePutGet(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	key := []byte("key1")
	val := []byte("val1")

	err = bc.Put(key, val)
	assert.Nil(t, err)

	fetchedVal, err := bc.Get(key)
	assert.Nil(t, err)
	assert.EqualValues(t, val, fetchedVal)
}

func TestSimplePutDelete(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	key := []byte("key1")
	val := []byte("val1")

	err = bc.Put(key, val)
	assert.Nil(t, err)

	err = bc.Delete(key)
	assert.Nil(t, err)

	fetchedVal, err := bc.Get(key)
	assert.Nil(t, fetchedVal)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestListKeys(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	key1, val1 := []byte("key1"), []byte("val1")
	err = bc.Put(key1, val1)
	assert.Nil(t, err)

	key2, val2 := []byte("key2"), []byte("val2")
	err = bc.Put(key2, val2)
	assert.Nil(t, err)

	keys := bc.ListKeys()
	assert.Equal(t, 2, len(keys))

	keySet := make(map[string]struct{})
	for _, key := range keys {
		keySet[string(key)] = struct{}{}
	}

	delete(keySet, string(key1))
	delete(keySet, string(key2))

	assert.Equal(t, 0, len(keySet))
}

func TestKeyDirWarmUp(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	key1, val1 := []byte("key1"), []byte("val1")
	key2, val2 := []byte("key2"), []byte("val2")

	err = bc.Put(key1, val1)
	assert.Nil(t, err)

	err = bc.Put(key2, val2)
	assert.Nil(t, err)

	err = bc.Close()
	assert.Nil(t, err)

	bc2, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc2)

	fetchedVal1, err := bc2.Get(key1)
	assert.Nil(t, err)
	assert.EqualValues(t, val1, fetchedVal1)

	fetchedVal2, err := bc2.Get(key2)
	assert.Nil(t, err)
	assert.EqualValues(t, val2, fetchedVal2)
}
