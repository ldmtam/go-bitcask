package gobitcask

import (
	"bytes"
	"hash/crc32"
	"os"
	"time"
)

type Bitcask struct {
	option         *Option
	openedSegments map[int]*Segment
	activeSegment  *Segment
	keyDir         *KeyDir
}

func NewBitcask(optsFn ...OptFn) (*Bitcask, error) {
	opts := &Option{}
	for _, optFn := range optsFn {
		optFn(opts)
	}

	dirEntries, err := os.ReadDir(opts.DirName)
	if os.IsNotExist(err) {
		err = os.Mkdir(opts.DirName, 0755)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	var nextSegmentID int
	if len(dirEntries) > 0 {
		lastSegment := dirEntries[len(dirEntries)-1]
		nextSegmentID = extractSegmentID(lastSegment.Name())
	}

	activeSegment, err := NewSegment(opts.DirName, nextSegmentID)
	if err != nil {
		return nil, err
	}

	return &Bitcask{
		option:         opts,
		openedSegments: make(map[int]*Segment),
		activeSegment:  activeSegment,
		keyDir:         NewKeyDir(),
	}, nil
}

func (b *Bitcask) Close() error {
	return b.activeSegment.Close()
}

func (b *Bitcask) Put(key, val []byte) error {
	segmentOffset, err := b.activeSegment.GetOffset()
	if err != nil {
		return err
	}

	ts := uint32(time.Now().UnixNano())
	encodedData, err := encode(key, val, uint32ToBytes(ts))
	if err != nil {
		return err
	}

	if segmentOffset+len(encodedData) > b.option.SegmentSize {
		nextSegmentID := b.activeSegment.GetID() + 1
		b.activeSegment, err = NewSegment(b.option.DirName, nextSegmentID)
		if err != nil {
			return err
		}
		segmentOffset = 0
	}

	b.activeSegment.Write(segmentOffset, encodedData)

	b.keyDir.Set(key, &Entry{
		FileID:    b.activeSegment.GetID(),
		ValueSize: len(val),
		ValuePos:  getValuePos(key, segmentOffset),
		Timestamp: ts,
	})

	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	entry, exist := b.keyDir.Get(key)
	if !exist {
		return nil, ErrKeyNotFound
	}

	var err error

	segment, ok := b.openedSegments[entry.FileID]
	if !ok {
		segment, err = OpenSegment(b.option.DirName, entry.FileID)
		if err != nil {
			return nil, ErrOpenSegmentFailed
		}
	}

	return segment.Read(entry.ValuePos, entry.ValueSize)
}

func (b *Bitcask) Delete(key []byte) error {
	_, exist := b.keyDir.Get(key)
	if !exist {
		return ErrKeyNotFound
	}

	err := b.Put(key, tombstoneValue)
	if err != nil {
		return err
	}

	b.keyDir.Delete(key)

	return nil
}

func encode(key, val, ts []byte) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write timestamp
	_, err := buf.Write(ts)
	if err != nil {
		return nil, err
	}

	// write key size
	_, err = buf.Write(uint32ToBytes(uint32(len(key))))
	if err != nil {
		return nil, err
	}

	// write value size
	_, err = buf.Write(uint64ToBytes(uint64(len(val))))
	if err != nil {
		return nil, err
	}

	// write key
	_, err = buf.Write(key)
	if err != nil {
		return nil, err
	}

	// write value
	_, err = buf.Write(val)
	if err != nil {
		return nil, err
	}

	data := buf.Bytes()

	// calculate checksum
	checksum := crc32.ChecksumIEEE(data)

	// re-initialize buffer
	buf = bytes.NewBuffer(nil)

	// write checksum
	_, err = buf.Write(uint32ToBytes(checksum))
	if err != nil {
		return nil, err
	}

	// write data
	_, err = buf.Write(data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decode(data []byte) (checksum, ts uint32, key, value []byte) {
	buf := bytes.NewBuffer(data)

	// get checksum
	checksum = bytesToUint32(buf.Next(checksumLen))

	// get ts
	ts = bytesToUint32(buf.Next(tsLen))

	// get key size
	keySize := bytesToUint32(buf.Next(keySizeLen))

	// get value size
	valueSize := bytesToUint64(buf.Next(valueSizeLen))

	// get key
	key = buf.Next(int(keySize))

	// get value
	value = buf.Next(int(valueSize))

	return
}

func getValuePos(key []byte, segmentOffset int) int {
	return int(segmentOffset) +
		checksumLen +
		tsLen +
		keySizeLen +
		valueSizeLen +
		len(key)
}
