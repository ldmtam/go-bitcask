package gobitcask

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

func extractSegmentID(filename string) int {
	arr := strings.Split(filename, ".")
	fileID, _ := strconv.Atoi(arr[0])
	return fileID
}

func uint32ToBytes(u uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, u)
	return b
}

func bytesToUint32(b []byte) uint32 {
	return uint32(binary.LittleEndian.Uint32(b))
}

func uint64ToBytes(u uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, u)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func getSegmentFilename(id int) string {
	return fmt.Sprintf("%06d.data", id)
}
