package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	gobitcask "github.com/ldmtam/go-bitcask"
)

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	bench("./test", 1_000_000, 8, 128)
	bench("./test", 1_000_000, 16, 512)
	bench("./test", 10_000_000, 8, 128)
}

func bench(dirName string, numKeys int, keySize, valSize int) {
	db, err := initDB(dirName)
	if err != nil {
		log.Fatalf("initialize database failed: %v", err)
	}
	defer db.Close()

	kvs := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		kvs[randStringRunes(keySize)] = randStringRunes(valSize)
	}

	now := time.Now()
	for k, v := range kvs {
		err = db.Put([]byte(k), []byte(v))
		if err != nil {
			log.Fatalf("put failed: %v", err)
		}
	}

	fmt.Printf("Put %v items to database with %v-byte key and %v-byte value in %v\n", numKeys, keySize, valSize, time.Since(now))

	now = time.Now()
	for k := range kvs {
		_, err = db.Get([]byte(k))
		if err != nil {
			log.Fatalf("get failed: %v", err)
		}
	}

	fmt.Printf("Get %v items to database with %v-byte key and %v-byte value in %v\n", numKeys, keySize, valSize, time.Since(now))

	os.RemoveAll(dirName)
}

func initDB(dirName string) (*gobitcask.Bitcask, error) {
	db, err := gobitcask.New(
		gobitcask.WithDirName(dirName),
		gobitcask.WithSegmentSize(128*1024*1024), // 128 MB
		gobitcask.WithMergeOpt(&gobitcask.MergeOption{
			Interval: 3 * time.Hour,
		}),
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}
