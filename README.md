# GO-BITCASK

### What is Go-Bitcask
A Log-Structured Hash Table for Fast Key/Value Data implemented in Golang

Go-Bitcask storage engine features:
1. Low latency per item read or written
2. High throughput, especially when writing an incoming stream of random items
3. Crash friendliness, both in terms of fast recovery and not losing data
4. Ease of backup and restore
5. Relatively simple, understandable code structure and data format with high unit-test coverage (84%)

For more details, please reference to [Bitcask whitepaper](https://riak.com/assets/bitcask-intro.pdf)

### Usage
Create new instance of **go-bitcask**
```
db, err := gobitcask.New(
    WithDirName(dirName),
    WithSegmentSize(1024 * 1024 * 1024),  // 1 GB per segment file
    WithMergeOpt(&MergeOption{
        Interval: 6 * time.Hour,          // run compaction every 6 hours
        MinFiles: 5,                      // at least 5 data files before merging
    })
)
if err != nil {
    log.Fatalf("create gobitcask instance failed: %v", err)
}
defer db.Close()
```

Store key/value pair to storage
```
err := db.Put([]byte("key1"), []byte("val1"))
if err != nil {
    log.Fatalf("store data to gobitcask failed: %v", err)
}
```

Get value from storage by particular key
```
val, err := db.Get([]byte("key1"))
if err != nil && err != gobitcask.ErrKeyNotFound {
    log.Fatalf("get data from gobitcask failed: %v", err)
}
fmt.Println(string(val))
```

Delete a key/value pair by key
```
err := db.Delete([]byte("key1"))
if err != nil {
    log.Fatalf("delete data from gobitcask failed: %v", err)
}
```

List all keys
```
keys, err := db.ListKeys()
if err != nil {
    log.Fatalf("list keys from gobitcask failed: %v", err)
}
```

Fold over all key/value pairs
```
err := db.Fold(func(key, val []byte) error {
    fmt.Printf("key: %v, val: %v\n", string(key), string(val))
    return nil
})
if err != nil {
    log.Fatalf("fold over all key/value pairs failed: %v", err)
}
```

### Benchmark
Machine information: Macbook Pro 2021 (16 inch), M1 Pro, 16 GB RAM, 512 GB SSD

| Action        | Num of keys | Key size (byte) | Value size (byte) | Duration (second)
| ------------- | ----------- |---------------- |------------------ |-----------------
| Put           | 1 000 000   |      8          |        128        |       2.809
| Put           | 1 000 000   |      16         |        512        |       3.331
| Put           | 10 000 000  |      8          |        128        |       30.047
| Random Get    | 1 000 000   |      8          |        128        |       0.769
| Random Get    | 1 000 000   |      16         |        512        |       0.867
| Random Get    | 10 000 000  |      8          |        128        |       8.81