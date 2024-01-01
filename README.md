# GO-BITCASK

### What is Bitcask
A Log-Structured Hash Table for Fast Key/Value Data which is used by [Riak](https://riak.com/)

For more details, please reference to [Bitcask whitepaper](https://riak.com/assets/bitcask-intro.pdf)

### How to use
Create new instance of **go-bitcask**
```
db, err := gobitcask.New(
    WithDirName(dirName),
    WithSegmentSize(128), // bytes
    WithMergeOpt(&MergeOption{
        Interval: 6 * time.Hour, // run compaction every 6 hours
        Min: 5,                  // at least 5 data files before merging
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
    log.Fatal("delete data from gobitcask failed: %v", err)
}
```