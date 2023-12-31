package gobitcask

type Entry struct {
	FileID    int
	ValueSize int
	ValuePos  int
	Timestamp uint32
}

type KeyDir struct {
	m map[string]*Entry
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		m: make(map[string]*Entry),
	}
}

func (k *KeyDir) Set(key []byte, entry *Entry) {
	k.m[string(key)] = entry
}

func (k *KeyDir) Get(key []byte) (*Entry, bool) {
	entry, ok := k.m[string(key)]
	return entry, ok
}

func (k *KeyDir) Delete(key []byte) {
	delete(k.m, string(key))
}

func (k *KeyDir) GetKeys() [][]byte {
	keys := make([][]byte, len(k.m))
	for key := range k.m {
		keys = append(keys, []byte(key))
	}

	return keys
}
