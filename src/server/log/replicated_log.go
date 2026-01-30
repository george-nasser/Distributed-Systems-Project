package log
import (
	"sync"
)

type LogEntry struct {
	Index   int64
	Command []byte
}

type ReplicatedLog struct {
  	entries map[int64]*LogEntry	
	nextIndex int64
	commitIndex int64
	storedIndex int64
	mutex   sync.Mutex
}

func NewReplicatedLog() *ReplicatedLog {
	return &ReplicatedLog{
		entries:    make(map[int64]*LogEntry),
		nextIndex:  0,
		commitIndex: -1,
		storedIndex: -1,
	}
}
func (log *ReplicatedLog) Append(index int64, command []byte){
	log.mutex.Lock()
	defer log.mutex.Unlock()

	log.entries[log.nextIndex] = &LogEntry{
		Index:   index,
		Command: command,
	}
	if index >= log.nextIndex {
		log.nextIndex = index + 1
	}
	if index > log.commitIndex {
		log.commitIndex = index
	}
}

func (log *ReplicatedLog) GetEntry(index int64) *LogEntry {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	return log.entries[index]
}	

func (log *ReplicatedLog) GetCommitIndex() int64 {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	return log.commitIndex
}

func (log *ReplicatedLog) GetNextIndex() int64 {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	index := log.nextIndex
	log.nextIndex++
	return index
}

func (log *ReplicatedLog) SetCommitIndex(index int64) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	log.commitIndex = index
}

func (log *ReplicatedLog) SetStoredIndex(index int64) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	log.storedIndex = index
}

func (log *ReplicatedLog) GetStoredIndex() int64 {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	return log.storedIndex
}

func (log *ReplicatedLog) SetNextIndex(index int64) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	log.nextIndex = index
}

func (log *ReplicatedLog) Store(upToIndex int64) {
	log.mutex.Lock()
	defer log.mutex.Unlock()

	for i := log.storedIndex ; i <= upToIndex; i++ {
		delete(log.entries, i)
	}
	log.storedIndex = upToIndex + 1
}