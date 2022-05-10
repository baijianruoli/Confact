package arrays

import pb "confact1/confact/proto"

var (
	DataBinaryDomain  = &DataBinary{}
	WriteBinaryDomain = &WriteBinary{}
	LockBinaryDomain  = &LockBinary{}
)

type Binary interface {
	Insert(list []*pb.LogEntry, entry *pb.LogEntry) []*pb.LogEntry
	UpperSearchIndex(list []*pb.LogEntry, entry *pb.LogEntry) int
	LowerSearchNode(list []*pb.LogEntry, ts int64) *pb.LogEntry
	IsExistNode(list []*pb.LogEntry, startTs, endTs int64) bool
}

type DataBinary struct {
}

type WriteBinary struct {
}

type LockBinary struct {
}

func (b *DataBinary) Insert(list []*pb.LogEntry, entry *pb.LogEntry) []*pb.LogEntry {
	index := b.UpperSearchIndex(list, entry)
	suffix := list[index:]
	list = list[0:index]
	list = append(list, entry)
	list = append(list, suffix...)
	return list
}

func (b *DataBinary) UpperSearchIndex(list []*pb.LogEntry, entry *pb.LogEntry) int {

	left := 0
	right := len(list)
	for right > left {
		mid := (left + right) >> 1
		if list[mid].Command.Values.StartTs <= entry.Command.Values.StartTs {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return right
}

func (b *DataBinary) IsExistNode(list []*pb.LogEntry, startTs, endTs int64) bool {
	return true
}

func (b *DataBinary) LowerSearchNode(list []*pb.LogEntry, ts int64) *pb.LogEntry {
	left := 0
	right := len(list)
	for right > left {
		mid := (left + right) >> 1
		if list[mid].Command.Values.StartTs == ts {
			return list[mid]
		}
		if list[mid].Command.Values.StartTs > ts {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return nil

}

func (b *WriteBinary) Insert(list []*pb.LogEntry, entry *pb.LogEntry) []*pb.LogEntry {
	index := b.UpperSearchIndex(list, entry)
	suffix := list[index:]
	list = list[0:index]
	list = append(list, entry)
	list = append(list, suffix...)
	return list
}

func (b *WriteBinary) UpperSearchIndex(list []*pb.LogEntry, entry *pb.LogEntry) int {

	left := 0
	right := len(list)
	for right > left {
		mid := (left + right) >> 1
		if list[mid].Command.Write.CommitTs <= entry.Command.Write.CommitTs {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return right
}

func (b *WriteBinary) LowerSearchNode(list []*pb.LogEntry, ts int64) *pb.LogEntry {
	left := 0
	right := len(list) - 1
	for right > left {
		mid := (left + right + 1) >> 1
		if list[mid].Command.Write.CommitTs <= ts {
			left = mid
		} else {
			right = mid - 1
		}
	}
	if len(list) == 0 || list[left].Command.Write.CommitTs > ts {
		return nil
	}
	return list[left]
}

func (b *WriteBinary) IsExistNode(list []*pb.LogEntry, startTs, endTs int64) bool {
	left := 0
	right := len(list)
	for right-left > 1 {
		mid := (left + right) >> 1
		node := list[mid].Command.Write
		if node.CommitTs >= startTs && node.CommitTs <= endTs {
			return true
		}
		if node.CommitTs < startTs {
			left = mid
		} else if node.CommitTs > endTs {
			right = mid
		}
	}
	if len(list) > 0 {
		node := list[left].Command.Write
		if node.CommitTs >= startTs && node.CommitTs <= endTs {
			return true
		}
	}
	return false
}

func (l *LockBinary) Insert(list []*pb.LogEntry, entry *pb.LogEntry) []*pb.LogEntry {
	index := l.UpperSearchIndex(list, entry)
	suffix := list[index:]
	list = list[0:index]
	list = append(list, entry)
	list = append(list, suffix...)
	return list
}

func (l *LockBinary) UpperSearchIndex(list []*pb.LogEntry, entry *pb.LogEntry) int {
	left := 0
	right := len(list)
	for right > left {
		mid := (left + right) >> 1
		if list[mid].Command.Lock.StartTs <= entry.Command.Lock.StartTs {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return right
}

func (l *LockBinary) LowerSearchNode(list []*pb.LogEntry, ts int64) *pb.LogEntry {
	left := 0
	right := len(list) - 1
	for right > left {
		mid := (left + right + 1) >> 1
		if list[mid].Command.Lock.StartTs <= ts {
			left = mid
		} else {
			right = mid - 1
		}
	}
	if len(list) == 0 || list[left].Command.Lock.StartTs > ts {
		return nil
	}
	return list[left]
}

func (l *LockBinary) IsExistNode(list []*pb.LogEntry, startTs, endTs int64) bool {
	left := 0
	right := len(list)
	for right-left > 1 {
		mid := (left + right) >> 1
		node := list[mid].Command.Lock
		// 默认被软删除了
		if node.Deleted {
			left = mid
			continue
		}
		if node.StartTs >= startTs && node.StartTs <= endTs {
			return true
		}
		if node.StartTs < startTs {
			left = mid
		} else if node.StartTs > endTs {
			right = mid
		}
	}
	if len(list) > 0 {
		node := list[left].Command.Lock
		if node.StartTs >= startTs && node.StartTs <= endTs {
			return true
		}
	}
	return false
}
