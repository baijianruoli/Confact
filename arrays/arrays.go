package arrays

import pb "confact1/confact/proto"

var (
	DataBinaryDomain  *DataBinary  = &DataBinary{}
	WriteBinaryDomain *WriteBinary = &WriteBinary{}
)

type Binary interface {
	Insert(list []*pb.LogEntry, entry *pb.LogEntry) []*pb.LogEntry
	UpperSearchIndex(list []*pb.LogEntry, entry *pb.LogEntry) int
	LowerSearchNode(list []*pb.LogEntry, ts int64) *pb.LogEntry
}

type DataBinary struct {
}

type WriteBinary struct {
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
