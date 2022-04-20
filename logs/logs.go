package logs

import (
	"fmt"
	"github.com/gookit/color"
	"time"
)

func PrintInfo(raftID int64, msg ...interface{}) {
	color.Green.Printf("【INFO】 time:%d raftID：%d msg: %s\n", time.Now().Nanosecond()/1e6, raftID, fmt.Sprint(msg...))
}

func PrintError(raftID int64, msg ...interface{}) {
	color.Red.Printf("【ERROR】 time:%d raftID：%d msg: %s\n", time.Now().Nanosecond()/1e6, fmt.Sprint(msg...))
}
