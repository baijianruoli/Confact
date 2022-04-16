package logs

import (
	"fmt"
	"github.com/gookit/color"
)

func PrintInfo(raftID int64, msg ...interface{}) {
	color.Green.Printf("【INFO】 raftID：%d msg: %s\n", raftID, fmt.Sprint(msg...))
}

func PrintError(raftID int64, msg ...interface{}) {
	color.Red.Printf("【ERROR】 raftID：%d msg: %s\n", fmt.Sprint(msg...))
}
