package db

import (
	"confact1/common"
	"confact1/util"
	"fmt"
	"testing"
)

func TestConfactDbInit(t *testing.T) {
	ConfactDbInit()
	data, err := Db.Get(util.StringToByte(common.RaftPersist), nil)
	fmt.Println(data,err)
}
