package db

import (
	"confact1/util"
	"fmt"

	"encoding/json"

	"testing"
)

type Snapshot struct {
	LastIndex int64
	LastTerm  int64
}

func TestConfactDbInit(t *testing.T) {
	ConfactDbInit()
	sp := &Snapshot{}
	data, err := Db.Get(util.StringToByte("12"), nil)
	if errs := json.Unmarshal(data, &sp); errs != nil {
		fmt.Println(errs)
	}
	fmt.Println(sp, err)

}
