package util

import (
	"reflect"
	"unsafe"
)

func StringToByte(s string) []byte {
	var b []byte
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pByte := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pByte.Data = sh.Data
	pByte.Len = sh.Len
	pByte.Cap = sh.Len
	return b
}

func ByteToString(b []byte) string {
	var s string
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pByte := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh.Data = pByte.Data
	sh.Len = pByte.Len
	return s
}
