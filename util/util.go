package util

import (
	"reflect"
	"unsafe"
)

func StringToByte(s string)  []byte{
	sh:=*(*reflect.StringHeader)(unsafe.Pointer(&s))
	b:=&reflect.SliceHeader{
		Len: sh.Len,
		Cap: sh.Len,
		Data: sh.Data,
	}
	return *(*[]byte)(unsafe.Pointer(&b))
}

func BYteToString (b byte)  string{
	sh:=*(*reflect.SliceHeader)(unsafe.Pointer(&b))
	s:=&reflect.StringHeader{
		Len: sh.Len,
		Data: sh.Data,
	}
	return *(*string)(unsafe.Pointer(&s))
}

